use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use clap::Parser;
use futures_util::{StreamExt, future::try_join_all};
use reqwest::{
    Client, Response, StatusCode,
    header::{
        ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_RANGE, ETAG, HeaderValue, IF_RANGE, RANGE,
    },
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt},
};

#[derive(Parser, Debug)]
#[command(
    name = "meiloader",
    about = "Descarga archivos HTTP(S) con rangos en paralelo"
)]
struct Args {
    /// URL directa del archivo
    url: String,
    /// Ruta del archivo final
    #[arg(short, long)]
    output: PathBuf,
    /// Máximo de solicitudes simultáneas por archivo
    #[arg(short, long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..=32))]
    connections: u16,
}

struct RemoteFile {
    size: u64,
    etag: Option<HeaderValue>,
}

#[derive(Debug)]
struct RangeIgnored;

impl std::fmt::Display for RangeIgnored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "el servidor ignoró una solicitud de rango")
    }
}

impl std::error::Error for RangeIgnored {}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !args.url.starts_with("http://") && !args.url.starts_with("https://") {
        bail!("solo se admiten URLs HTTP y HTTPS");
    }
    if args.output.exists() {
        bail!("el archivo de salida ya existe: {}", args.output.display());
    }

    let parent = args
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        bail!("el directorio de salida no existe: {}", parent.display());
    }

    let client = Client::builder()
        .build()
        .context("no se pudo crear el cliente HTTP")?;
    let probe = client
        .get(&args.url)
        .header(ACCEPT_ENCODING, "identity")
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .context("falló la consulta inicial")?;

    let remote = match probe.status() {
        StatusCode::PARTIAL_CONTENT => Some(inspect_probe(&probe)?),
        StatusCode::OK => None,
        StatusCode::RANGE_NOT_SATISFIABLE => None,
        status => bail!("la consulta inicial devolvió HTTP {status}"),
    };

    let mut temp =
        tempfile::NamedTempFile::new_in(parent).context("no se pudo crear el archivo temporal")?;
    let temp_path = temp.path().to_path_buf();

    if let Some(remote) = remote.filter(|r| args.connections > 1 && r.size >= 2 * 1024 * 1024) {
        let count = (remote.size / (1024 * 1024)).min(args.connections as u64) as usize;
        temp.as_file_mut()
            .set_len(remote.size)
            .context("no se pudo reservar el archivo")?;
        eprintln!("Descargando {} bytes en {count} conexiones", remote.size);
        if let Err(error) = download_parallel(&client, &args.url, &temp_path, &remote, count).await
        {
            if error.downcast_ref::<RangeIgnored>().is_none() {
                return Err(error);
            }
            eprintln!("El servidor dejó de aceptar rangos; reiniciando con una conexión");
            drop(temp);
            temp = tempfile::NamedTempFile::new_in(parent)?;
            let response = client
                .get(&args.url)
                .header(ACCEPT_ENCODING, "identity")
                .send()
                .await
                .context("falló la descarga simple")?;
            download_single(response, temp.path()).await?;
        }
    } else {
        eprintln!("Descargando con una conexión");
        let response = if probe.status() == StatusCode::OK {
            probe
        } else {
            client
                .get(&args.url)
                .header(ACCEPT_ENCODING, "identity")
                .send()
                .await
                .context("falló la descarga")?
        };
        download_single(response, &temp_path).await?;
    }

    temp.persist_noclobber(&args.output)
        .with_context(|| format!("no se pudo guardar {}", args.output.display()))?;
    println!("Descarga completa: {}", args.output.display());
    Ok(())
}

fn inspect_probe(response: &Response) -> Result<RemoteFile> {
    ensure_identity(response)?;
    let value = response
        .headers()
        .get(CONTENT_RANGE)
        .context("respuesta 206 sin Content-Range")?
        .to_str()?;
    let size = parse_content_range(value, 0, 0)
        .context("Content-Range inválido en la consulta inicial")?;
    if size == 0 {
        bail!("el tamaño anunciado es cero");
    }
    Ok(RemoteFile {
        size,
        etag: response.headers().get(ETAG).cloned(),
    })
}

fn parse_content_range(value: &str, expected_start: u64, expected_end: u64) -> Result<u64> {
    let rest = value
        .strip_prefix("bytes ")
        .context("unidad de rango inesperada")?;
    let (range, total) = rest.split_once('/').context("falta el tamaño total")?;
    let (start, end) = range.split_once('-').context("intervalo inválido")?;
    if start.parse::<u64>()? != expected_start || end.parse::<u64>()? != expected_end {
        bail!("el servidor respondió con otro intervalo");
    }
    let total = total.parse::<u64>().context("tamaño total desconocido")?;
    if total <= expected_end {
        bail!("tamaño total incoherente");
    }
    Ok(total)
}

fn ensure_identity(response: &Response) -> Result<()> {
    if let Some(value) = response.headers().get(CONTENT_ENCODING) {
        if !value.as_bytes().eq_ignore_ascii_case(b"identity") {
            bail!("el servidor aplicó Content-Encoding; no es seguro combinar rangos");
        }
    }
    Ok(())
}

async fn download_single(response: Response, path: &Path) -> Result<()> {
    if response.status() != StatusCode::OK {
        bail!("la descarga simple devolvió HTTP {}", response.status());
    }
    ensure_identity(&response)?;
    let expected = response.content_length();
    let mut file = OpenOptions::new().write(true).open(path).await?;
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("se interrumpió la descarga")?;
        file.write_all(&chunk).await?;
        received += chunk.len() as u64;
    }
    if let Some(expected) = expected {
        if received != expected {
            bail!("descarga incompleta: {received} de {expected} bytes");
        }
    }
    file.sync_all().await?;
    Ok(())
}

async fn download_parallel(
    client: &Client,
    url: &str,
    path: &Path,
    remote: &RemoteFile,
    count: usize,
) -> Result<()> {
    let chunk_size = remote
        .size
        .div_ceil(count as u64 * 8)
        .clamp(256 * 1024, 8 * 1024 * 1024);
    let next = AtomicU64::new(0);
    let tasks = (0..count).map(|_| async {
        let mut file = OpenOptions::new().write(true).open(path).await?;
        loop {
            let start = next.fetch_add(chunk_size, Ordering::Relaxed);
            if start >= remote.size {
                break;
            }
            let end = start.saturating_add(chunk_size - 1).min(remote.size - 1);
            download_range(client, url, &mut file, remote, start, end).await?;
        }
        file.sync_all().await?;
        Ok::<(), anyhow::Error>(())
    });
    try_join_all(tasks).await?;
    Ok(())
}

async fn download_range(
    client: &Client,
    url: &str,
    file: &mut tokio::fs::File,
    remote: &RemoteFile,
    start: u64,
    end: u64,
) -> Result<()> {
    let mut request = client
        .get(url)
        .header(ACCEPT_ENCODING, "identity")
        .header(RANGE, format!("bytes={start}-{end}"));
    if let Some(etag) = &remote.etag {
        if !etag.as_bytes().starts_with(b"W/") {
            request = request.header(IF_RANGE, etag.clone());
        }
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("falló el tramo {start}-{end}"))?;
    if response.status() == StatusCode::OK {
        return Err(RangeIgnored.into());
    }
    if response.status() != StatusCode::PARTIAL_CONTENT {
        bail!(
            "el tramo {start}-{end} devolvió HTTP {} en vez de 206",
            response.status()
        );
    }
    ensure_identity(&response)?;
    let value = response
        .headers()
        .get(CONTENT_RANGE)
        .context("tramo sin Content-Range")?
        .to_str()?;
    if parse_content_range(value, start, end)? != remote.size {
        bail!("el tamaño del archivo cambió durante la descarga");
    }
    if let (Some(expected), Some(actual)) = (&remote.etag, response.headers().get(ETAG)) {
        if expected != actual {
            bail!("el ETag cambió durante la descarga");
        }
    }

    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut received = 0_u64;
    let mut stream = response.bytes_stream();
    let expected = end - start + 1;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("se interrumpió el tramo {start}-{end}"))?;
        received += chunk.len() as u64;
        if received > expected {
            bail!("el servidor envió bytes de más para el tramo {start}-{end}");
        }
        file.write_all(&chunk).await?;
    }
    if received != expected {
        bail!("tramo incompleto {start}-{end}: {received} de {expected} bytes");
    }
    Ok(())
}
