use std::{path::PathBuf, sync::Arc};

use meiloader::{Args, ProgressSink, ProgressUpdate};
use tauri::{ipc::Channel, Manager};

#[tauri::command]
fn default_download_path(app: tauri::AppHandle, filename: String) -> Result<String, String> {
    let directory = app.path().download_dir().map_err(|error| error.to_string())?;
    Ok(directory.join(filename).to_string_lossy().into_owned())
}

#[tauri::command]
async fn start_download(
    url: String,
    output: String,
    connections: u16,
    fixed_connections: bool,
    on_progress: Channel<ProgressUpdate>,
) -> Result<(), String> {
    if !(1..=32).contains(&connections) {
        return Err("El número de conexiones debe estar entre 1 y 32".into());
    }
    let sink: ProgressSink = Arc::new(move |progress| {
        let _ = on_progress.send(progress);
    });
    meiloader::run(
        Args {
            url,
            output: PathBuf::from(output),
            connections,
            fixed_connections,
        },
        Some(sink),
    )
    .await
    .map_err(|error| format!("{error:#}"))
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![start_download, default_download_path])
        .run(tauri::generate_context!())
        .expect("no se pudo iniciar Meiloader");
}
