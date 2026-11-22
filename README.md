# Meiloader

Motor inicial de descargas HTTP/HTTPS con múltiples solicitudes de rangos por archivo.

El prototipo está configurado para Rust MSVC en Windows.

## Uso

Para probar una descarga real con rangos, abre dos terminales en el directorio del proyecto. En la primera, inicia el servidor local de prueba:

```powershell
python tests/e2e.py --serve
```

En la segunda, descarga el archivo de prueba:

```powershell
cargo run --release -- "http://127.0.0.1:8765/ranges" --output "prueba.bin" --connections 8
Get-FileHash .\prueba.bin -Algorithm SHA256
```

El hash debe coincidir con el que muestra el servidor. Detén el servidor con Ctrl+C. Para repetir la prueba, usa otro nombre de salida o elimina `prueba.bin`. Para descargar un archivo propio, sustituye la URL local por una URL directa de HTTP o HTTPS que exista.

`--connections` admite de 1 a 32 (8 por defecto). Cuando un servidor acepta rangos, las conexiones toman nuevos bloques según van terminando. Si el servidor no admite rangos, Meiloader descarga con una conexión. Para archivos menores de 2 MiB también usa una conexión. Nunca reemplaza un archivo de salida existente.

## Estado

Esta primera versión admite URLs directas y guarda el resultado mediante un archivo temporal. Todavía no conserva descargas interrumpidas, no ajusta automáticamente la concurrencia y no tiene interfaz gráfica. Esas son las próximas etapas del proyecto.

## Pruebas

```powershell
cargo build
python tests/e2e.py
```

Las pruebas ejecutan un servidor HTTP local con respuestas por rangos, respuestas sin rangos y respuestas parciales incorrectas. Comparan el hash SHA-256 de los archivos descargados.
