# Meiloader para Windows

Interfaz Tauri para el motor de descargas de este repositorio. La CLI continúa disponible desde la raíz.

```powershell
npm install
npm run tauri dev
```

Para compilar sin abrir la ventana de desarrollo:

```powershell
npm run tauri build
```

La aplicación procesa una descarga por vez y muestra progreso, velocidad y conexiones activas. Si una descarga falla, el botón **Reintentar** usa la misma URL y ruta para aprovechar los bloques ya completados. La cola no se conserva al cerrar la aplicación; la reanudación del archivo sí se conserva cuando el servidor admite rangos y proporciona un `ETag` fuerte.
