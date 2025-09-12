# Organizador de Descargas

Este proyecto es una herramienta diseñada para organizar automáticamente los archivos en una carpeta, como la carpeta de Descargas. Permite personalizar el comportamiento mediante un archivo de configuración.

## Características

- **Organización Automática**: Clasifica los archivos por tipo (imágenes, documentos, audio, etc.).
- **Exclusión de Carpetas**: Permite definir carpetas que no deben ser afectadas.
- **Log de Eventos**: Registra todas las acciones realizadas y errores en un archivo de log.

## Requisitos

- Python 3.8 o superior
- Librerías estándar de Python:
  - `json`
  - `os`
  - `shutil`

## Estructura del Proyecto

```
Organizadorpy/
├── main.py                # Archivo principal para ejecutar el programa
├── config/
│   ├── config.json        # Archivo de configuración del proyecto
├── utils/
│   ├── __init__.py        # Indica que utils es un paquete
│   ├── file_operations.py # Funciones para organizar y mover archivos
│   ├── validations.py     # Funciones para validar rutas
│   ├── logging_utils.py   # Funciones para manejar logs
```

## Configuración Inicial

### Archivo `config.json`

El archivo de configuración define las carpetas excluidas y los tipos de archivos a organizar. Se encuentra en la carpeta `config/` y tiene el siguiente formato:

```json
{
    "carpetas_excluidas": [
        "Escritorio",
        "Desktop",
        "Documentos",
        "Downloads",
        "Videos",
        "Music",
        "Pictures",
        "OneDrive"
    ],
    "tipos_archivos": {
        "Imágenes": [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp"],
        "Documentos": [".pdf", ".doc", ".docx", ".txt", ".xls", ".xlsx", ".ppt", ".pptx", ".odt"],
        "Audio": [".mp3", ".wav", ".aac", ".flac", ".ogg"],
        "Vídeos": [".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv"],
        "Comprimidos": [".zip", ".rar", ".7z", ".tar", ".gz"],
        "Ejecutables": [".exe", ".msi", ".bat", ".sh"]
    }
}
```

### Cómo Personalizar

1. **Carpetas Excluidas**:
   - Agrega o elimina nombres de carpetas dentro de `carpetas_excluidas`.
2. **Tipos de Archivos**:
   - Modifica las extensiones en `tipos_archivos` para clasificar archivos según tus necesidades.

## Ejecución

### Modo Línea de Comandos

1. Abre una terminal o consola.
2. Ejecuta el archivo principal:

   ```bash
   python main.py
   ```

3. Sigue las instrucciones para:
   - Introducir la ruta de la carpeta a organizar.
   - Ver el log de eventos al finalizar.

### Archivos Generados

- **log_organizacion.txt**: Se guarda en la carpeta organizada y contiene un registro detallado de las operaciones realizadas.

## Contribuir

Si deseas contribuir al desarrollo de este proyecto:

1. Haz un fork del repositorio.
2. Crea una nueva rama para tu funcionalidad o corrección.
   ```bash
   git checkout -b nueva-funcionalidad
   ```
3. Haz un pull request con tus cambios.

## Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.

