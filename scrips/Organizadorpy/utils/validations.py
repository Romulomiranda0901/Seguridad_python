from pathlib import Path
import platform

def validar_ruta(ruta):
    try:
        ruta_path = Path(ruta)
        if not ruta_path.exists() or not ruta_path.is_dir():
            return False, "La ruta no existe o no es una carpeta."
        sistema_operativo = platform.system()
        if (sistema_operativo == "Windows" and not str(ruta_path).startswith("C:\\Users")) or \
           (sistema_operativo == "Linux" and not str(ruta_path).startswith(str(Path.home()))):
            return False, "La ruta no está dentro de un directorio permitido del usuario."
        return True, ""
    except Exception as e:
        return False, f"Error al validar la ruta: {e}"
