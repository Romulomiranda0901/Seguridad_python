import json
from pathlib import Path
from utils.file_operations import organizar_archivos, eliminar_carpetas_vacias
from utils.validations import validar_ruta
from utils.logging_utils import agregar_tiempo_log

def cargar_configuracion(config_path):
    """
    Carga el archivo de configuración desde una ruta relativa al archivo principal.
    """
    config_path = Path(__file__).parent / config_path  # Convierte la ruta a absoluta
    if not config_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {config_path}")
    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)

if __name__ == "__main__":
    log = []
    try:
        # Cargar configuración desde config/config.json
        config = cargar_configuracion("config/config.json")
        carpetas_excluidas = config["carpetas_excluidas"]
        tipos_archivos = config["tipos_archivos"]
    except FileNotFoundError as e:
        print(f"Error: {e}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Error al parsear config.json: {e}")
        exit(1)

    while True:
        ruta_descargas = input("Introduce la ruta de la carpeta: ").strip()
        es_valida, mensaje = validar_ruta(ruta_descargas)
        if es_valida:
            break
        log.append(agregar_tiempo_log(f"Ruta inválida: {ruta_descargas}. Motivo: {mensaje}"))
        print(f"Error: {mensaje}. Por favor, introduce una ruta válida.")

    try:
        organizar_archivos(ruta_descargas, log, carpetas_excluidas, tipos_archivos)
        eliminar_carpetas_vacias(ruta_descargas, log, carpetas_excluidas)
        log.append(agregar_tiempo_log("Organización completa."))
    except Exception as e:
        log.append(agregar_tiempo_log(f"Ocurrió un error inesperado: {e}"))

    log_file = Path(ruta_descargas) / "log_organizacion.txt"
    try:
        with open(log_file, "w", encoding="utf-8") as file:
            file.write("\n".join(log))
        print(f"Log guardado en: {log_file}")
    except Exception as e:
        print(f"Error al guardar el log: {e}")