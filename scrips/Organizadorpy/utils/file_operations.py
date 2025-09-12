from pathlib import Path
import shutil
from .logging_utils import agregar_tiempo_log

def mover_archivo(archivo, destino, log):
    if archivo.parent == destino:
        log.append(agregar_tiempo_log(f"Archivo ya está organizado: {archivo}"))
    else:
        destino.mkdir(parents=True, exist_ok=True)
        shutil.move(str(archivo), str(destino / archivo.name))
        log.append(agregar_tiempo_log(f"Archivo movido: {archivo} -> {destino / archivo.name}"))

def organizar_archivos(ruta_descargas, log, carpetas_excluidas, tipos_archivos):
    ruta_otros = Path(ruta_descargas) / "Otros"
    for archivo in Path(ruta_descargas).iterdir():
        if any(str(archivo).startswith(str(Path(ruta_descargas) / excluida)) for excluida in carpetas_excluidas):
            log.append(agregar_tiempo_log(f"Carpeta excluida: {archivo}"))
            continue

        if archivo.is_file():
            extension = archivo.suffix.lower()
            destino = next((Path(ruta_descargas) / carpeta for carpeta, extensiones in tipos_archivos.items() if extension in extensiones), ruta_otros)
            mover_archivo(archivo, destino, log)

def eliminar_carpetas_vacias(ruta_base, log, carpetas_excluidas):
    for carpeta in sorted(Path(ruta_base).rglob("*"), key=lambda x: len(str(x)), reverse=True):
        if any(str(carpeta).startswith(str(Path(ruta_base) / excluida)) for excluida in carpetas_excluidas):
            continue
        if carpeta.is_dir() and not any(carpeta.iterdir()):
            carpeta.rmdir()
            log.append(agregar_tiempo_log(f"Carpeta vacía eliminada: {carpeta}"))
