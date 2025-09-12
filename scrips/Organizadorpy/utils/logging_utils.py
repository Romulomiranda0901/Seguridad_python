from datetime import datetime

def agregar_tiempo_log(mensaje):
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {mensaje}"
