import psutil
import time
import re
import os
import platform
import subprocess
from datetime import datetime

def parse_duration(duration_str):
    """
    Interpreta el tiempo ingresado en segundos, minutos u horas.
    """
    duration_str = duration_str.lower().replace(" ", "")
    match = re.match(r"(\d+)(s|seg|segundos|m|min|minutos|h|hora|horas)?", duration_str)
    if not match:
        raise ValueError("Formato de tiempo no válido. Ejemplo válido: '10s', '5min', '2h'.")
    
    value, unit = int(match[1]), match[2]
    if unit in ["s", "seg", "segundos", None]:
        return value
    elif unit in ["m", "min", "minutos"]:
        return value * 60
    elif unit in ["h", "hora", "horas"]:
        return value * 3600
    else:
        raise ValueError("Unidad de tiempo no válida.")

def get_bluetooth_devices():
    """
    Obtiene dispositivos Bluetooth conectados (solo en Linux con bluetoothctl).
    """
    if platform.system() == "Linux":
        try:
            output = subprocess.check_output("bluetoothctl devices connected", shell=True).decode()
            devices = output.strip().split("\n")
            return devices if devices else ["No hay dispositivos Bluetooth conectados."]
        except subprocess.CalledProcessError:
            return ["Error al obtener dispositivos Bluetooth."]
    return ["Monitorización de dispositivos Bluetooth solo implementada en Linux."]

def generate_log_filename():
    """
    Genera un nombre de archivo único con timestamp en la carpeta 'log' dentro del proyecto.
    """
    # Ruta RELATIVA a la carpeta donde se ejecuta el script
    base_dir = os.path.dirname(os.path.abspath(__file__))  # Directorio actual del script
    log_dir = os.path.join(base_dir, "log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(log_dir, f"network_log_{timestamp}.txt")

def monitor_network(interval=1):
    """
    Monitorea el tráfico de red y dispositivos Bluetooth conectados.
    """
    print("===== Monitoreo de tráfico de red y Bluetooth =====")
    
    while True:
        try:
            duration_input = input("¿Cuánto tiempo deseas monitorear? (Ej: 10s, 5min, 2h): ").strip()
            duration = parse_duration(duration_input)
            break
        except ValueError as e:
            print(f"Entrada no válida: {e}. Intenta nuevamente.\n")
    
    log_file = generate_log_filename()
    print(f"\nIniciando monitoreo durante {duration_input}...\n")
    print(f"El log será guardado en: {log_file}\n")

    old_data = psutil.net_io_counters(pernic=True)
    
    # Arrays para almacenar flujo
    ip_data = []
    bt_data = []
    other_data = []
    bt_devices_data = []

    end_time = time.time() + duration

    try:
        while True:
            remaining_time = int(end_time - time.time())
            if remaining_time <= 0:
                print("\nTiempo de monitoreo alcanzado.")
                break

            # Mostrar contador de tiempo restante
            time_display = time.strftime("%H:%M:%S", time.gmtime(remaining_time))
            print(f"Tiempo restante: {time_display}", end="\r")

            time.sleep(interval)
            new_data = psutil.net_io_counters(pernic=True)
            formatted_time = time.strftime("%Y-%m-%d %H:%M:%S")

            for iface in new_data:
                sent = new_data[iface].bytes_sent - old_data[iface].bytes_sent
                recv = new_data[iface].bytes_recv - old_data[iface].bytes_recv
                log_entry = f"{formatted_time} | {iface}: Enviado={sent / 1024:.2f} KB, Recibido={recv / 1024:.2f} KB"

                # Clasificación de tráfico
                if "Ethernet" in iface or "Wi-Fi" in iface:
                    ip_data.append(log_entry)
                elif "Bluetooth" in iface:
                    bt_data.append(log_entry)
                else:
                    other_data.append(log_entry)

                print(log_entry)

            # Obtener dispositivos Bluetooth activos
            devices = get_bluetooth_devices()
            for device in devices:
                device_entry = f"{formatted_time} | {device}"
                bt_devices_data.append(device_entry)
                print(device_entry)

            old_data = new_data
    except KeyboardInterrupt:
        print("\nMonitoreo detenido manualmente.")
    finally:
        # Guardar el log
        with open(log_file, "w") as f:
            f.write("===== FLUJO DE PUERTO IP =====\n")
            f.write("\n".join(ip_data))
            f.write("\n\n\f")

            f.write("===== FLUJO DE BLUETOOTH (Red) =====\n")
            f.write("\n".join(bt_data))
            f.write("\n\n\f")

            f.write("===== DISPOSITIVOS BLUETOOTH =====\n")
            f.write("\n".join(bt_devices_data))
            f.write("\n\n\f")

            f.write("===== FLUJO RESTANTE =====\n")
            f.write("\n".join(other_data))
        
        print(f"\nLog guardado en {log_file}")

# Llamada a la función
monitor_network()