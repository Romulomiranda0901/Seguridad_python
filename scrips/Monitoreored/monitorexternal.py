from scapy.all import sniff, IP
import time
from collections import defaultdict

# Diccionario para almacenar el tráfico total por IP
traffic_data = defaultdict(lambda: {"sent": 0, "received": 0})

def packet_callback(packet):
    """
    Callback que se ejecuta por cada paquete capturado.
    Se encarga de clasificar el tráfico por IP de origen y destino.
    """
    if IP in packet:
        src_ip = packet[IP].src  # Dirección IP de origen
        dst_ip = packet[IP].dst  # Dirección IP de destino
        packet_length = len(packet)  # Tamaño del paquete en bytes

        # Actualizar tráfico enviado y recibido por IP
        traffic_data[src_ip]["sent"] += packet_length
        traffic_data[dst_ip]["received"] += packet_length

def print_summary():
    """
    Imprime un resumen del tráfico de red clasificado por IP.
    """
    print("\n===== Resumen del tráfico de red por dispositivo =====")
    print(f"{'IP':<20}{'Enviado (KB)':<15}{'Recibido (KB)':<15}")
    print("-" * 50)

    for ip, stats in traffic_data.items():
        sent_kb = stats['sent'] / 1024  # Convertir a KB
        received_kb = stats['received'] / 1024  # Convertir a KB
        print(f"{ip:<20}{sent_kb:<15.2f}{received_kb:<15.2f}")
    print("-" * 50)

def monitor_external(duration):
    """
    Monitorea los dispositivos externos en la red por un tiempo determinado.
    Args:
        duration (int): Duración del monitoreo en segundos.
    """
    print("Iniciando monitoreo de dispositivos externos en la red...")
    start_time = time.time()
    
    try:
        while time.time() - start_time < duration:
            sniff(prn=packet_callback, store=0, timeout=1)
            print_summary()
            time.sleep(1)  # Mostrar el resumen cada segundo
    except KeyboardInterrupt:
        print("\nMonitoreo detenido manualmente.")
    finally:
        print("\nMonitoreo finalizado. Resumen final:")
        print_summary()

if __name__ == "__main__":
    while True:
        try:
            duration = int(input("¿Cuánto tiempo deseas monitorear la red? (en segundos): "))
            if duration <= 0:
                raise ValueError("El tiempo debe ser mayor que 0.")
            break
        except ValueError as e:
            print(f"Entrada no válida: {e}. Intenta nuevamente.\n")

    monitor_external(duration)
