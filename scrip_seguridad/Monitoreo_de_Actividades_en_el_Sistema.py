import psutil
import time

def monitor_system_resources(interval=1):
    print("Monitoreando recursos del sistema. Presiona Ctrl+C para detener.")
    try:
        while True:
            print(f"Uso de CPU: {psutil.cpu_percent()}%")
            print(f"Uso de RAM: {psutil.virtual_memory().percent}%")
            print("Lista de procesos:")
            for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
                print(f"PID: {proc.info['pid']}, Nombre: {proc.info['name']}, Memoria: {proc.info['memory_info'].rss / (1024 * 1024):.2f} MB")
            print("-" * 30)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nMonitoreo detenido.")

if __name__ == "__main__":
    monitor_system_resources()