import psutil
import time

def monitor_cpu(interval=1):
    print("Monitoreando el uso de CPU. Presiona Ctrl+C para detener.")
    try:
        while True:
            cpu_usage = psutil.cpu_percent(interval=interval)
            print(f"Uso de CPU: {cpu_usage}%")
    except KeyboardInterrupt:
        print("\nMonitoreo detenido.")

if __name__ == "__main__":
    monitor_cpu()