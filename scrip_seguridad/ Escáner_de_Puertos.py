import socket

def scan_ports(target):
    print(f"Iniciando escaneo de puertos en {target}...")
    for port in range(1, 1025):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((target, port))
        if result == 0:
            print(f"Puerto {port} está abierto")
        sock.close()

if __name__ == "__main__":
    target_ip = input("Introduce la dirección IP a escanear: ")
    scan_ports(target_ip)