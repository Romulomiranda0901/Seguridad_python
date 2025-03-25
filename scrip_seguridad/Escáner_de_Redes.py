from scapy.all import ARP, Ether, srp

def scan_network(ip_range):
    # Crea un paquete ARP
    arp = ARP(pdst=ip_range)
    ether = Ether(dst="ff:ff:ff:ff:ff:ff")
    packet = ether / arp

    # Envía el paquete y recibe la respuesta
    result = srp(packet, timeout=3, verbose=0)[0]

    # Extrae las direcciones IP y MAC de los dispositivos
    devices = []
    for sent, received in result:
        devices.append({'ip': received.psrc, 'mac': received.hwsrc})

    return devices

if __name__ == "__main__":
    ip_range = input("Introduce el rango de IP a escanear (ejemplo: 192.168.1.1/24): ")
    devices = scan_network(ip_range)

    print("Dispositivos encontrados en la red:")
    for device in devices:
        print(f"IP: {device['ip']}, MAC: {device['mac']}")