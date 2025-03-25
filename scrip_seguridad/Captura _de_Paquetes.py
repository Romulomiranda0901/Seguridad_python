from scapy.all import sniff

def packet_callback(packet):
    print(packet.summary())

if __name__ == "__main__":
    print("Capturando paquetes... Presiona Ctrl+C para detener.")
    sniff(prn=packet_callback, count=10)  # Captura 10 paquetes