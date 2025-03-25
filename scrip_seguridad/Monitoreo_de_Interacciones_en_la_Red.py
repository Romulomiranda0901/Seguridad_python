from scapy.all import sniff

def packet_callback(packet):
    print(f"Packete capturado: {packet.summary()}")

if __name__ == "__main__":
    print("Capturando paquetes. Presiona Ctrl+C para detener.")
    try:
        sniff(prn=packet_callback, filter="ip", count=10)  # Captura 10 paquetes IP
    except KeyboardInterrupt:
        print("\nCaptura detenida.")