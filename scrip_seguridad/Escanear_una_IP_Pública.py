import requests

def get_ip_info(ip):
    try:
        # Consultar la API de ipinfo.io
        ipinfo_url = f"https://ipinfo.io/{ip}/json"
        response = requests.get(ipinfo_url)
        ip_data = response.json()

        return ip_data
    except Exception as e:
        print(f"Error al obtener información de IP: {e}")
        return {}

def check_vpn(ip):
    try:
        # Consultar la API de vpnapi.io
        vpnapi_url = f"https://vpnapi.io/api/{ip}"
        response = requests.get(vpnapi_url)
        vpn_data = response.json()

        return vpn_data
    except Exception as e:
        print(f"Error al verificar si la IP es de una VPN: {e}")
        return {}

if __name__ == "__main__":
    target_ip = input("Introduce la IP pública a escanear: ")

    # Obtener información de la IP
    ip_info = get_ip_info(target_ip)
    if ip_info:
        print("\nInformación de la IP:")
        print(f"IP: {ip_info.get('ip')}")
        print(f"Ubicación: {ip_info.get('city', 'Desconocida')}, {ip_info.get('region', 'Desconocida')}, {ip_info.get('country', 'Desconocida')}")
        print(f"Organización: {ip_info.get('org', 'Desconocida')}")
        print(f"Más información: https://ipinfo.io/{ip_info.get('ip')}")

    # Comprobar si es VPN
    vpn_info = check_vpn(target_ip)
    if vpn_info:
        if vpn_info.get('security') and vpn_info['security'].get('vpn'):
            print("\nLa IP es una dirección de VPN.")
            print(f"Tipo de VPN: {vpn_info['security'].get('vpn_type', 'Desconocido')}")
        else:
            print("\nLa IP no es una dirección de VPN.")
    else:
        print("No se pudo verificar la información de VPN.")