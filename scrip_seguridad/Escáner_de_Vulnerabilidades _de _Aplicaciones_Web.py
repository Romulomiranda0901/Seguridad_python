import requests

def scan_headers(url):
    try:
        response = requests.get(url)
        headers = response.headers

        print(f"Encabezados de seguridad para {url}:")
        for header, value in headers.items():
            print(f"{header}: {value}")

        # Comprobación básica de algunos encabezados de seguridad
        security_headers = ['X-Content-Type-Options', 'X-XSS-Protection', 'Strict-Transport-Security']
        for header in security_headers:
            if header in headers:
                print(f"[OK] `{header}` está presente.")
            else:
                print(f"[WARNING] `{header}` NO está presente.")

    except requests.exceptions.RequestException as e:
        print(f"Error al acceder a {url}: {e}")

if __name__ == "__main__":
    target_url = input("Introduce la URL de la aplicación web (ej: http://example.com): ")
    scan_headers(target_url)