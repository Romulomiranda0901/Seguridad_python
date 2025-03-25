import requests

def check_vulnerable(url):
    common_paths = ['/admin', '/login', '/test', '/config', '/backup']
    for path in common_paths:
        full_url = url + path
        response = requests.get(full_url)
        if response.status_code == 200:
            print(f"Posible vulnerabilidad encontrada en: {full_url}")

if __name__ == "__main__":
    target_url = input("Introduce la URL a escanear (sin http/https): ")
    check_vulnerable(target_url)