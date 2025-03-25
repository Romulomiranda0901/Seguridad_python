import requests

def check_https(url):
    try:
        response = requests.get(url)
        if response.url.startswith("https://"):
            print(f"{url} utiliza HTTPS.")
        else:
            print(f"{url} no utiliza HTTPS.")
    except requests.exceptions.RequestException as e:
        print(f"Error al acceder a {url}: {e}")

if __name__ == "__main__":
    website = input("Introduce la URL del sitio a comprobar (ejemplo: http://example.com): ")
    check_https(website)