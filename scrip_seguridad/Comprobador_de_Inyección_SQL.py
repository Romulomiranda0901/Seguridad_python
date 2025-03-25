import requests

def check_sql_injection(url):
    payload = "' OR '1'='1' -- "
    response = requests.get(url + payload)

    if "sql syntax" in response.text.lower() or \
       response.status_code == 500:  # Errores típicos de SQL
        print("Posible vulnerabilidad de inyección SQL detectada!")
    else:
        print("No se detectó vulnerabilidad de inyección SQL.")

if __name__ == "__main__":
    target_url = input("Introduce la URL a comprobar: ")
    check_sql_injection(target_url)