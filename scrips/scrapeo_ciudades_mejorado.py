import requests
from bs4 import BeautifulSoup
import json
import psycopg2
from unidecode import unidecode

DB_HOST = "host"
DB_NAME = "base datos"
DB_USER = "usuario"
DB_PASSWORD = "contraseña"
BASE_URL = "https://www.javiercolomo.com/index_archivos/Mundo/"
JSON_FILE = "ciudades_del_mundo.json"

# --- Funciones de Normalización y Limpieza ---
def normalizar_nombre(nombre):
    return unidecode(nombre).upper().replace(" ", "")

def limpiar_nombre_pais(nombre_pais):
    nombre_pais = nombre_pais.replace("\u00A0\u00A0", " ").replace("  ", " ").strip()
    return nombre_pais

def conectar_db():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def obtener_paises_de_db(conn):
    paises = {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM public.countries")
        rows = cur.fetchall()
        for row in rows:
            pais_id = row[0]
            nombre_pais = row[1]
            nombre_pais_normalizado = normalizar_nombre(nombre_pais)
            paises[nombre_pais_normalizado] = pais_id
        cur.close()
    except psycopg2.Error as e:
        print(f"Error al consultar la base de datos: {e}")
    return paises

def cerrar_db(conn):
    if conn:
        conn.close()
        print("Conexión a la base de datos cerrada.")

def scrape_continent_page(url, data, paises_db):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error al acceder a la página {url}: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

    # Encuentra la tabla que contiene la información
    main_table = soup.find('table', style="border-collapse: collapse; ", bordercolor="#F2F2F2", cellpadding="0", cellspacing="0", border="1")

    if main_table:
        nested_table = main_table.find('table', width="100%", border="1", cellspacing="0", cellpadding="0", bordercolor="#F2F2F2", style="border-collapse: collapse; ")

        if nested_table:
            tbody = nested_table.find('tbody', id='myTable')

            if tbody:
                for row in tbody.find_all('tr')[1:]:  # Ignorando el encabezado
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        pais = limpiar_nombre_pais(cells[0].get_text().strip())
                        ciudad = cells[1].get_text().strip()

                        pais_normalizado = normalizar_nombre(pais)
                        pais_id = paises_db.get(pais_normalizado)

                        data.append({"pais": pais, "ciudad": ciudad, "id_pais": pais_id})
            else:
                print(f"No se encontró el tbody en {url}.")
        else:
            print(f"No se encontró la tabla anidada en {url}.")
    else:
        print(f"No se encontró la tabla principal en {url}.")

def scrape_all_continents(main_url, paises_db):
    try:
        response = requests.get(main_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        links = soup.find_all('font', {'face': 'Arial', 'size': '5'})
        data = []

        for link in links:
            a_tag = link.find('a')
            if a_tag:
                href = a_tag.get('href')
                continent_url = BASE_URL + href
                print(f"Scrapeando: {continent_url}")
                scrape_continent_page(continent_url, data, paises_db)

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error al acceder a la página principal: {e}")
        return []

MAIN_URL = "https://www.javiercolomo.com/index_archivos/Mundo/Mundo2.htm"

def main():
    conn = conectar_db()
    if not conn:
        return

    paises_db = obtener_paises_de_db(conn)
    result = scrape_all_continents(MAIN_URL, paises_db)

    if result:  # Verifica que result tenga datos
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)
        print(f"Archivo JSON '{JSON_FILE}' guardado con éxito.")
    else:
        print("No se pudo obtener los datos.")
    cerrar_db(conn)

if __name__ == "__main__":
    main()