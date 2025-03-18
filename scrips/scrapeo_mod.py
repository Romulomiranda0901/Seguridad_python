import requests
from bs4 import BeautifulSoup
import json
import psycopg2
from unidecode import unidecode

# --- Configuración ---
DB_HOST = "localhost"
DB_NAME = "servi"
DB_USER = "postgres"
DB_PASSWORD = "jpeh0901"
WIKI_URL = "https://es.m.wikipedia.org/wiki/Anexo:Aeropuertos_seg%C3%BAn_el_c%C3%B3digo_IATA"
JSON_FILE = "aeropuertos.json"

# --- Funciones de Normalización y Limpieza ---
def normalizar_nombre(nombre):
    """Normaliza un nombre eliminando espacios y caracteres no ASCII, y convirtiéndolo a mayúsculas."""
    nombre = unidecode(nombre).upper().replace(" ", "")
    return nombre

def limpiar_nombre_pais(nombre_pais):
    """Limpia el nombre del país eliminando repeticiones."""
    nombre_pais = nombre_pais.replace("\u00A0\u00A0", " ").replace("  ", " ").strip()
    words = nombre_pais.split()
    if len(words) > 1 and all(word == words[0] for word in words):
        nombre_pais = words[0]
    return nombre_pais

# --- Funciones de Base de Datos ---
def conectar_db():
    """Establece una conexión a la base de datos."""
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def obtener_paises_de_db(conn):
    """Obtiene los nombres y IDs de los países de la base de datos y los normaliza."""
    paises = {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM public.countries")
        rows = cur.fetchall()
        for row in rows:
            pais_id = row[0]
            nombre_pais = row[1]
            nombre_pais_normalizado = normalizar_nombre(nombre_pais)
            paises[nombre_pais_normalizado] = (pais_id, nombre_pais)  # Guarda el ID y el nombre original
        cur.close()
    except psycopg2.Error as e:
        print(f"Error al consultar la base de datos: {e}")
    return paises

def obtener_ciudad_id(conn, nombre_ciudad, pais_id):
    """Obtiene el ID de la ciudad de la base de datos, dado el nombre de la ciudad y el ID del país."""
    ciudad_id = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM public.cities WHERE country_id = %s AND name = %s", (pais_id, nombre_ciudad))
        result = cur.fetchone()
        if result:
            ciudad_id = result[0]
        cur.close()
    except psycopg2.Error as e:
        print(f"Error al consultar la base de datos para la ciudad: {e}")
    return ciudad_id

def cerrar_db(conn):
    """Cierra la conexión a la base de datos."""
    if conn:
        conn.close()
        print("Conexión a la base de datos cerrada.")

# --- Funciones de Extracción de Datos de Wikipedia ---
def obtener_html(url):
    """Obtiene el contenido HTML de una página web."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error al acceder a la página: {e}")
        return None

def parsear_tabla_aeropuertos(html, paises, conn):
    """Extrae datos de aeropuertos de una página de Wikipedia."""
    soup = BeautifulSoup(html, 'html.parser')
    tablas = soup.find_all('table', {'class': 'wikitable'})
    if not tablas:
        print("No se encontraron tablas con la clase 'wikitable' en la página.")
        return []

    datos_aeropuertos = []
    for tabla in tablas:
        filas = tabla.find_all('tr')

        for fila in filas[1:]:
            celdas = fila.find_all('td')
            if len(celdas) >= 4:
                codigo_iata = celdas[0].text.strip() if celdas[0].text else ""

                if len(celdas) > 4:
                    nombre = celdas[1].text.strip() if celdas[1].text else ""
                    sirve_a = celdas[2].text.strip() if celdas[2].text else ""
                    nombre_pais = celdas[4].text.strip() if celdas[4].text else ""
                else:
                    nombre = ""
                    sirve_a = celdas[1].text.strip() if celdas[1].text else ""
                    nombre_pais = celdas[3].text.strip() if celdas[3].text else ""

                nombre_pais = limpiar_nombre_pais(nombre_pais)

                pais_id = None
                ciudad_id = None

                nombre_pais_normalizado = normalizar_nombre(nombre_pais)

                if nombre_pais_normalizado in paises:
                    pais_id, nombre_pais_db = paises[nombre_pais_normalizado]  # Obtén el ID y nombre original de la DB

                    # Obtener ciudad_id de la base de datos
                    ciudad_id = obtener_ciudad_id(conn, sirve_a, pais_id)  # Usa el nombre en español

                datos_aeropuertos.append({
                    'Código IATA': codigo_iata if codigo_iata else "",
                    'Nombre': nombre if nombre else "",
                    'Ciudad': sirve_a if sirve_a else "",
                    'País': nombre_pais if nombre_pais else "",  # Usa el nombre original sin traducción
                    'País ID': pais_id,
                    'Ciudad ID': ciudad_id,
                })

    return datos_aeropuertos

# --- Funciones de Guardado de Datos ---
def guardar_en_json(datos, nombre_archivo):
    """Guarda los datos en un archivo JSON."""
    with open(nombre_archivo, 'w', encoding='utf-8') as archivo_json:
        json.dump(datos, archivo_json, indent=4, ensure_ascii=False)
    print(f"Datos guardados en {nombre_archivo}")

# --- Función Principal ---
def main():
    """Función principal que coordina la extracción, el procesamiento y el guardado de datos."""
    conn = conectar_db()
    if not conn:
        return

    paises = obtener_paises_de_db(conn)
    html = obtener_html(WIKI_URL)

    if html:
        datos_aeropuertos = parsear_tabla_aeropuertos(html, paises, conn)
        guardar_en_json(datos_aeropuertos, JSON_FILE)

    cerrar_db(conn)

# --- Ejecución ---
if __name__ == "__main__":
    main()