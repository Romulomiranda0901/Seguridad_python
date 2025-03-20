import json
import psycopg2
from unidecode import unidecode

DB_HOST = "host"
DB_NAME = "base datos"
DB_USER = "usuario"
DB_PASSWORD = "contraseña"
INPUT_JSON_FILE = "ciudades_del_mundo.json"
OUTPUT_JSON_FILE = "ciudades_completadas.json"

# --- Funciones de Normalización y Limpieza ---
def normalizar_nombre(nombre):
    return unidecode(nombre).upper().replace(" ", "")

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

def completar_id_pais(data, paises_db):
    for entry in data:
        if entry['id_pais'] is None:
            pais_normalizado = normalizar_nombre(entry['pais'])
            if pais_normalizado in paises_db:
                entry['id_pais'] = paises_db[pais_normalizado]

def main():
    conn = conectar_db()
    if not conn:
        return

    paises_db = obtener_paises_de_db(conn)

    # Cargar el JSON de entrada
    with open(INPUT_JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Completar los ids de país
    completar_id_pais(data, paises_db)

    # Guardar el resultado en un nuevo archivo JSON
    with open(OUTPUT_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"Archivo JSON '{OUTPUT_JSON_FILE}' guardado con éxito.")
    cerrar_db(conn)

if __name__ == "__main__":
    main()