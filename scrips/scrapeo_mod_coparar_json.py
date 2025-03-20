import json
import psycopg2
from unidecode import unidecode

# --- Configuración ---
DB_HOST = "host"
DB_NAME = "base datos"
DB_USER = "usuario"
DB_PASSWORD = "contraseña"
JSON_FILE = "aeropuertos.json"

# --- Funciones de Normalización y Limpieza ---
def normalizar_nombre(nombre):
    """Normaliza un nombre eliminando espacios y caracteres no ASCII, y convirtiéndolo a mayúsculas."""
    nombre = unidecode(nombre).upper().replace(" ", "")
    return nombre

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
            paises[nombre_pais_normalizado] = pais_id  # Guarda solo el ID
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

# --- Funciones de Guardado de Datos ---
def guardar_en_json(datos, nombre_archivo):
    """Guarda los datos en un archivo JSON."""
    with open(nombre_archivo, 'w', encoding='utf-8') as archivo_json:
        json.dump(datos, archivo_json, indent=4, ensure_ascii=False)
    print(f"Datos actualizados y guardados en {nombre_archivo}")

# --- Función Principal ---
def main():
    """Función principal que coordina la carga, el procesamiento y el guardado de datos."""
    conn = conectar_db()
    if not conn:
        return

    paises = obtener_paises_de_db(conn)

    # Cargar datos del archivo JSON
    with open(JSON_FILE, 'r', encoding='utf-8') as archivo_json:
        datos_aeropuertos = json.load(archivo_json)

    # Actualizar País ID y Ciudad ID
    for aeropuerto in datos_aeropuertos:
        if aeropuerto['País ID'] is None:
            pais_id = paises.get(normalizar_nombre(aeropuerto['País']))
            if pais_id is not None:
                aeropuerto['País ID'] = pais_id

        if aeropuerto['Ciudad ID'] is None:
            ciudad_id = obtener_ciudad_id(conn, aeropuerto['Ciudad'], aeropuerto['País ID'])
            if ciudad_id is not None:
                aeropuerto['Ciudad ID'] = ciudad_id

    # Guardar los datos actualizados en el archivo JSON
    guardar_en_json(datos_aeropuertos, JSON_FILE)

    # Cerrar la conexión a la base de datos
    cerrar_db(conn)

def cerrar_db(conn):
    """Cierra la conexión a la base de datos."""
    if conn:
        conn.close()
        print("Conexión a la base de datos cerrada.")

# --- Ejecución ---
if __name__ == "__main__":
    main()