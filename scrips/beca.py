import pandas as pd
import psycopg2
import requests
from datetime import datetime

# Configuración de la conexión a la base de datos
db_config = {
    'dbname': 'siscoga',
    'user': 'postgres',
    'password': 'postgres',
    'host': '192.168.100.36',
    'port': '5432'
}

# URL de la API
api_url = "https://academico.uncpggl.edu.ni:3000/api/grants?token=6806|IesCukVQy4PLUqnMqIBHMuFCSaarT0h89WOsFo3Wce4c0b9c"

# Obtener datos de la API (desactivando la verificación del certificado SSL)
response = requests.get(api_url, verify=False)
data = response.json()

# Convertir los datos de la API a un DataFrame
df_aprobados = pd.DataFrame(data)

# Filtrar solo los registros donde el estado es "Aprobado"
df_aprobados = df_aprobados[df_aprobados['status_name'] == 'Aprobado']

# Obtener el año actual
current_year = datetime.now().year
semester = 1  # Establecer semestre en 1
usuario_creador = 1  # ID del usuario creador

# Conectar a la base de datos PostgreSQL
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Obtener los apellidos y nombres de la tabla clientes
    cursor.execute("SELECT id, apellidos, nombres FROM general.clientes WHERE activo = 'SI' AND eliminado = 'NO'")
    rows_db = cursor.fetchall()

    # Convertir los datos de la base de datos a un DataFrame
    df_db = pd.DataFrame(rows_db, columns=['id', 'Apellidos', 'Nombres'])

    # Comparar los DataFrames, buscando coincidencias en apellidos y nombres
    matches = pd.merge(df_aprobados[['lastnames', 'firstnames', 'approved_percentage']], df_db, left_on=['lastnames', 'firstnames'], right_on=['Apellidos', 'Nombres'], how='inner')

    # Encontrar registros no encontrados
    non_found = pd.merge(df_aprobados[['lastnames', 'firstnames', 'approved_percentage']], df_db, left_on=['lastnames', 'firstnames'], right_on=['Apellidos', 'Nombres'], how='outer', indicator=True)
    non_found = non_found[non_found['_merge'] == 'left_only']

    # Insertar o actualizar registros en la tabla cliente_beca
    for index, row in matches.iterrows():
        id_cliente = row['id']
        porcentaje = row['approved_percentage']

        # Verificar si ya existe un registro en cliente_beca
        cursor.execute("""
            SELECT id FROM tesoreria.cliente_beca
            WHERE id_cliente = %s AND activo = 'SI' AND eliminado = 'NO' AND anyo = %s
        """, (id_cliente, current_year))
        existing_record = cursor.fetchone()

        if existing_record:
            # Actualizar el registro existente
            cursor.execute("""
                UPDATE tesoreria.cliente_beca
                SET porcentaje = %s, semectre_beca = %s, anyo = %s, usuario_creador = %s
                WHERE id_cliente = %s
            """, (porcentaje, semester, current_year, usuario_creador, id_cliente))
        else:
            # Insertar nuevo registro
            cursor.execute("""
                INSERT INTO tesoreria.cliente_beca (id_cliente, porcentaje, semectre_beca, activo, eliminado, usuario_creador, anyo)
                VALUES (%s, %s, %s, 'SI', 'NO', %s, %s)
            """, (id_cliente, porcentaje, semester, usuario_creador, current_year))

    conn.commit()  # Confirmar los cambios

    # Mostrar resultados
    print(f"Registros procesados: {len(matches)}")
    print("Inserciones y actualizaciones completadas.")

    # Mostrar registros no encontrados
    print("\nRegistros no encontrados:")
    if not non_found.empty:
        print(non_found[['lastnames', 'firstnames']])
    else:
        print("Todos los registros fueron encontrados.")

except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()