import pandas as pd
import psycopg2
from datetime import datetime

# Configuración de la conexión a la base de datos
db_config = {
    'dbname': 'base datos',
    'user': 'usuario',
    'password': 'contrasena',
    'host': 'hist',
    'port': '5432'
}

# Cargar datos desde un archivo Excel
excel_file_path = 'report.xlsx'
df_excel = pd.read_excel(excel_file_path)

# Filtrar solo los registros donde el estado es "Aprobado"
df_aprobados = df_excel[df_excel['Estado'] == 'Aprobado']

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
    matches = pd.merge(df_aprobados[['Apellidos', 'Nombres', 'Porcentaje Aprobado']], df_db, on=['Apellidos', 'Nombres'], how='inner')

    # Encontrar registros no encontrados
    non_found = pd.merge(df_aprobados[['Apellidos', 'Nombres', 'Porcentaje Aprobado']], df_db, on=['Apellidos', 'Nombres'], how='outer', indicator=True)
    non_found = non_found[non_found['_merge'] == 'left_only']

    # Insertar o actualizar registros en la tabla cliente_beca
    for index, row in matches.iterrows():
        id_cliente = row['id']
        porcentaje = row['Porcentaje Aprobado']

        # Verificar si ya existe un registro en cliente_beca
        cursor.execute("""
            SELECT id FROM tesoreria.cliente_beca
            WHERE id_cliente = %s AND activo = 'SI' AND eliminado = 'NO' AND anyo = %s
        """, (id_cliente,current_year))
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
        print(non_found[['Apellidos', 'Nombres']])
    else:
        print("Todos los registros fueron encontrados.")

except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()