import psycopg2

# Configuración de la conexión al primer servidor PostgreSQL (fuente)
source_postgres_config = {
    'host': 'your_source_postgres_host',
    'user': 'your_source_postgres_user',
    'password': 'your_source_postgres_password',
    'database': 'your_source_postgres_database'
}

# Configuración de la conexión al segundo servidor PostgreSQL (destino)
destination_postgres_config = {
    'host': 'your_destination_postgres_host',
    'user': 'your_destination_postgres_user',
    'password': 'your_destination_postgres_password',
    'database': 'your_destination_postgres_database'
}

# Conectar al servidor PostgreSQL fuente
source_conn = psycopg2.connect(**source_postgres_config)
source_cursor = source_conn.cursor()

# Conectar al servidor PostgreSQL destino
destination_conn = psycopg2.connect(**destination_postgres_config)
destination_cursor = destination_conn.cursor()

# Lista de tablas a procesar
tables_to_process = ['table1', 'table2', 'table3']  # Agrega aquí las tablas que deseas procesar

try:
    for table in tables_to_process:
        # Extraer datos del servidor fuente
        source_cursor.execute(f"SELECT * FROM {table}")

        # Obtener los nombres de las columnas
        column_names = [desc[0] for desc in source_cursor.description]
        rows = source_cursor.fetchall()

        for row in rows:
            # Generar una consulta para verificar si el registro ya existe en el destino
            existing_query = f"SELECT 1 FROM {table} WHERE id = %s"  # Cambia 'id' por la clave primaria adecuada
            destination_cursor.execute(existing_query, (row[0],))  # Asumiendo que la clave primaria está en la primera columna

            # Si no existe, insertar el registro
            if destination_cursor.fetchone() is None:
                # Generar la consulta de inserción dinámica
                placeholders = ', '.join(['%s'] * len(column_names))  # Crea una cadena de marcadores (%s, %s, %s...)
                insert_query = f"INSERT INTO {table} ({', '.join(column_names)}) VALUES ({placeholders})"
                destination_cursor.execute(insert_query, row)

        # Confirmar la transacción en el servidor destino
        destination_conn.commit()
        print(f"Datos de la tabla {table} transferidos exitosamente.")

except Exception as e:
    print("Ocurrió un error:", e)
    destination_conn.rollback()

finally:
    # Cerrar las conexiones
    source_cursor.close()
    source_conn.close()
    destination_cursor.close()
    destination_conn.close()