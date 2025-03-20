import pandas as pd
from sqlalchemy import create_engine
import os

# Configuración de la conexión a la base de datos
DB_USER = 'usuario'
DB_PASSWORD = 'contraseña'
DB_HOST = 'host'
DB_PORT = 'puerto'
DB_NAME = 'base datos'


print("Estableciendo conexión a la base de datos...")
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
print("Conexión establecida.")

# Leer el archivo CSV como texto para asegurarnos de separar correctamente las columnas
print("Leyendo el archivo CSV...")
with open('users-teachers.csv', 'r', encoding='utf-8') as file:
    data = file.readlines()

# Separar cada línea y limpiar datos
rows = []
for line in data:
    clean_line = line.strip().replace('"', '')
    rows.append(clean_line.split(','))

# Crear un DataFrame a partir de las filas procesadas
df = pd.DataFrame(rows, columns=['id', 'nombre_completo', 'inss', 'usuario', 'contraseña'])

# Mostrar los primeros registros
print("\nDatos leídos del CSV (primeras filas):")
print(df.head())

# Validar y limpiar datos, omitiendo filas con datos nulos
df.dropna(inplace=True)  # Eliminar filas con valores nulos

# Asegurarse de que los datos son del tipo correcto
df['nombre_completo'] = df['nombre_completo'].str.strip()  # Limpiar espacios
df['inss'] = df['inss'].astype(str).str.strip()  # Asegurarse de que INSS sea una cadena
df['usuario'] = df['usuario'].astype(str).str.strip()  # Asegurarse de que usuario sea una cadena
df['contraseña'] = df['contraseña'].astype(str).str.strip()  # Asegurarse de que contraseña sea una cadena

print("Archivo CSV leído con éxito, se encontraron", len(df), "registros válidos.")
print("\nDatos después de la limpieza (primeras filas):")
print(df.head())

# Iterar a través de cada fila del DataFrame
print("Iniciando la búsqueda de cursos por INSS...")
for index, row in df.iterrows():
    # Obtener y validar el INSS
    inss = row['inss']
    if not isinstance(inss, str) or not inss.strip():
        print(f"INSS no válido en fila {index + 1}: {inss}. Saltando este registro.")
        continue  # Si el INSS no es válido, saltar a la siguiente fila

    print(f"Buscando cursos para INSS: {inss}...")

    # Realizar la consulta SQL
    query = f"""
    SELECT c.nombre AS curs
    FROM rrhh.empleados e
    INNER JOIN general.recintos r ON e.id_recintos = r.id
    INNER JOIN general.recinto_curs rc ON r.id = rc.id_recintos
    INNER JOIN general.curs c ON c.id = rc.id_curs
    WHERE e.inss ILIKE '{inss}'
    """
    curs_result = pd.read_sql(query, engine)

    # Verificar si se encontraron cursos
    if curs_result.empty:
        print(f"No se encontraron cursos para INSS: {inss}.")
    else:
        print(f"Se encontraron {len(curs_result)} cursos para INSS: {inss}.")
        for curs in curs_result['curs']:
            # Crear un DataFrame para cada curso
            curso_df = pd.DataFrame([{
                'nombre_completo': row['nombre_completo'],
                'usuario': row['usuario'],
                'contraseña': row['contraseña']
            }])

            # Asegurarse de que el nombre del archivo sea válido
            safe_curs_name = ''.join(e for e in curs if e.isalnum() or e in (' ', '_')).rstrip()
            file_name = f"{safe_curs_name}.xlsx"

            # Guardar el DataFrame en un archivo Excel
            curso_df.to_excel(file_name, index=False)
            print(f"Archivo creado: {file_name}")

print("Todos los archivos Excel han sido creados con éxito.")