import pandas as pd
from sqlalchemy import create_engine
import os

DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_HOST = '192.168.100.36'
DB_PORT = '5432'
DB_NAME = 'siscoga'

print("Estableciendo conexión a la base de datos...")
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
print("Conexión establecida.")

print("Leyendo el archivo CSV...")
with open('users-teachers.csv', 'r', encoding='utf-8') as file:
    data = file.readlines()

rows = []
for line in data:
    clean_line = line.strip().replace('"', '')
    rows.append(clean_line.split(','))

df = pd.DataFrame(rows, columns=['id', 'nombre_completo', 'inss', 'usuario', 'contraseña'])

print("\nDatos leídos del CSV (primeras filas):")
print(df.head())

df.dropna(inplace=True)

df['nombre_completo'] = df['nombre_completo'].str.strip()
df['inss'] = df['inss'].astype(str).str.strip()
df['usuario'] = df['usuario'].astype(str).str.strip()
df['contraseña'] = df['contraseña'].astype(str).str.strip()

print("Archivo CSV leído con éxito, se encontraron", len(df), "registros válidos.")
print("\nDatos después de la limpieza (primeras filas):")
print(df.head())

cursos_dict = {}

print("Iniciando la búsqueda de cursos por INSS...")
for index, row in df.iterrows():
    inss = row['inss']
    if not isinstance(inss, str) or not inss.strip():
        print(f"INSS no válido en fila {index + 1}: {inss}. Saltando este registro.")
        continue

    print(f"Buscando cursos para INSS: {inss}...")

    query = f"""
    SELECT c.nombre AS curs
    FROM rrhh.empleados e
    INNER JOIN general.recintos r ON e.id_recintos = r.id
    INNER JOIN general.recinto_curs rc ON r.id = rc.id_recintos
    INNER JOIN general.curs c ON c.id = rc.id_curs
    WHERE e.inss ILIKE '{inss}'
    """
    curs_result = pd.read_sql(query, engine)

    if curs_result.empty:
        print(f"No se encontraron cursos para INSS: {inss}.")
    else:
        for curs in curs_result['curs']:
            if curs not in cursos_dict:
                cursos_dict[curs] = []
            cursos_dict[curs].append({
                'nombre_completo': row['nombre_completo'],
                'usuario': row['usuario'],
                'contraseña': row['contraseña']
            })

for curs, empleados in cursos_dict.items():
    curso_df = pd.DataFrame(empleados)
    safe_curs_name = ''.join(e for e in curs if e.isalnum() or e in (' ', '_')).rstrip()
    file_name = f"{safe_curs_name}.xlsx"
    curso_df.to_excel(file_name, index=False)
    print(f"Archivo creado: {file_name}")

print("Todos los archivos Excel han sido creados con éxito.")