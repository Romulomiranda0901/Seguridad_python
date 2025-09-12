import pandas as pd

# Cargar los archivos Excel
archivo_2016_path = "ARQUITECTURA EN DISEÑO GRÁFICO CON ÉNFASIS EN ARTE DIGITAL PLAN 2016 (1) (1).xlsx"
archivo_2008_path = "ADG_plan2008.xlsx"

# Leer los archivos y limpiar nombres de columnas
archivo_2016 = pd.read_excel(archivo_2016_path, skiprows=16)
archivo_2008 = pd.read_excel(archivo_2008_path, skiprows=3)

# Renombrar columnas
archivo_2016.columns = [
    'Régimen Académico', 'Código', 'Asignatura', 'Tipo Asignatura',
    'Total horas', 'Créditos', 'Pre-Requisito'
]
archivo_2008.columns = [
    'Regimen', 'Codigo clase', 'Nombre clase', 'Tipo Asignatura',
    'Horas Presenciales', 'Horas autestudio', 'Creditos', 'Pre_Requisito'
]

# Eliminar duplicados en archivo 2016
archivo_2016 = archivo_2016.drop_duplicates(subset='Código')

# Crear un diccionario para búsqueda
dict_2016 = archivo_2016.set_index('Código').to_dict('index')

# Mantener un registro de clases no encontradas
clases_no_encontradas = []

# Llenar datos faltantes en archivo_2008
for idx, row in archivo_2008.iterrows():
    # Ignorar filas que contienen 'TOTAL'
    if 'TOTAL' in str(row['Codigo clase']) or 'TOTAL' in str(row['Nombre clase']):
        continue

    codigo = row['Codigo clase']
    # Verificar si alguno de los campos está vacío
    if pd.isna(row['Tipo Asignatura']) or pd.isna(row['Creditos']) or pd.isna(row['Pre_Requisito']):
        # Comprobar si el código existe en el diccionario
        if codigo in dict_2016:
            datos = dict_2016[codigo]
            # Llenar los datos correspondientes
            archivo_2008.at[idx, 'Tipo Asignatura'] = datos.get('Tipo Asignatura', row['Tipo Asignatura'])
            archivo_2008.at[idx, 'Creditos'] = datos.get('Créditos', row['Creditos'])
            archivo_2008.at[idx, 'Pre_Requisito'] = datos.get('Pre-Requisito', row['Pre_Requisito'])
        else:
            clases_no_encontradas.append(codigo)
            print(f"Codigo no encontrado: {codigo}")

# Mensaje informativo sobre clases no encontradas
if clases_no_encontradas:
    print("Clases no encontradas:", clases_no_encontradas)

# Crear una nueva lista para almacenar las filas
nueva_estructura = []

# Llenar la nueva estructura con los datos
for _, row in archivo_2008.iterrows():
    # Ignorar filas que contienen 'TOTAL'
    if 'TOTAL' in str(row['Codigo clase']) or 'TOTAL' in str(row['Nombre clase']):
        continue

    nueva_estructura.append({
        'Regimen': row['Regimen'],
        'Codigo clase': row['Codigo clase'],
        'Nombre clase': row['Nombre clase'],
        'Tipo Asignatura': row['Tipo Asignatura'],
        'Horas Presenciales': row['Horas Presenciales'],
        'Horas autestudio': row['Horas autestudio'],
        'Creditos': row['Creditos'],
        'Pre_Requisito': row['Pre_Requisito']
    })

# Convertir la lista en un DataFrame
nueva_estructura_df = pd.DataFrame(nueva_estructura)

# Guardar en un nuevo archivo Excel
nuevo_archivo_path = "ADG_plan2008_modificado.xlsx"
nueva_estructura_df.to_excel(nuevo_archivo_path, index=False)

print(f"Datos llenados y guardados en '{nuevo_archivo_path}'.")