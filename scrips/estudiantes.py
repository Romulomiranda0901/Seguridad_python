import pandas as pd
import os

# Cargar el archivo Excel original
input_file = 'ruta_del_archivo.xlsx'  # Cambia esto por la ruta de tu archivo
df = pd.read_excel(input_file)

# Obtener la lista de CURs únicos
curs = df['cur'].unique()

# Crear una carpeta para almacenar los nuevos archivos Excel
output_folder = 'salida_archivos'
os.makedirs(output_folder, exist_ok=True)

# Iterar sobre cada CUR
for cur in curs:
    cur_df = df[df['cur'] == cur]

    # Obtener la lista de áreas de conocimiento únicas
    areas_conocimiento = cur_df['area_conocimiento'].unique()

    # Crear un archivo Excel para cada CUR
    cur_file = os.path.join(output_folder, f'{cur}.xlsx')
    with pd.ExcelWriter(cur_file, engine='openpyxl') as writer:
        # Iterar sobre cada área de conocimiento
        for area in areas_conocimiento:
            area_df = cur_df[cur_df['area_conocimiento'] == area]

            # Ordenar por carrera y luego por año de estudio
            area_df = area_df.sort_values(by=['carrera', 'anyo_estudio'])

            # Escribir el DataFrame en una hoja de Excel
            area_df.to_excel(writer, sheet_name=area[:31], index=False)  # Limitar nombre de la hoja a 31 caracteres

print("Archivos Excel creados exitosamente en la carpeta 'salida_archivos'.")