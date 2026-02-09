import pandas as pd

# Rutas de los archivos CSV
csv_file_path_1 = 'NOTAS FALTANTES (1).csv'


# Cargar los archivos CSV
data1 = pd.read_csv(csv_file_path_1)


# Guardar como archivo Excel
excel_file_path = 'converted.xlsx'
with pd.ExcelWriter(excel_file_path) as writer:
    data1.to_excel(writer, sheet_name='NOTAS FALTANTES', index=False)


print(f'Los archivos han sido convertidos y guardados como {excel_file_path}')