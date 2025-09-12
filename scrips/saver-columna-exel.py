import pandas as pd

def inspect_excel(file_path):
    # Cargar el archivo Excel y visualizar los nombres de las columnas en cada hoja
    xls = pd.ExcelFile(file_path)

    for sheet in xls.sheet_names:
        print(f"Columnas en la hoja '{sheet}':")
        df = pd.read_excel(xls, sheet_name=sheet)
        print(df.columns.tolist())
        print()  # Línea en blanco para mejorar la legibilidad

# Ruta al archivo de Excel
file_path = 'nombres_no_encontrados.xlsx'  # Asegúrate de que la ruta sea correcta

# Inspeccionar el archivo Excel
inspect_excel(file_path)