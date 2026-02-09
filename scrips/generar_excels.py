import pandas as pd
import os

# Ruta del archivo original
file_path = "report.xlsx"   # <-- cámbialo por la ruta de tu archivo

# Carpeta de salida
output_dir = "output_excels"
os.makedirs(output_dir, exist_ok=True)

# Leer el archivo
df = pd.read_excel(file_path, sheet_name="Worksheet")

# Filtrar columnas necesarias y crear nombre completo
df_filtered = df[[
    "cur", "carrera", "carnet", "nombres_1", "nombres_2",
    "apellidos_1", "apellidos_2", "anyo_estudio", "turno",
    "modalidad", "genero", "departamento_residencia", "direccion_residencia"
]].copy()

# Crear columna nombre completo
df_filtered["nombre_completo"] = (
    df_filtered["nombres_1"].fillna("") + " " +
    df_filtered["nombres_2"].fillna("") + " " +
    df_filtered["apellidos_1"].fillna("") + " " +
    df_filtered["apellidos_2"].fillna("")
).str.strip()

# Reordenar columnas finales
df_filtered = df_filtered[[
    "carnet", "nombre_completo", "anyo_estudio", "turno",
    "modalidad", "genero", "departamento_residencia",
    "direccion_residencia", "cur", "carrera"
]]

# Generar un archivo Excel por cada CUR
for cur, df_cur in df_filtered.groupby("cur"):
    cur_file = os.path.join(output_dir, f"{cur}.xlsx")
    with pd.ExcelWriter(cur_file, engine="openpyxl") as writer:
        # Cada carrera será una hoja
        for carrera, df_carrera in df_cur.groupby("carrera"):
            df_carrera.drop(columns=["cur", "carrera"]).to_excel(
                writer, sheet_name=str(carrera)[:31], index=False
            )

print(f"Archivos generados en la carpeta: {output_dir}")
