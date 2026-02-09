import pandas as pd

archivo_2025 = "Matricula 2025.xlsx"
archivo_2026 = "matricula 2026.xlsx"

columna_id = "identificacion"
columna_cur = "CUR"

salida = "No_matriculados_2026_por_CUR.xlsx"

df_2025 = pd.read_excel(archivo_2025)
df_2026 = pd.read_excel(archivo_2026)

# normalizar columnas
df_2025.columns = df_2025.columns.str.lower().str.strip()
df_2026.columns = df_2026.columns.str.lower().str.strip()

columna_id = columna_id.lower()
columna_cur = columna_cur.lower()

# normalizar valores
df_2025[columna_id] = df_2025[columna_id].astype(str).str.strip()
df_2026[columna_id] = df_2026[columna_id].astype(str).str.strip()

# filtrar no matriculados
no_matriculados = df_2025[
    ~df_2025[columna_id].isin(df_2026[columna_id])
]

# exportar por CUR
with pd.ExcelWriter(salida, engine="openpyxl") as writer:
    for cur, grupo in no_matriculados.groupby(columna_cur):
        grupo.to_excel(writer, sheet_name=str(cur)[:31], index=False)

print("===================================")
print("ARCHIVO CREADO:", salida)
print("TOTAL NO MATRICULADOS:", len(no_matriculados))
print("===================================")
