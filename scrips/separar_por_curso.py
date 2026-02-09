import pandas as pd

# ===== CONFIGURACIÓN =====
archivo_entrada = "report(17).xlsx"
archivo_salida = "resultado_por_curso.xlsx"
columna_curso = "cur"   # <-- cambia esto si tu columna se llama diferente
# ========================

# Leer Excel
df = pd.read_excel(archivo_entrada)

# Verificar que exista la columna
if columna_curso not in df.columns:
    raise Exception(f"No existe la columna '{columna_curso}'")

# Crear archivo Excel con múltiples hojas
with pd.ExcelWriter(archivo_salida, engine="openpyxl") as writer:
    for curso, grupo in df.groupby(columna_curso):
        nombre_hoja = str(curso)[:31]  # Excel solo permite 31 caracteres
        grupo.to_excel(writer, sheet_name=nombre_hoja, index=False)

print("✅ Archivo generado:", archivo_salida)
