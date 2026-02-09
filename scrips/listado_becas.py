import pandas as pd
import unidecode

# === Cargar UNCPGGL León ===
uncpggl = pd.read_excel("UNCPGGL León.xlsx")

# === Intentar cargar report(1) ===
try:
    report = pd.read_excel("report.xlsx", engine="openpyxl")
except:
    try:
        report = pd.read_excel("report.xls", engine="xlrd")
    except Exception as e:
        raise Exception(f"No se pudo abrir report(1).xls/.xlsx. Error original: {e}")

# === Función para limpiar texto ===
def limpiar_texto(texto):
    if pd.isna(texto):
        return ""
    return unidecode.unidecode(str(texto)).replace(" ", "").lower()

# === Crear columna concatenada ===
report["nombre_completo"] = (
    report["nombres_1"].fillna("") + " " +
    report["nombres_2"].fillna("") + " " +
    report["apellidos_1"].fillna("") + " " +
    report["apellidos_2"].fillna("")
).apply(limpiar_texto)

# === Limpiar nombres en el otro Excel ===
uncpggl["nombre_limpio"] = uncpggl["NOMBRE"].apply(limpiar_texto)

# === Cruce de datos ===
encontrados = uncpggl.merge(
    report[["nombre_completo", "numero", "numero_emergencia"]],
    left_on="nombre_limpio",
    right_on="nombre_completo",
    how="inner"
)

no_encontrados = uncpggl[~uncpggl["nombre_limpio"].isin(encontrados["nombre_limpio"])]

# === Limpiar columnas temporales ===
encontrados.drop(columns=["nombre_limpio", "nombre_completo"], inplace=True, errors='ignore')
no_encontrados.drop(columns=["nombre_limpio"], inplace=True, errors='ignore')

# === Guardar resultados ===
encontrados.to_excel("encontrado.xlsx", index=False)
no_encontrados.to_excel("no_encontrado.xlsx", index=False)

print("✅ Archivos generados correctamente: encontrado.xlsx y no_encontrado.xlsx")
