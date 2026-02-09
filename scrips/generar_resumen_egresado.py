import pandas as pd

ARCHIVO_ENTRADA = "reporta-notas-egresados-2025.xlsx"
ARCHIVO_SALIDA = "reporte_final.xlsx"

COLUMNA_CARRERA = "Carrera"
COLUMNA_NIVEL = "Nivel de formación"
COLUMNA_AREA = "Area de conocimiento"
COLUMNA_SEXO = "Sexo"
COLUMNA_RANGO = "Rango"

df = pd.read_excel(ARCHIVO_ENTRADA)
df.columns = df.columns.str.strip()

df[COLUMNA_RANGO] = df[COLUMNA_RANGO].astype(str).str.upper()
df[COLUMNA_SEXO] = df[COLUMNA_SEXO].astype(str).str.upper()

def normalizar_rango(x):
    if "R1" in x:
        return "Menos de 60"
    if "R2" in x:
        return "60-75"
    if "R3" in x:
        return "76-89"
    if "R4" in x:
        return "90-100"
    return None

df["RANGO_OK"] = df[COLUMNA_RANGO].apply(normalizar_rango)

rangos = ["Menos de 60", "60-75", "76-89", "90-100"]

resultado = []

for (carrera, nivel, area), g in df.groupby(
        [COLUMNA_CARRERA, COLUMNA_NIVEL, COLUMNA_AREA]):

    fila = {
        "Nombre de la carrera": carrera,
        "Nivel de formación": nivel,
        "Área del conocimiento": area,
    }

    for r in rangos:
        sub = g[g["RANGO_OK"] == r]

        femenino = len(sub[sub[COLUMNA_SEXO].str.contains("MUJER")])
        masculino = len(sub[sub[COLUMNA_SEXO].str.contains("HOMBRE")])

        fila[f"{r} Femenino"] = femenino
        fila[f"{r} Masculino"] = masculino
        fila[f"{r} Total"] = femenino + masculino

    resultado.append(fila)

salida = pd.DataFrame(resultado)

columnas = [
    "Nombre de la carrera",
    "Nivel de formación",
    "Área del conocimiento",

    "Menos de 60 Femenino",
    "Menos de 60 Masculino",
    "Menos de 60 Total",

    "60-75 Femenino",
    "60-75 Masculino",
    "60-75 Total",

    "76-89 Femenino",
    "76-89 Masculino",
    "76-89 Total",

    "90-100 Femenino",
    "90-100 Masculino",
    "90-100 Total",
]

salida = salida[columnas]

salida.to_excel(ARCHIVO_SALIDA, index=False)

print("✅ REPORTE GENERADO:", ARCHIVO_SALIDA)
