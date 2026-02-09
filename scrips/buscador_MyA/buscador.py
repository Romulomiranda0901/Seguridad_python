import pandas as pd
import psycopg2
import unicodedata
import re
from difflib import SequenceMatcher

# ==================================================
# UTILIDADES
# ==================================================
def normalizar(texto):
    if pd.isna(texto):
        return ""
    texto = str(texto).upper().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = re.sub(r"[^A-Z\s]", "", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto

def safe_str(v):
    return "" if pd.isna(v) else str(v)

def similitud(a, b):
    return SequenceMatcher(None, a, b).ratio()

def similitud_por_partes(excel_texto, bd_texto):
    excel_parts = excel_texto.split()
    bd_parts = bd_texto.split()
    if not excel_parts or not bd_parts:
        return 0
    hits = 0
    for e in excel_parts:
        for b in bd_parts:
            if similitud(e, b) >= 0.80:
                hits += 1
                break
    return hits / len(excel_parts)

def score_total_flexible(nombre_excel, apellido_excel, nombre_bd, apellido_bd):
    score_normal = (
        similitud_por_partes(nombre_excel, nombre_bd) +
        similitud_por_partes(apellido_excel, apellido_bd)
    ) / 2

    score_invertido = (
        similitud_por_partes(nombre_excel, apellido_bd) +
        similitud_por_partes(apellido_excel, nombre_bd)
    ) / 2

    return max(score_normal, score_invertido)

# ==================================================
# MAPEO CUR → ID_CAJA
# ==================================================
MAP_CUR_ID_CAJA = {
    "LEON": [1, 32, 33],
    "CHINANDEGA": [9, 10],
    "ESTELI": [7, 8],
    "MASAYA": [6],
    "MATAGALPA": [13, 14, 15],
    "OCOTAL": [28],
    "JUIGALPA": [4, 5],
    "GRANADA": [12]
}

# ==================================================
# CONFIGURACIÓN
# ==================================================
DB_CONFIG = {
    "host": "192.168.100.4",
    "database": "sisconga",
    "user": "sisconga",
    "password": "1f3aUTSZI5A40lKOQnlp",
    "port": 5432
}

EXCEL_ENTRADA = "ubicacion de aranceles.xlsx"
EXCEL_ENCONTRADOS = "alumnos_encontrados_con_precios.xlsx"
EXCEL_NO_ENCONTRADOS = "alumnos_no_encontrados.xlsx"
ANYO_DESDE = 2025

# ==================================================
# CONEXIÓN
# ==================================================
conn = psycopg2.connect(**DB_CONFIG)

# ==================================================
# 1ª VUELTA – NOMBRE EXACTO
# ==================================================
SQL_PAGOS_NOMBRE = """
SELECT
    c.id AS id_cliente,
    UPPER(unaccent(TRIM(c.nombres)))   AS nombres,
    UPPER(unaccent(TRIM(c.apellidos))) AS apellidos,
    MAX(CASE WHEN rd.id_tipo_pagp = 2 THEN rd.monto END) AS matricula,
    MAX(CASE WHEN rd.id_tipo_pagp = 3 THEN rd.monto END) AS mensualidad
FROM general.clientes c
JOIN tesoreria.recibos r ON r.id_cliente = c.id
JOIN tesoreria.recibos_detalle rd ON rd.id_recibos = r.id
WHERE
    r.anyo >= %s
    AND rd.id_tipo_pagp IN (2,3)
    AND c.activo = 'SI'
    AND c.eliminado = 'NO'
GROUP BY
    c.id, c.nombres, c.apellidos;
"""

pagos = pd.read_sql(SQL_PAGOS_NOMBRE, conn, params=(ANYO_DESDE,))
pagos["key"] = pagos.apply(
    lambda x: normalizar(x["nombres"]) + "|" + normalizar(x["apellidos"]),
    axis=1
)

df = pd.read_excel(EXCEL_ENTRADA)

encontrados = []
no_encontrados = []

for _, row in df.iterrows():

    nombres = [normalizar(safe_str(row.get("nombres_1")))]
    if safe_str(row.get("nombres_2")):
        nombres.append(normalizar(safe_str(row.get("nombres_2"))))

    apellidos = [normalizar(safe_str(row.get("apellidos_1")))]
    if safe_str(row.get("apellidos_2")):
        apellidos.append(normalizar(safe_str(row.get("apellidos_2"))))

    keys = []
    for n in range(1, len(nombres) + 1):
        for a in range(1, len(apellidos) + 1):
            keys.append(" ".join(nombres[:n]) + "|" + " ".join(apellidos[:a]))

    match = pd.DataFrame()
    for k in keys:
        match = pagos[pagos["key"] == k]
        if not match.empty:
            break

    if not match.empty:
        fila = row.copy()
        fila["id_cliente"] = match.iloc[0]["id_cliente"]
        fila["Matricula"] = match.iloc[0]["matricula"]
        fila["Mensualidad"] = match.iloc[0]["mensualidad"]
        fila["Metodo_Encontrado"] = "NOMBRE"
        encontrados.append(fila)
    else:
        no_encontrados.append(row)

# ==================================================
# 2ª VUELTA – RECIBO + CUR + SIMILITUD
# ==================================================
SQL_RECIBO = """
SELECT
    c.id AS id_cliente,
    UPPER(unaccent(TRIM(c.nombres)))   AS nombres,
    UPPER(unaccent(TRIM(c.apellidos))) AS apellidos,
    MAX(CASE WHEN rd.id_tipo_pagp = 2 THEN rd.monto END) AS matricula,
    MAX(CASE WHEN rd.id_tipo_pagp = 3 THEN rd.monto END) AS mensualidad
FROM tesoreria.recibos r
JOIN general.clientes c ON c.id = r.id_cliente
JOIN tesoreria.recibos_detalle rd ON rd.id_recibos = r.id
WHERE
    r.numero_recibo = %s::varchar
    AND r.id_caja = ANY(%s)
    AND rd.id_tipo_pagp IN (2,3)
GROUP BY
    c.id, c.nombres, c.apellidos;
"""

nuevos_encontrados = []
aun_no_encontrados = []

for _, row in pd.DataFrame(no_encontrados).iterrows():

    numero = row.get("num_recibo")
    cur = normalizar(row.get("cur"))

    if pd.isna(numero) or cur not in MAP_CUR_ID_CAJA:
        aun_no_encontrados.append(row)
        continue

    nombre_excel = normalizar(f"{safe_str(row.get('nombres_1'))} {safe_str(row.get('nombres_2'))}")
    apellido_excel = normalizar(f"{safe_str(row.get('apellidos_1'))} {safe_str(row.get('apellidos_2'))}")

    candidatos = pd.read_sql(
        SQL_RECIBO,
        conn,
        params=(numero, MAP_CUR_ID_CAJA[cur])
    )

    encontrado = False
    for _, c in candidatos.iterrows():
        if (
            similitud(nombre_excel, normalizar(c["nombres"])) >= 0.75 and
            similitud(apellido_excel, normalizar(c["apellidos"])) >= 0.75
        ):
            fila = row.copy()
            fila["id_cliente"] = c["id_cliente"]
            fila["Matricula"] = c["matricula"]
            fila["Mensualidad"] = c["mensualidad"]
            fila["Metodo_Encontrado"] = "RECIBO + SIMILITUD"
            nuevos_encontrados.append(fila)
            encontrado = True
            break

    if not encontrado:
        aun_no_encontrados.append(row)

# ==================================================
# 3ª VUELTA – CUR + SIMILITUD FLEXIBLE
# ==================================================
SQL_CUR_FLEX = """
SELECT
    c.id AS id_cliente,
    UPPER(unaccent(TRIM(c.nombres)))   AS nombres,
    UPPER(unaccent(TRIM(c.apellidos))) AS apellidos,
    MAX(CASE WHEN rd.id_tipo_pagp = 2 THEN rd.monto END) AS matricula,
    MAX(CASE WHEN rd.id_tipo_pagp = 3 THEN rd.monto END) AS mensualidad
FROM tesoreria.recibos r
JOIN general.clientes c ON c.id = r.id_cliente
JOIN tesoreria.recibos_detalle rd ON rd.id_recibos = r.id
WHERE
    r.anyo >= %s
    AND r.id_caja = ANY(%s)
    AND rd.id_tipo_pagp IN (2,3)
GROUP BY
    c.id, c.nombres, c.apellidos;
"""

tercera_encontrados = []
final_no_encontrados = []

for _, row in pd.DataFrame(aun_no_encontrados).iterrows():

    cur = normalizar(row.get("cur"))
    if cur not in MAP_CUR_ID_CAJA:
        final_no_encontrados.append(row)
        continue

    nombre_excel = normalizar(f"{safe_str(row.get('nombres_1'))} {safe_str(row.get('nombres_2'))}")
    apellido_excel = normalizar(f"{safe_str(row.get('apellidos_1'))} {safe_str(row.get('apellidos_2'))}")

    candidatos = pd.read_sql(
        SQL_CUR_FLEX,
        conn,
        params=(ANYO_DESDE, MAP_CUR_ID_CAJA[cur])
    )

    posibles = []
    for _, c in candidatos.iterrows():
        score = score_total_flexible(
            nombre_excel,
            apellido_excel,
            normalizar(c["nombres"]),
            normalizar(c["apellidos"])
        )
        if score >= 0.80:
            posibles.append(c)

    if len(posibles) == 1:
        c = posibles[0]
        fila = row.copy()
        fila["id_cliente"] = c["id_cliente"]
        fila["Matricula"] = c["matricula"]
        fila["Mensualidad"] = c["mensualidad"]
        fila["Metodo_Encontrado"] = "CUR + SIMILITUD FLEXIBLE"
        tercera_encontrados.append(fila)
    else:
        final_no_encontrados.append(row)

# ==================================================
# CONSOLIDAR
# ==================================================
df_encontrados = pd.concat([
    pd.DataFrame(encontrados),
    pd.DataFrame(nuevos_encontrados),
    pd.DataFrame(tercera_encontrados)
])

# ==================================================
# UPDATE CLIENTES
# ==================================================
SQL_UPDATE_CLIENTE = """
UPDATE general.clientes
SET
    id_estudiante_academico = %s,
    carnet = %s,
    nombres = %s,
    apellidos = %s,
    updated_at = NOW()
WHERE id = %s;
"""

cursor = conn.cursor()
total_actualizados = 0

for _, row in df_encontrados.iterrows():

    if pd.isna(row.get("id_academico")):
        continue

    nombres_excel = normalizar(
        f"{safe_str(row.get('nombres_1'))} {safe_str(row.get('nombres_2'))}"
    )
    apellidos_excel = normalizar(
        f"{safe_str(row.get('apellidos_1'))} {safe_str(row.get('apellidos_2'))}"
    )

    cursor.execute(
        SQL_UPDATE_CLIENTE,
        (
            int(row["id_academico"]),
            safe_str(row.get("carnet")),
            nombres_excel,
            apellidos_excel,
            int(row["id_cliente"])
        )
    )

    total_actualizados += 1

conn.commit()
cursor.close()
conn.close()

# ==================================================
# EXPORTAR
# ==================================================
df_encontrados.to_excel(EXCEL_ENCONTRADOS, index=False)
pd.DataFrame(final_no_encontrados).to_excel(EXCEL_NO_ENCONTRADOS, index=False)

print("✔ Proceso terminado correctamente")
print("✔ Total encontrados:", len(df_encontrados))
print("✔ Total no encontrados:", len(final_no_encontrados))
print("✔ Total clientes actualizados:", total_actualizados)
