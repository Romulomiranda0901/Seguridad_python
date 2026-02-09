#!/usr/bin/env python3
"""
Inserta registros desde un Excel a temporal.control_docente
Requisitos: pandas, openpyxl, psycopg2-binary
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import logging
import sys
from datetime import date

# --- CONFIGURACIÓN: cambia estos valores a los de tu BD ---
DB_HOST = "192.168.100.4"
DB_PORT = 5432
DB_NAME = "tu_basedatos"
DB_USER = "tu_usuario"
DB_PASSWORD = "tu_password"

# Ruta al Excel (ajusta si tu archivo está en otra ubicación)
EXCEL_PATH = "control Docentes.xlsx"  # o "control_docentes.xlsx"
SHEET_NAME = 0  # puedes poner nombre de la hoja si lo prefieres

# Archivo donde guardamos filas que no pudieron procesarse
NOT_FOUND_CSV = "empleados_no_encontrados.csv"

# Logging básico
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def normalizar_nombre(s):
    if pd.isna(s):
        return ""
    # quitar espacios dobles y bordes
    return " ".join(str(s).strip().split())

def buscar_id_empleado(cur, inss, cedula, nombre_completo):
    """
    Busca el id del empleado en rrhh.empleados.
    Retorna id (int) o None si no existe.
    """
    sql = """
    SELECT id
    FROM rrhh.empleados
    WHERE inss = %s
      AND cedula = %s
      AND concat_ws(' ', primer_nombre, segundo_nombre, primer_apellido, segundo_apellido) = %s
    LIMIT 1;
    """
    cur.execute(sql, (inss, cedula, nombre_completo))
    row = cur.fetchone()
    return row[0] if row else None

def insertar_control(cur, id_empleado, horas_170, horas_360, horas_366, tutorias, id_mes, anyo):
    """
    Inserta un registro en temporal.control_docente.
    """
    sql = """
    INSERT INTO temporal.control_docente
    (id_empleado, horas_170, horas_360, tutorias, id_mes, anyo, horas_366, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.execute(sql, (id_empleado, horas_170, horas_360, tutorias, id_mes, anyo, horas_366, date.today()))

def main():
    # 1) leer excel
    logging.info("Leyendo Excel: %s", EXCEL_PATH)
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype=str)
    except Exception as e:
        logging.error("Error leyendo Excel: %s", e)
        sys.exit(1)

    # Normalizar nombres de columnas (para acomodarnos a variaciones)
    df.columns = [c.strip() for c in df.columns]

    # Columnas esperadas — si no existen, el script trata de mapear nombres comunes
    # Esperamos: inss, cedula, Nombre Completo, id_mes, anyo, horas_170, horas_360, horas_366, tutorias
    # Hacemos un mapeo flexible:
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if "inss" in lc:
            col_map["inss"] = c
        elif "ced" in lc:
            col_map["cedula"] = c
        elif "nombre" in lc and "complet" in lc:
            col_map["nombre_completo"] = c
        elif "id_mes" in lc or "mes" == lc:
            col_map["id_mes"] = c
        elif "anyo" in lc or "año" in lc or "anyo" in lc:
            col_map["anyo"] = c
        elif "170" in lc:
            col_map["horas_170"] = c
        elif "360" in lc:
            col_map["horas_360"] = c
        elif "366" in lc:
            col_map["horas_366"] = c
        elif "tutor" in lc:
            col_map["tutorias"] = c

    # Verificamos al menos las columnas mínimas
    missing = [k for k in ("inss", "cedula", "nombre_completo", "id_mes", "anyo") if k not in col_map]
    if missing:
        logging.error("Faltan columnas obligatorias en el Excel: %s", missing)
        logging.error("Columnas detectadas: %s", df.columns.tolist())
        sys.exit(1)

    # 2) Conectar a la BD
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
    except Exception as e:
        logging.error("Error conectando a la base de datos: %s", e)
        sys.exit(1)

    cur = conn.cursor()
    no_encontrados = []

    try:
        row_count = 0
        inserted_count = 0
        for idx, row in df.iterrows():
            row_count += 1
            inss = row[col_map["inss"]]
            cedula = row[col_map["cedula"]]
            nombre_raw = row[col_map["nombre_completo"]]
            nombre = normalizar_nombre(nombre_raw)

            # Convertir valores numéricos (si vienen como strings) y manejar nulos
            def to_int_safe(x):
                if pd.isna(x) or x == "":
                    return None
                try:
                    # puede tener decimales en Excel -> convertir a float y luego int
                    return int(float(str(x).strip()))
                except Exception:
                    return None

            horas_170 = to_int_safe(row[col_map.get("horas_170")]) if col_map.get("horas_170") else None
            horas_360 = to_int_safe(row[col_map.get("horas_360")]) if col_map.get("horas_360") else None
            horas_366 = to_int_safe(row[col_map.get("horas_366")]) if col_map.get("horas_366") else None
            tutorias  = to_int_safe(row[col_map.get("tutorias")]) if col_map.get("tutorias") else None
            id_mes    = to_int_safe(row[col_map["id_mes"]])
            anyo      = to_int_safe(row[col_map["anyo"]])

            # Buscar empleado
            emp_id = buscar_id_empleado(cur, str(inss).strip(), str(cedula).strip(), nombre)
            if emp_id is None:
                logging.warning("Empleado NO encontrado (fila %s): INSS=%s, CED=%s, NOMBRE=%s", idx+1, inss, cedula, nombre)
                no_encontrados.append({
                    "fila": idx+1,
                    "inss": inss,
                    "cedula": cedula,
                    "nombre_completo": nombre,
                    "id_mes": id_mes,
                    "anyo": anyo,
                    "horas_170": horas_170,
                    "horas_360": horas_360,
                    "horas_366": horas_366,
                    "tutorias": tutorias
                })
                continue

            # Inserción
            try:
                insertar_control(cur, emp_id, horas_170, horas_360, horas_366, tutorias, id_mes, anyo)
                inserted_count += 1
            except Exception as e_ins:
                logging.error("Error insertando fila %s (empleado %s): %s", idx+1, emp_id, e_ins)
                conn.rollback()
                # decidir si continuar o no; aquí seguimos
            else:
                # OK, confirmamos
                conn.commit()

        logging.info("Filas leídas: %d, insertadas: %d, no encontradas: %d", row_count, inserted_count, len(no_encontrados))

        # Guardar no encontrados en CSV para revisión
        if no_encontrados:
            df_nf = pd.DataFrame(no_encontrados)
            df_nf.to_csv(NOT_FOUND_CSV, index=False)
            logging.info("Listado de empleados no encontrados guardado en: %s", NOT_FOUND_CSV)

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
