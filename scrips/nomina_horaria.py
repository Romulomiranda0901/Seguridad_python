import pandas as pd
import psycopg2

excel_file_path = 'Reporte(1).xlsx'
df = pd.read_excel(excel_file_path)

conn = psycopg2.connect(
    dbname='siscoga',
    user='postgres',
    password='postgres',
    host='192.168.100.36',
    port='5432'
)

cur = conn.cursor()
registros_coincidentes = []

for index, row in df.iterrows():
    inss = row['N° INSS']
    consulta = """
    SELECT id FROM rrhh.empleados
    WHERE inss = %s
    AND (primer_nombre || ' ' || segundo_nombre) = %s
    AND (primer_apellido || ' ' || segundo_apellido) = %s;
    """

    cur.execute(consulta, (inss, row['NOMBRES'], row['APELLIDOS']))
    resultado = cur.fetchone()

    if resultado:
        id_empleado = resultado[0]
        horas_sin_prestaciones = row['Horas Sin Prestaciones']
        registros_coincidentes.append((id_empleado, horas_sin_prestaciones))

for id_empleado, horas in registros_coincidentes:
    consulta_actualizacion = """
    UPDATE rrhh.nomina_detalles
    SET salario_bruto = %s
    WHERE id_empleados = %s AND id_nomina = 122;
    """
    cur.execute(consulta_actualizacion, (horas, id_empleado))

conn.commit()
cur.close()
conn.close()

print("Registros coincidentes:", registros_coincidentes)