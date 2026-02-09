import pyodbc
import mysql.connector
import random

# ------------------ CONFIG ------------------

SQLSERVER_CONN = (
   "DRIVER={ODBC Driver 18 for SQL Server};"
   "SERVER=localhost,1433;"
   "DATABASE=Credifacv1_viernes;"
   "Trusted_Connection=yes;"
   "TrustServerCertificate=yes;"


)

MYSQL_CONN = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "credifac"
}

# ================= HELPERS =================

def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None

def dividir_nombre(t):
    t = clean(t)
    if not t:
        return None, None
    p = t.split()
    return p[0], " ".join(p[1:]) if len(p) > 1 else None

def dividir_dir(t, lim=255):
    t = clean(t)
    if not t:
        return None, ""
    return t[:lim], t[lim:] if len(t) > lim else ""

def tel(t):
    t = clean(t)
    if not t:
        return None
    return t.replace(" ", "").replace("-", "")

def sexo_val(v):
    if not v:
        return 1
    v = str(v).upper()
    return 1 if v.startswith("M") else 2

# ================= CONEXIONES =================

sqlserver = pyodbc.connect(SQLSERVER_CONN)
mysql = mysql.connector.connect(**MYSQL_CONN)

ss = sqlserver.cursor()
ss_update = sqlserver.cursor()
my = mysql.cursor(dictionary=True)

# ================= CLIENTES =================

ss.execute("""
SELECT
    c.codclte,c.nombres,c.apellidos,c.cedula,c.telefono,c.telefono1,
    c.direccion,c.nota,c.codvende,c.f_reg,c.f_edit,
    c.Longitud,c.Latitud,c.Codsucursal,c.Rtn,c.sexo,c.f_nac,
    v.cedula AS cedula_vendedor
FROM Clientes c
LEFT JOIN vendedores v ON v.codvende=c.codvende
WHERE ISNULL(c.migrate,0)=0
""")

rows = ss.fetchall()

migrados=0
fallidos=0

for row in rows:

    try:

        persona_id=f"DNI-{row.cedula}"

        # ---- vendedor user ----
        my.execute("SELECT id FROM users WHERE persona_id=CONCAT('DNI-',%s)",(row.cedula_vendedor,))
        u=my.fetchone()
        user_vendedor = u["id"] if u else 1

        # ---- personas ----
        my.execute("SELECT id FROM personas WHERE id=%s",(persona_id,))
        if not my.fetchone():
            my.execute("""
            INSERT INTO personas
            (id,numero_documento,tipo_persona,pais_nacimiento_id,tipo_documento_id,estado,
             created_at,updated_at,user_id_created,user_id_updated)
            VALUES (%s,%s,1,'HND','DNI',1,%s,%s,%s,%s)
            """,(persona_id,row.cedula,row.f_reg,row.f_edit,user_vendedor,user_vendedor))

        # ---- personas_naturales ----
        pn,sn=dividir_nombre(row.nombres)
        pa,sa=dividir_nombre(row.apellidos)

        my.execute("SELECT persona_id FROM personas_naturales WHERE persona_id=%s",(persona_id,))
        if not my.fetchone():
            my.execute("""
            INSERT INTO personas_naturales
            (persona_id,primer_nombre,segundo_nombre,primer_apellido,segundo_apellido,
             sexo,fecha_nacimiento,estado_civil,rtn_numero,
             created_at,updated_at,user_id_created,user_id_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s,%s,%s)
            """,(persona_id,pn,sn,pa,sa,sexo_val(row.sexo),row.f_nac,row.Rtn,
                 row.f_reg,row.f_edit,user_vendedor,user_vendedor))

        # ---- telefonos ----
        for t in [tel(row.telefono),tel(row.telefono1)]:
            if t:
                my.execute("""
                INSERT IGNORE INTO telefonos
                (persona_id,numero,tipo,codigo_verificacion,estado,created_at,updated_at,user_id_created,user_id_updated)
                VALUES (%s,%s,1,%s,1,%s,%s,%s,%s)
                """,(persona_id,t,row.Codsucursal,row.f_reg,row.f_edit,user_vendedor,user_vendedor))

        # ---- comentarios ----
        nota=clean(row.nota)
        if nota:
            my.execute("""
            INSERT IGNORE INTO personas_comentarios
            (persona_id,comentario,created_at,updated_at,user_id_created,user_id_updated)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,(persona_id,nota,row.f_reg,row.f_edit,user_vendedor,user_vendedor))

        # ---- ubicaciones ----
        ubicacion_id=None
        if row.Latitud and row.Longitud:
            my.execute("""
            INSERT INTO ubicaciones
            (latitud,longitud,created_at,updated_at,user_id_created,user_id_updated)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,(row.Latitud,row.Longitud,row.f_reg,row.f_edit,user_vendedor,user_vendedor))
            ubicacion_id=my.lastrowid

        # ---- direcciones ----
        av,ca=dividir_dir(row.direccion)
        if not av:
            av = "SIN DIRECCION"
        my.execute("""
        INSERT INTO direcciones
        (pais_id,tipo,sector_edificio,bloque_piso,casa_apartamento,codigo_empresa_electrica,departamento_id,municipio_id,comunidad_id,barrio_colonia_id,
        ubicacion_id,avenida,calle,created_at,updated_at,user_id_created,user_id_updated)
        VALUES ('HND',1,1,1,1,1,13,1301,1,1,%s,%s,%s,%s,%s,%s,%s)
        """,(ubicacion_id,av,ca,row.f_reg,row.f_edit,user_vendedor,user_vendedor))
        direccion_id=my.lastrowid

        # ---- direcciones_personas ----
        my.execute("""
        INSERT INTO direcciones_personas
        (persona_id,direccion_id,created_at,updated_at,user_id_created,user_id_updated)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,(persona_id,direccion_id,row.f_reg,row.f_edit,user_vendedor,user_vendedor))

        mysql.commit()

        # ---- update sqlserver ----
        ss_update.execute("""
        UPDATE Clientes
        SET migrate=1, personal_id=?
        WHERE codclte=?
        """,(persona_id,row.codclte))

        sqlserver.commit()

        migrados+=1
        print("✔",row.codclte)

    except Exception as e:

        mysql.rollback()
        sqlserver.rollback()
        fallidos+=1
        print("❌",row.codclte,e)

print("\n=== RESUMEN ===")
print("Migrados:",migrados)
print("Fallidos:",fallidos)

mysql.close()
sqlserver.close()