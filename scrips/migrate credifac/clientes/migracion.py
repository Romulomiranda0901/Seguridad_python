from helpers import *
from config import conectar_sqlserver, conectar_mysql
from tqdm import tqdm


def migrar_clientes():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("Obteniendo clientes pendientes...")

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

    print("Total clientes:", len(rows))

    migrados = 0
    fallidos = 0

    for row in tqdm(rows, desc="Migrando clientes"):

        try:

            # ---------- persona_id unico por cedula ----------
            base_persona_id = f"DNI-{row.cedula}"

            my.execute("""
            SELECT COUNT(*) AS total
            FROM personas
            WHERE id LIKE %s
            """, (base_persona_id + "%",))

            total = my.fetchone()["total"]

            persona_id = base_persona_id if total == 0 else f"{base_persona_id}-{total+1}"

            # ---------- usuario vendedor ----------
            my.execute(
                "SELECT id FROM users WHERE persona_id = CONCAT('DNI-', %s)",
                (row.cedula_vendedor,)
            )

            u = my.fetchone()
            user_vendedor = u["id"] if u else 1

            # ---------- personas ----------
            my.execute("SELECT id FROM personas WHERE id=%s", (persona_id,))
            if not my.fetchone():

                my.execute("""
                INSERT INTO personas
                (id,numero_documento,tipo_persona,pais_nacimiento_id,tipo_documento_id,estado,
                 created_at,updated_at,user_id_created,user_id_updated)
                VALUES (%s,%s,1,'HND','DNI',1,%s,%s,%s,%s)
                """, (
                    persona_id,
                    row.cedula,
                    row.f_reg,
                    row.f_edit,
                    user_vendedor,
                    user_vendedor
                ))

            # ---------- personas_naturales ----------
            pn, sn = dividir_nombre(row.nombres)
            pa, sa = dividir_nombre(row.apellidos)

            my.execute("SELECT persona_id FROM personas_naturales WHERE persona_id=%s", (persona_id,))
            if not my.fetchone():

                my.execute("""
                INSERT INTO personas_naturales
                (persona_id,primer_nombre,segundo_nombre,primer_apellido,segundo_apellido,
                 sexo,fecha_nacimiento,estado_civil,rtn_numero,
                 created_at,updated_at,user_id_created,user_id_updated)
                VALUES (%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s,%s,%s)
                """, (
                    persona_id,
                    pn,
                    sn,
                    pa,
                    sa,
                    sexo_val(row.sexo),
                    row.f_nac,
                    row.Rtn,
                    row.f_reg,
                    row.f_edit,
                    user_vendedor,
                    user_vendedor
                ))

            # ---------- telefonos ----------
            for t in [tel(row.telefono), tel(row.telefono1)]:
                if t:
                    my.execute("""
                    INSERT IGNORE INTO telefonos
                    (persona_id,numero,tipo,codigo_verificacion,estado,
                     created_at,updated_at,user_id_created,user_id_updated)
                    VALUES (%s,%s,1,%s,1,%s,%s,%s,%s)
                    """, (
                        persona_id,
                        t,
                        row.Codsucursal,
                        row.f_reg,
                        row.f_edit,
                        user_vendedor,
                        user_vendedor
                    ))

            # ---------- comentarios ----------
            nota = clean(row.nota)
            if nota:
                my.execute("""
                INSERT IGNORE INTO personas_comentarios
                (persona_id,comentario,created_at,updated_at,user_id_created,user_id_updated)
                VALUES (%s,%s,%s,%s,%s,%s)
                """, (
                    persona_id,
                    nota,
                    row.f_reg,
                    row.f_edit,
                    user_vendedor,
                    user_vendedor
                ))

            # ---------- direcciones ----------
            avenida, calle = dividir_dir(row.direccion)
            if not avenida:
                avenida = "SIN DIRECCION"

            my.execute("""
            INSERT INTO direcciones
            (pais_id,tipo,sector_edificio,bloque_piso,casa_apartamento,codigo_empresa_electrica,
             departamento_id,municipio_id,comunidad_id,barrio_colonia_id,
             avenida,calle,created_at,updated_at,user_id_created,user_id_updated)
            VALUES ('HND',1,1,1,1,1,13,1301,1,1,%s,%s,%s,%s,%s,%s)
            """, (
                avenida,
                calle,
                row.f_reg,
                row.f_edit,
                user_vendedor,
                user_vendedor
            ))

            direccion_id = my.lastrowid

            # ---------- direcciones_personas ----------
            my.execute("""
            INSERT INTO direcciones_personas
            (persona_id,direccion_id,created_at,updated_at,user_id_created,user_id_updated)
            VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                persona_id,
                direccion_id,
                row.f_reg,
                row.f_edit,
                user_vendedor,
                user_vendedor
            ))

            mysql.commit()

            # ---------- marcar migrado SQL Server ----------
            ss_update.execute("""
            UPDATE Clientes
            SET migrate = 1,
                personal_id = ?
            WHERE codclte = ?
            """, (persona_id, row.codclte))

            sqlserver.commit()

            migrados += 1

        except Exception as e:

            mysql.rollback()
            sqlserver.rollback()
            fallidos += 1
            print("❌", row.codclte, e)

    print("\n=== RESUMEN FINAL CLIENTES ===")
    print("Migrados:", migrados)
    print("Fallidos:", fallidos)

    mysql.close()
    sqlserver.close()
