from helpers import *
from config import conectar_sqlserver, conectar_mysql
from tqdm import tqdm


def migrar_clientes():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

   # =========================================================
   # 🔥 DIAS DE MORA POR CLIENTE (INCLUYE LOS QUE ESTAN EN 0)
   # =========================================================
   print("Obteniendo días de mora...")

   ss.execute("""
   WITH cuotas_unicas AS (
       SELECT
           pp.codclte,
           pp.codprestamo,
           pp.cuota,
           MIN(CAST(pp.f_mov AS date)) AS fecha_establecida,
           MAX(ISNULL(pp.amortizacion, 0)) AS amortizacion
       FROM Planpagos pp
       GROUP BY pp.codclte, pp.codprestamo, pp.cuota
   ),

   cuotas_en_atraso_no_pagadas AS (
       SELECT
           cu.codclte,
           cu.codprestamo,
           cu.cuota,
           cu.fecha_establecida,
           DATEDIFF(DAY, cu.fecha_establecida, CAST(GETDATE() AS date)) AS dias_atraso_cuota
       FROM cuotas_unicas cu
       WHERE cu.fecha_establecida < CAST(GETDATE() AS date)
         AND cu.amortizacion > 0
         AND NOT EXISTS (
               SELECT 1
               FROM cxcdist d
               WHERE LTRIM(RTRIM(d.codprestamo)) = LTRIM(RTRIM(cu.codprestamo))
                 AND d.cuota = cu.cuota
                 AND d.f_mov IS NOT NULL
         )
   ),

   clientes_totales AS (
       SELECT DISTINCT codclte
       FROM Planpagos
   )

   SELECT
       ct.codclte,
       ISNULL(SUM(ca.dias_atraso_cuota), 0) AS dias_mora
   FROM clientes_totales ct
   LEFT JOIN cuotas_en_atraso_no_pagadas ca
       ON ca.codclte = ct.codclte
   GROUP BY ct.codclte
   """)

   mora_map = {r.codclte: r.dias_mora for r in ss.fetchall()}


    # =========================================================
    # 🔥 CLIENTES PENDIENTES
    # =========================================================
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

    # =========================================================
    for row in tqdm(rows, desc="Migrando clientes"):

        try:

            # ---------- persona_id ----------
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

            # =========================================================
            # PERSONAS
            # =========================================================
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

            # =========================================================
            # PERSONAS NATURALES
            # =========================================================
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

            # =========================================================
            # TELEFONOS
            # =========================================================
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

            # =========================================================
            # COMENTARIOS
            # =========================================================
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

            # =========================================================
            # DIRECCION
            # =========================================================
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

            # =========================================================
            # 🔥 CLIENTES MYSQL — SOLO SI EXISTE EN mora_map
            # =========================================================

            if row.codclte in mora_map:

                dias_mora = mora_map[row.codclte] or 0

                my.execute("""
                INSERT INTO clientes
                (persona_id,dias_mora,centro_costo_id,
                 created_at,updated_at,user_id_created,user_id_updated)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (
                    persona_id,
                    dias_mora,
                    row.Codsucursal,
                    row.f_reg,
                    row.f_edit,
                    user_vendedor,
                    user_vendedor
                ))

                cliente_mysql_id = my.lastrowid

            else:
                cliente_mysql_id = None

            mysql.commit()

            # ---------- marcar migrado SQL Server ----------
            ss_update.execute("""
            UPDATE Clientes
            SET migrate = 1,
                personal_id = ?,
                cliente_id = ?
            WHERE codclte = ?
            """, (
                persona_id,
                str(cliente_mysql_id) if cliente_mysql_id else None,
                row.codclte
            ))

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
