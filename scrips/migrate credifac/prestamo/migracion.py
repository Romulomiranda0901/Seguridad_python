from config import conectar_sqlserver, conectar_mysql
from helpers import get_user_id_by_persona
from tqdm import tqdm


def migrar_prestamos():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("📥 Obteniendo préstamos desde SQL Server...")

    ss.execute("""
    SELECT
        s.id_solicitud   AS solicitud_id,
        cs.personal_id   AS persona_id,
        p.Codsucursal    AS centro_costo_id,
        p.codmod         AS producto_financiero_id,
        'TA01'           AS tipo_amortizacion_id,
        p.codmod         AS tipo_modalidad_id,
       CASE
           WHEN LTRIM(RTRIM(p.codzona)) = '' THEN 0
           WHEN p.codzona IS NULL THEN 0
           ELSE p.codzona
       END AS zona_id,
        v.cedula         AS persona_empleado,
        CASE
            WHEN s.Meses > 0 THEN s.tasa / s.Meses
            ELSE 0
        END              AS tasa_interes,
        0                AS tasa_mora_anual,
        CASE
            WHEN p.balance > 0 THEN 1   -- ACTIVO
            WHEN p.balance = 0 THEN 4   -- CANCELADO
        END              AS estado,
        p.cuotas         AS plazos,
        p.valor          AS monto,
        CASE
            WHEN sp.TipoSolicitud IS NOT NULL THEN 5
            WHEN sp.TipoSolicitud IS NULL THEN 3
        END              AS categoria,
        ISNULL(SUM(c.interes), 0)  AS interes_pagado,
        ISNULL(SUM(c.capital), 0)  AS capital_pagado,
        0                AS mora_pagada,
        p.ultimopago     AS total,
        p.balance        AS saldo,
        p.f_reg          AS created_at,
        p.f_edit         AS updated_at,
        p.codprestamo    AS codprestamo
    FROM dbo.Solicitudes s
        LEFT JOIN SolicitudPrestamo sp
            ON sp.CodSolicitud = s.codsolicitud
        INNER JOIN Prestamos p
            ON p.codsolicitud = s.codsolicitud
        INNER JOIN cxcdist c
            ON p.codprestamo = c.codprestamo
        INNER JOIN dbo.Clientes cs
            ON cs.codclte = p.codclte
        INNER JOIN vendedores v
            ON v.codvende = p.codvende
    WHERE s.id_solicitud IS NOT NULL
      AND ISNULL(p.migrate, 0) = 0
    GROUP BY
        s.id_solicitud,
        cs.personal_id,
        p.Codsucursal,
        p.codmod,
        p.codzona,
        v.cedula,
        s.tasa,
        s.Meses,
        p.balance,
        sp.TipoSolicitud,
        p.cuotas,
        p.valor,
        p.ultimopago,
        p.f_reg,
        p.f_edit,
        p.codprestamo
    """)

    rows = ss.fetchall()
    print("Total préstamos a migrar:", len(rows))

    for row in tqdm(rows, desc="Migrando préstamos"):

        try:
            # ---------- resolver empleado / usuario ----------
            empleado_id = get_user_id_by_persona(
                my, f"DNI-{row.persona_empleado}"
            )

            # ---------- INSERT MySQL ----------
            my.execute("""
            INSERT INTO prestamos (
                solicitud_id,
                persona_id,
                centro_costo_id,
                producto_financiero_id,
                tipo_amortizacion_id,
                tipo_modalidad_id,
                zona_id,
                empleado_id,
                tasa_interes,
                tasa_mora_anual,
                estado,
                plazos,
                monto,
                categoria,
                interes_pagado,
                capital_pagado,
                mora_pagada,
                total,
                saldo,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """, (
                row.solicitud_id,
                row.persona_id,
                row.centro_costo_id,
                row.producto_financiero_id,
                row.tipo_amortizacion_id,
                row.tipo_modalidad_id,
                row.zona_id,
                empleado_id,
                row.tasa_interes or 0,
                row.tasa_mora_anual,
                row.estado,
                row.plazos,
                row.monto or 0,
                row.categoria,
                row.interes_pagado or 0,
                row.capital_pagado or 0,
                row.mora_pagada,
                row.total or 0,
                row.saldo or 0,
                row.created_at,
                row.updated_at,
                empleado_id,
                empleado_id
            ))

            mysql.commit()
            prestamo_mysql_id = my.lastrowid

            # ---------- UPDATE SQL SERVER ----------
            ss_update.execute("""
            UPDATE dbo.Prestamos
            SET migrate = 1,
                id_prestamo = ?
            WHERE codprestamo = ?
            """, (
                prestamo_mysql_id,
                row.codprestamo
            ))

            sqlserver.commit()

        except Exception as e:
            mysql.rollback()
            sqlserver.rollback()
            print(f"❌ Error préstamo solicitud {row.solicitud_id}: {e}")

    print("✅ Migración de préstamos finalizada correctamente")

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_prestamos()
