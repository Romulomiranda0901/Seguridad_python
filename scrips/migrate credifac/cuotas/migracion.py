from config import conectar_sqlserver, conectar_mysql
from helpers import get_user_id_by_persona
from tqdm import tqdm


def migrar_cuotas():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("📥 Obteniendo cuotas desde SQL Server...")

    ss.execute("""
    SELECT
        p.id_prestamo,
        p.codprestamo,
        pp.cuota AS nro,
        pp.valor AS monto,

        (p.valor * (p.tasa / 100.0)) / p.cuotas AS interes_pagado,
        p.valor / p.cuotas AS capital_pagado,
        p.tasa / p.cuotas AS tasa_aplicada,

        0 AS tasa_mora,

        CASE
            WHEN last_mov.F_mov > last_mov.Ref_F_mov THEN 1
            WHEN last_mov.F_mov IS NULL AND GETDATE() > last_mov.Ref_F_mov THEN 1
            ELSE 2
        END AS es_mora,

        CASE
            WHEN last_mov.F_mov IS NOT NULL
                AND last_mov.F_mov > last_mov.Ref_F_mov
                THEN DATEDIFF(DAY, last_mov.Ref_F_mov, last_mov.F_mov)
            WHEN last_mov.F_mov IS NULL
                AND GETDATE() > last_mov.Ref_F_mov
                THEN DATEDIFF(DAY, last_mov.Ref_F_mov, GETDATE())
            ELSE 0
        END AS dias_mora,

        CASE
            WHEN ABS(
                ROUND(
                    ((p.valor * (p.tasa / 100.0)) + p.valor)
                    -
                    SUM(ROUND(pp.valor,2)) OVER (
                        PARTITION BY p.id_prestamo
                        ORDER BY pp.cuota
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                ,2)
            ) < 0.05
            THEN 0
            ELSE ROUND(
                ((p.valor * (p.tasa / 100.0)) + p.valor)
                -
                SUM(ROUND(pp.valor,2)) OVER (
                    PARTITION BY p.id_prestamo
                    ORDER BY pp.cuota
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ,2)
        END AS saldo,

        pp.f_mov as fecha_establecida,
        1 AS procesado,

        CASE
            WHEN ABS(ISNULL(pagos.total_pagado,0) - pp.valor) < 0.05 THEN 2
            WHEN ISNULL(pagos.total_pagado,0) = 0 THEN 3
            WHEN ISNULL(pagos.total_pagado,0) < pp.valor THEN 1
            ELSE 1
        END AS estado,

        pp.f_reg as created_at,
        pp.f_mov as updated_at,
        v.cedula as persona_user

    FROM dbo.Prestamos p
        INNER JOIN dbo.Planpagos pp
            ON p.codprestamo = pp.codprestamo
        LEFT JOIN vendedores v
            ON v.codvende = p.codvende

        LEFT JOIN (
            SELECT
                codprestamo,
                LTRIM(RTRIM(cuota)) AS cuota,
                SUM(valor) AS total_pagado
            FROM dbo.cxcdist
            GROUP BY codprestamo, LTRIM(RTRIM(cuota))
        ) pagos
            ON pagos.codprestamo = pp.codprestamo
            AND pagos.cuota = LTRIM(RTRIM(pp.cuota))

        OUTER APPLY (
            SELECT TOP 1 F_mov, Ref_F_mov
            FROM dbo.cxcdist c
            WHERE c.codprestamo = pp.codprestamo
              AND LTRIM(RTRIM(c.cuota)) = LTRIM(RTRIM(pp.cuota))
            ORDER BY c.F_mov DESC
        ) last_mov

    WHERE p.id_prestamo IS NOT NULL
      AND ISNULL(pp.migrate,0) = 0

    ORDER BY p.id_prestamo ASC, pp.cuota
    """)

    rows = ss.fetchall()
    print("Total cuotas a migrar:", len(rows))

    for row in tqdm(rows, desc="Migrando cuotas"):

        try:

            user_id = get_user_id_by_persona(
                my, f"DNI-{row.persona_user}"
            )

            my.execute("""
            INSERT INTO cuotas (
                prestamo_id,
                nro,
                monto,
                interes_pagado,
                capital_pagado,
                tasa_aplicada,
                tasa_mora,
                es_mora,
                dias_mora,
                saldo,
                fecha_establecida,
                procesado,
                estado,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """, (
                row.id_prestamo,
                int(row.nro),
                row.monto or 0,
                row.interes_pagado or 0,
                row.capital_pagado or 0,
                row.tasa_aplicada or 0,
                row.tasa_mora,
                row.es_mora,
                row.dias_mora,
                row.saldo or 0,
                row.fecha_establecida,
                row.procesado,
                row.estado,
                row.created_at,
                row.updated_at,
                user_id,
                user_id
            ))

            mysql.commit()
            cuota_mysql_id = my.lastrowid

            # 🔹 UPDATE SQL SERVER
            ss_update.execute("""
            UPDATE dbo.Planpagos
            SET migrate = 1,
                id_cuotas = ?
            WHERE codprestamo = ?
              AND LTRIM(RTRIM(cuota)) = ?
            """, (
                cuota_mysql_id,
                row.codprestamo,
                str(row.nro)
            ))

            sqlserver.commit()

        except Exception as e:
            mysql.rollback()
            sqlserver.rollback()
            print(f"❌ Error cuota préstamo {row.id_prestamo} cuota {row.nro}: {e}")

    print("✅ Migración de cuotas finalizada correctamente")

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_cuotas()
