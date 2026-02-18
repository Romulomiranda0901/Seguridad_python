from config import conectar_sqlserver, conectar_mysql
from helpers import get_user_id_by_persona
from tqdm import tqdm


def migrar_cobros():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("📥 Obteniendo cobros desde SQL Server...")

    ss.execute("""
    SELECT
        pp.id_cuotas AS cuota_id,
        c.cuota,
        v.cedula AS persona_empleado,
        1 AS visita_id,
        p.id_prestamo AS prestamo_id,
        c.capital,
        c.interes,
        0 AS mora,
        c.capital + c.interes AS total,

        CASE
            WHEN (c.capital + c.interes) >= c.Ref_valor THEN 2
            ELSE 1
        END AS tipo,

        1 AS modo,
        ' ' AS nota,

        pp.valor
            - ISNULL(
                SUM(c.valor) OVER (
                    PARTITION BY c.codprestamo, c.cuota
                    ORDER BY c.id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ), 0
            ) AS saldo_antiguo,

        pp.valor
            - SUM(c.valor) OVER (
                PARTITION BY c.codprestamo, c.cuota
                ORDER BY c.id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS saldo_actual,

        ' ' AS firma,
        c.f_reg AS created_at,
        c.F_mov AS updated_at,
        c.id AS id_mov,
        c.codprestamo

    FROM dbo.Prestamos p
        INNER JOIN dbo.Planpagos pp
            ON p.codprestamo = pp.codprestamo
        INNER JOIN cxcdist c
            ON c.codprestamo = pp.codprestamo
            AND c.cuota = pp.cuota
        LEFT JOIN vendedores v
            ON v.codvende = c.codvende

    WHERE pp.id_cuotas IS NOT NULL
      AND ISNULL(c.migrate, 0) = 0

    ORDER BY pp.id_cuotas, c.cuota, c.id
    """)

    rows = ss.fetchall()
    print("Total cobros a migrar:", len(rows))

    for row in tqdm(rows, desc="Migrando cobros"):

        try:

            # ---------- resolver empleado ----------
            empleado_id = get_user_id_by_persona(
                my, f"DNI-{row.persona_empleado}"
            )

            # ---------- INSERT MySQL ----------
            my.execute("""
            INSERT INTO cobros (
                cuota_id,
                empleado_id,
                visita_id,
                prestamo_id,
                capital,
                interes,
                mora,
                total,
                tipo,
                modo,
                nota,
                saldo_antiguo,
                saldo_actual,
                firma,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """, (
                row.cuota_id,
                empleado_id,
                row.visita_id,
                row.prestamo_id,
                row.capital or 0,
                row.interes or 0,
                row.mora,
                row.total or 0,
                row.tipo,
                row.modo,
                (row.nota or '').strip(),
                row.saldo_antiguo or 0,
                row.saldo_actual or 0,
                (row.firma or '').strip(),
                row.created_at,
                row.updated_at,
                empleado_id,
                empleado_id
            ))

            mysql.commit()
            cobro_mysql_id = my.lastrowid

            # ---------- UPDATE SQL SERVER ----------
            ss_update.execute("""
            UPDATE dbo.cxcdist
            SET migrate = 1,
                id_cobros = ?
            WHERE id = ?
            """, (
                cobro_mysql_id,
                row.id_mov
            ))

            sqlserver.commit()

        except Exception as e:
            mysql.rollback()
            sqlserver.rollback()
            print(f"❌ Error cobro cuota {row.cuota_id}: {e}")

    print("✅ Migración de cobros finalizada correctamente")

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_cobros()
