from config import conectar_sqlserver, conectar_mysql
from helpers import get_user_id_by_persona
from tqdm import tqdm


def migrar_solicitudes_prestamos():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("📥 Obteniendo datos desde SQL Server...")

    ss.execute("""
    SELECT
        s.id_solicitud,

        -- PRESTAMO
        s.codmod         AS producto_financiero_id,
        s.codmod         AS tipo_modalidad_id,
        2                AS riesgo,
        s.cuotas         AS plazos,
        'TA01'           AS tipo_amortizacion_id,
        s.f_mov          AS fecha_verificacion,
        1                AS tipo_desembolso,
        s.valor          AS monto_solicitado,
        s.valor          AS monto_aprobado,
        0                AS tasa_mora,
        CASE
            WHEN s.Meses > 0 THEN s.tasa / s.Meses
            ELSE 0
        END              AS tasa_aceptada,
        v.cedula         AS persona_empleado_desembolso,
        p.f_mov          AS fecha_programada,
        p.f_reg          AS created_at,
        p.f_edit         AS updated_at,
        v2.cedula        AS user_created,

        -- REFINANCIAMIENTO
        sp.CodPrestamo   AS prestamo_id,
        sp.CodMod        AS producto_financiero_id_ref,
        sp.CodMod        AS tipo_modalidad_id_ref,
        2                AS riesgo_ref,
        sp.Cuotas        AS plazos_ref,
        'TA01'           AS tipo_amortizacion_id_ref,
        sp.Fecha         AS fecha_verificacion_ref,
        1                AS tipo_desembolso_ref,
        ISNULL(sp.ARecibir, 0) AS monto_adicional,
        sp.Valor         AS monto_aprobado_ref,
        0                AS tasa_mora_ref,
        CASE
            WHEN s.Meses > 0 THEN s.tasa / s.Meses
            ELSE 0
        END              AS tasa_aceptada_ref,
        v3.cedula        AS persona_empleado_desembolso_ref,
        sp.Fecha         AS fecha_programada_ref,
        sp.Fecha         AS created_at_ref,
        sp.Fecha         AS updated_at_ref,
        v3.cedula        AS user_created_ref

    FROM dbo.Solicitudes s
        LEFT JOIN dbo.SolicitudPrestamo sp ON sp.CodSolicitud = s.codsolicitud
        LEFT JOIN Prestamos p ON p.codsolicitud = s.codsolicitud
        LEFT JOIN vendedores v ON s.Desembolsador = v.codvende
        LEFT JOIN vendedores v2 ON v2.codvende = p.codvende
        LEFT JOIN vendedores v3 ON v3.codvende = sp.codvende
    WHERE s.id_solicitud IS NOT NULL
    """)

    rows = ss.fetchall()
    print("Total registros:", len(rows))

    for row in tqdm(rows, desc="Migrando prestamos / refinanciamientos"):

        # ---------- tipo_solicitud desde MySQL ----------
        my.execute(
            "SELECT tipo_solicitud FROM solicitudes WHERE id = %s",
            (row.id_solicitud,)
        )
        ts = my.fetchone()
        if not ts:
            continue

        tipo_solicitud = ts["tipo_solicitud"]

        # ---------- PRESTAMO ----------
        if tipo_solicitud == 1:

            empleado_id = get_user_id_by_persona(
                my, f"DNI-{row.persona_empleado_desembolso}"
            )

            my.execute("""
            INSERT INTO solicitudes_prestamos (
                solicitud_id,
                producto_financiero_id,
                tipo_modalidad_id,
                riesgo,
                plazos,
                tipo_amortizacion_id,
                fecha_verificacion,
                tipo_desembolso,
                monto_solicitado,
                monto_aprobado,
                tasa_mora,
                tasa_aceptada,
                empleado_id_desembolso,
                fecha_programada,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                row.id_solicitud,
                row.producto_financiero_id,
                row.tipo_modalidad_id,
                row.riesgo,
                row.plazos,
                row.tipo_amortizacion_id,
                row.fecha_verificacion,
                row.tipo_desembolso,
                row.monto_solicitado or 0,
                row.monto_aprobado or 0,
                row.tasa_mora,
                row.tasa_aceptada,
                empleado_id,
                row.fecha_programada,
                row.created_at,
                row.updated_at,
                empleado_id,
                empleado_id
            ))

        # ---------- REFINANCIAMIENTO ----------
        elif tipo_solicitud == 3 and row.prestamo_id:

            empleado_id = get_user_id_by_persona(
                my, f"DNI-{row.persona_empleado_desembolso_ref}"
            )

            my.execute("""
            INSERT INTO solicitudes_refinanciamientos (
                solicitud_id,
                prestamo_id,
                producto_financiero_id,
                tipo_modalidad_id,
                riesgo,
                plazos,
                tipo_amortizacion_id,
                fecha_verificacion,
                tipo_desembolso,
                monto_adicional,
                monto_aprobado,
                tasa_mora,
                tasa_aceptada,
                empleado_id_desembolso,
                fecha_programada,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                row.id_solicitud,
                row.prestamo_id,
                row.producto_financiero_id_ref,
                row.tipo_modalidad_id_ref,
                row.riesgo_ref,
                row.plazos_ref,
                row.tipo_amortizacion_id_ref,
                row.fecha_verificacion_ref,
                row.tipo_desembolso_ref,
                row.monto_adicional or 0,
                row.monto_aprobado_ref or 0,
                row.tasa_mora_ref,
                row.tasa_aceptada_ref,
                empleado_id,
                row.fecha_programada_ref,
                row.created_at_ref,
                row.updated_at_ref,
                empleado_id,
                empleado_id
            ))

        mysql.commit()

    print("✅ Migración de prestamos y refinanciamientos finalizada")

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_solicitudes_prestamos()
