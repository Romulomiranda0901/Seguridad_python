from config import conectar_sqlserver, conectar_mysql
from helpers import estado_historial_val, get_user_id_by_persona
from tqdm import tqdm


def migrar_solicitud_detalles():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("📥 Obteniendo datos desde SQL Server...")

    ss.execute("""
    SELECT DISTINCT
        s.id_solicitud,
        1 AS nivel_estudio_id,
        'MT04' AS medio_transporte_id,
        'TV01' AS tipo_vivienda_id,
       ISNULL(esc.TotalConceptos, 0) AS ingreso_mensual,
       ISNULL(esc.TotalNegocio, 0) AS gasto_mensual,
        2 AS tiene_factura,
        'ENEE' AS empresa_proveedora_electrica,
        'ENEE' AS codigo_cliente,
        'UBICACION SEGUN ENEE' AS ubicacion_vivienda,
        4 AS razon_falta_factura,
       ISNULL(ie.Actividad, 'NO DEFINIDA') AS detalle_principal_fuente,
        CAST(GETDATE() AS DATE) AS inicio_fuente,
        CAST(GETDATE() AS DATE) AS ultima_fuente,
        2 AS razon_credito,
        12 AS tipo_gasto,
        1 AS verf_nivel_estudio_id,
        1 AS verf_medio_transporte_id,
        1 AS verf_tipo_vivienda_id,
        1 AS verf_ingreso_mensual,
        1 AS verf_gasto_mensual,
        1 AS verf_tiene_factura,
        1 AS verf_empresa_proveedora_electrica,
        1 AS verf_codigo_cliente,
        1 AS verf_ubicacion_vivienda,
        1 AS verf_razon_falta_factura,
        1 AS verf_detalle_principal_fuente,
        1 AS verf_inicio_fuente,
        1 AS verf_ultima_fuente,
        1 AS verf_razon_credito,
        1 AS verf_tipo_gasto,
        hs.Estado AS estado,
        CONCAT('DNI-', v3.cedula) AS persona_empleado,
        hs.Comentario AS observacion
    FROM Solicitudes s
        INNER JOIN Clientes c ON c.codclte = s.codclte
        LEFT JOIN EstudioSocioEconomico esc ON esc.Codclte = s.codclte

        OUTER APPLY (
            SELECT TOP 1 *
            FROM HistorialSolicitud hs
            WHERE hs.Codsolicitud = s.codsolicitud
              AND hs.Estado IS NOT NULL
              AND LTRIM(RTRIM(hs.Estado)) <> ''
            ORDER BY hs.id DESC
        ) hs

        LEFT JOIN vendedores v3 ON hs.Usuario = v3.Usuario
        LEFT JOIN InfoEmpresa ie ON ie.Codclte = s.codclte
    WHERE s.id_solicitud IS NOT NULL
    """)

    rows = ss.fetchall()
    print("Total registros:", len(rows))

    for row in tqdm(rows, desc="Migrando solicitud_detalles"):

        # ---------- auditoría desde MySQL ----------
        my.execute("""
        SELECT created_at, updated_at, user_id_created, user_id_updated
        FROM solicitudes
        WHERE id = %s
        """, (row.id_solicitud,))

        audit = my.fetchone()
        if not audit:
            continue

        estado_id = estado_historial_val(row.estado)
        empleado_id = get_user_id_by_persona(my, row.persona_empleado)

        # ---------- INSERT solicitud_detalles ----------
        my.execute("""
        INSERT INTO solicitud_detalles (
            solicitud_id,
            nivel_estudio_id,
            medio_transporte_id,
            tipo_vivienda_id,
            ingreso_mensual,
            gasto_mensual,
            tiene_factura,
            empresa_proveedora_electrica,
            codigo_cliente,
            ubicacion_vivienda,
            razon_falta_factura,
            detalle_principal_fuente,
            inicio_fuente,
            ultima_fuente,
            razon_credito,
            tipo_gasto,
            verf_nivel_estudio_id,
            verf_medio_transporte_id,
            verf_tipo_vivienda_id,
            verf_ingreso_mensual,
            verf_gasto_mensual,
            verf_tiene_factura,
            verf_empresa_proveedora_electrica,
            verf_codigo_cliente,
            verf_ubicacion_vivienda,
            verf_razon_falta_factura,
            verf_detalle_principal_fuente,
            verf_inicio_fuente,
            verf_ultima_fuente,
            verf_razon_credito,
            verf_tipo_gasto,
            estado,
            created_at,
            updated_at,
            user_id_created,
            user_id_updated
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """, (
            row.id_solicitud,
            row.nivel_estudio_id,
            row.medio_transporte_id,
            row.tipo_vivienda_id,
            row.ingreso_mensual,
            row.gasto_mensual,
            row.tiene_factura,
            row.empresa_proveedora_electrica,
            row.codigo_cliente,
            row.ubicacion_vivienda,
            row.razon_falta_factura,
            row.detalle_principal_fuente,
            row.inicio_fuente,
            row.ultima_fuente,
            row.razon_credito,
            row.tipo_gasto,
            row.verf_nivel_estudio_id,
            row.verf_medio_transporte_id,
            row.verf_tipo_vivienda_id,
            row.verf_ingreso_mensual,
            row.verf_gasto_mensual,
            row.verf_tiene_factura,
            row.verf_empresa_proveedora_electrica,
            row.verf_codigo_cliente,
            row.verf_ubicacion_vivienda,
            row.verf_razon_falta_factura,
            row.verf_detalle_principal_fuente,
            row.verf_inicio_fuente,
            row.verf_ultima_fuente,
            row.verf_razon_credito,
            row.verf_tipo_gasto,
            estado_id,
            audit["created_at"],
            audit["updated_at"],
            audit["user_id_created"],
            audit["user_id_updated"]
        ))

        # ---------- INSERT solicitud_observaciones ----------
        if row.observacion:
            my.execute("""
            INSERT INTO solicitud_observaciones (
                solicitud_id,
                empleado_id,
                observacion,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                row.id_solicitud,
                empleado_id,
                row.observacion,
                audit["created_at"],
                audit["updated_at"],
                audit["user_id_created"],
                audit["user_id_updated"]
            ))

        mysql.commit()

    print("✅ Migración finalizada correctamente")

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_solicitud_detalles()