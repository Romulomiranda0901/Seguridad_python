from config import conectar_sqlserver, conectar_mysql
from helpers import tipo_solicitud_val, estado_val, estado_verificacion_val
from tqdm import tqdm


def migrar_solicitudes():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("Obteniendo solicitudes pendientes...")

    ss.execute("""
    SELECT
        distinct
        c.personal_id,
        c.Codsucursal              AS centro_costo_id,
        sp.TipoSolicitud          AS tipo_solicitud,
        s.f_mov                   AS fecha_presentacion,
        s.f_mov                   AS fecha_resolucion,
        s.EstatdoSl               AS estado,
        s.EstatdoSl               AS estado_verificacion,
        v.cedula                  AS cedula_gestion,
        v.cedula                  AS cedula_correccion,
        v2.cedula                 AS cedula_verificacion,
        v3.cedula                 AS cedula_aprobacion,
        s.f_reg                   AS created_at,
        s.f_edit                  AS updated_at,
        s.codsolicitud
    FROM Solicitudes s
    LEFT JOIN SolicitudPrestamo sp ON s.codsolicitud = sp.CodSolicitud
    INNER JOIN Clientes c ON c.codclte = s.codclte
    LEFT JOIN vendedores v ON s.Desembolsador = v.codvende
    LEFT JOIN vendedores v2 ON s.CodVerificador = v2.codvende
    LEFT JOIN HistorialSolicitud hs ON hs.Codsolicitud = s.codsolicitud
    LEFT JOIN vendedores v3 ON hs.Usuario = v3.Usuario
    WHERE ISNULL(s.migrate,0)=0
      AND c.personal_id IS NOT NULL
    """)

    rows = ss.fetchall()

    print("Total solicitudes:", len(rows))

    migrados = 0
    fallidos = 0

    for row in tqdm(rows, desc="Migrando solicitudes"):

        try:

            # ---------- usuario cliente ----------
            my.execute("""
            SELECT u.id
            FROM personas p
            INNER JOIN users u ON p.user_id_created = u.id
            WHERE p.id = %s
            """,(row.personal_id,))

            uc = my.fetchone()
            empleado_gestion = uc["id"] if uc else 1

            # ---------- aprobador ----------
            my.execute("SELECT id FROM users WHERE persona_id=CONCAT('DNI-',%s)",(row.cedula_aprobacion,))
            ua = my.fetchone()
            empleado_aprobacion = ua["id"] if ua else 1

            # ---------- verificador ----------
            my.execute("SELECT id FROM users WHERE persona_id=CONCAT('DNI-',%s)",(row.cedula_verificacion,))
            uv = my.fetchone()
            empleado_verificacion = uv["id"] if uv else 1

            # ---------- evitar duplicados ----------


            existe = None

            if existe:
                solicitud_mysql_id = existe["id"]
            else:

                my.execute("""
                INSERT INTO solicitudes (
                    persona_id,
                    centro_costo_id,
                    tipo_solicitud,
                    fecha_presentacion,
                    fecha_resolucion,
                    estado,
                    estado_verificacion,
                    empleado_id_gestion,
                    empleado_id_correccion,
                    empleado_id_verificacion,
                    empleado_id_aprobacion,
                    created_at,
                    updated_at,
                    user_id_created,
                    user_id_updated
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                """,(
                    row.personal_id,
                    row.centro_costo_id,
                    tipo_solicitud_val(row.tipo_solicitud),
                    row.fecha_presentacion,
                    row.fecha_resolucion,
                    estado_val(row.estado),
                    estado_verificacion_val(row.estado_verificacion),
                    empleado_gestion,
                    empleado_gestion,
                    empleado_verificacion,
                    empleado_aprobacion,
                    row.created_at,
                    row.updated_at,
                    empleado_gestion,
                    empleado_verificacion
                ))

                mysql.commit()
                solicitud_mysql_id = my.lastrowid

            # ---------- marcar migrado SQL Server ----------
            ss_update.execute("""
            UPDATE Solicitudes
            SET migrate=1,
                id_solicitud=?
            WHERE codsolicitud=?
            """,(solicitud_mysql_id,row.codsolicitud))

            sqlserver.commit()

            migrados += 1

        except Exception as e:

            mysql.rollback()
            sqlserver.rollback()
            fallidos += 1
            print("❌ solicitud", row.codsolicitud, e)

    print("\n=== RESUMEN FINAL SOLICITUDES ===")
    print("Migradas:", migrados)
    print("Fallidas:", fallidos)

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_solicitudes()
