from config import conectar_sqlserver, conectar_mysql
from helpers import (
    get_empleado_id_by_persona,
    get_user_id_by_persona,
    safe_int
)
from tqdm import tqdm


def migrar_recibos():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("📥 Obteniendo movimientos NO migrados...")

    ss.execute("""
    SELECT
        c.coddoc,
        c.codprestamo,
        c.numero,
        c.valor,
        c.comentario,
        c.f_reg,
        c.F_mov,
        v.user_id        AS persona_id,
        cx.id_cobros     AS cobro_id,
        s.id_solicitud
    FROM dbo.Solicitudes s
        INNER JOIN dbo.Prestamos p ON p.codsolicitud = s.codsolicitud
        INNER JOIN cxcmov c ON c.codprestamo = p.codprestamo
        INNER JOIN cxcdist cx on c.numero = cx.numero
        INNER JOIN vendedores v ON v.codvende = c.codvende
    WHERE cx.id_cobros IS NOT NULL
      AND c.migrate = 0
    ORDER BY c.numero
    """)

    rows = ss.fetchall()
    print("Total registros:", len(rows))

    for row in tqdm(rows, desc="Migrando"):

        try:
            persona_id = row.persona_id

            empleado_id = get_empleado_id_by_persona(my, persona_id)
            user_id = get_user_id_by_persona(my, persona_id)

            nro = safe_int(row.numero)
            codigo = f"RI-{nro}"

            # =====================================================
            # 🔹 DESEMBOLSOS
            # =====================================================
            if row.coddoc == "DB":

                my.execute("""
                SELECT id FROM desembolsos
                WHERE solicitud_id = %s
                  AND monto = %s
                  AND fecha_programada = %s
                  AND empleado_id = %s
                """, (
                    row.id_solicitud,
                    row.valor or 0,
                    row.F_mov,
                    empleado_id
                ))

                existe = my.fetchone()

                if existe:
                    desembolso_id = existe["id"]
                else:

                    my.execute("""
                    INSERT INTO desembolsos (
                        empleado_id,
                        solicitud_id,
                        monto,
                        tipo_desembolso,
                        estado,
                        fecha_programada,
                        created_at,
                        updated_at,
                        user_id_created,
                        user_id_updated
                    ) VALUES (
                        %s,%s,%s,1,3,%s,%s,%s,%s,%s
                    )
                    """, (
                        empleado_id,
                        row.id_solicitud,
                        row.valor or 0,
                        row.F_mov,
                        row.f_reg,
                        row.F_mov,
                        user_id,
                        user_id
                    ))

                    mysql.commit()
                    desembolso_id = my.lastrowid

                # 🔥 ACTUALIZAR SQL SERVER
                ss_update.execute("""
                UPDATE dbo.cxcmov
                SET migrate = 1,
                    desembolso_id = ?
                WHERE numero = ?
                """, (
                    str(desembolso_id),
                    row.numero
                ))

                sqlserver.commit()
                continue

            # =====================================================
            # 🔹 RECIBOS
            # =====================================================

            my.execute("""
            SELECT id FROM recibos
            WHERE nro = %s
              AND codigo = %s
              AND monto = %s
            """, (
                nro,
                codigo,
                row.valor or 0
            ))

            existe = my.fetchone()

            if existe:
                recibo_id = existe["id"]
            else:

                my.execute("""
                INSERT INTO recibos (
                    codigo_verificacion,
                    empleado_id,
                    caja_id,
                    nro,
                    codigo,
                    tipo,
                    metodo,
                    monto,
                    estado,
                    glosa,
                    user_id_created,
                    user_id_updated,
                    created_at,
                    updated_at
                ) VALUES (
                    FLOOR(RAND()*900000)+100000,
                    %s,1,%s,%s,1,1,%s,1,%s,
                    %s,%s,%s,%s
                )
                """, (
                    empleado_id,
                    nro,
                    codigo,
                    row.valor or 0,
                    row.comentario or "",
                    user_id,
                    user_id,
                    row.f_reg,
                    row.F_mov
                ))

                mysql.commit()
                recibo_id = my.lastrowid

            # =====================================================
            # 🔹 RECIBO_COBROS
            # =====================================================

            my.execute("""
            SELECT id FROM recibo_cobros
            WHERE recibo_id = %s
              AND cobro_id = %s
            """, (
                recibo_id,
                row.cobro_id
            ))

            if not my.fetchone():

                my.execute("""
                INSERT INTO recibo_cobros (
                    recibo_id,
                    cobro_id,
                    user_id_created,
                    user_id_updated,
                    created_at,
                    updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s)
                """, (
                    recibo_id,
                    row.cobro_id,
                    user_id,
                    user_id,
                    row.f_reg,
                    row.F_mov
                ))

                mysql.commit()

            # 🔥 ACTUALIZAR SQL SERVER
            ss_update.execute("""
            UPDATE dbo.cxcmov
            SET migrate = 1,
                recibo_id = ?
            WHERE numero = ?
            """ , (
                str(recibo_id),
                row.numero
            ))

            sqlserver.commit()

        except Exception as e:
            mysql.rollback()
            sqlserver.rollback()
            print(f"❌ Error movimiento {row.numero}: {e}")

    print("✅ Migración finalizada")

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_recibos()
