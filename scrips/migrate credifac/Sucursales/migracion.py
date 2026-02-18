from config import conectar_sqlserver, conectar_mysql
from tqdm import tqdm


def migrar_sucursales():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("📥 Obteniendo zonas y sucursales...")

    ss.execute("""
    SELECT
        z.codzona,
        z.descrip       AS zona_nombre,
        z.f_reg         AS zona_fecha,

        s.Codsucursal   AS sucursal_id,
        s.nombre        AS sucursal_nombre,
        s.f_reg         AS sucursal_fecha

    FROM zonas z
        INNER JOIN ZonasSucursales zs
            ON zs.Codzona = z.codzona
        INNER JOIN dbo.Sucursales s
            ON s.Codsucursal = zs.Codsucursal

    WHERE ISNULL(z.migrate,0) = 0
       OR ISNULL(zs.migrate,0) = 0
    """)

    rows = ss.fetchall()
    print("Total registros:", len(rows))

    zonas_cache = {}

    for row in tqdm(rows, desc="Migrando zonas y sucursales"):

        try:
            # =====================================================
            # 🔹 ZONA
            # =====================================================
            if row.codzona not in zonas_cache:

                my.execute(
                    "SELECT id FROM zonas WHERE nombre = %s",
                    (row.zona_nombre,)
                )
                zona_existente = my.fetchone()

                if zona_existente:
                    zona_id_mysql = zona_existente["id"]
                else:
                    my.execute("""
                    INSERT INTO zonas (
                        nombre,
                        estado,
                        created_at,
                        updated_at,
                        user_id_created,
                        user_id_updated
                    ) VALUES (%s,1,%s,%s,1,1)
                    """, (
                        row.zona_nombre,
                        row.zona_fecha,
                        row.zona_fecha
                    ))

                    mysql.commit()
                    zona_id_mysql = my.lastrowid

                zonas_cache[row.codzona] = zona_id_mysql

                # 🔹 UPDATE SQL SERVER ZONAS
                ss_update.execute("""
                UPDATE dbo.Zonas
                SET migrate = 1,
                    zonas_id = ?
                WHERE codzona = ?
                """, (
                    str(zona_id_mysql),
                    row.codzona
                ))

                sqlserver.commit()

            else:
                zona_id_mysql = zonas_cache[row.codzona]

            # =====================================================
            # 🔹 CENTRO DE COSTOS (SUCURSAL)
            # =====================================================
            my.execute(
                "SELECT id FROM centro_costos WHERE id = %s",
                (row.sucursal_id,)
            )
            cc_existente = my.fetchone()

            if not cc_existente:

                my.execute("""
                INSERT INTO centro_costos (
                    id,
                    zona_id,
                    direccion_id,
                    nombre,
                    licencia_negocio,
                    estado,
                    created_at,
                    updated_at,
                    user_id_created,
                    user_id_updated
                ) VALUES (
                    %s,%s,1,%s,1,1,%s,%s,1,1
                )
                """, (
                    row.sucursal_id,
                    zona_id_mysql,
                    row.sucursal_nombre,
                    row.sucursal_fecha,
                    row.sucursal_fecha
                ))

                mysql.commit()

            # 🔹 UPDATE SQL SERVER ZonasSucursales
            ss_update.execute("""
            UPDATE dbo.ZonasSucursales
            SET migrate = 1,
                centro_costos_id = ?
            WHERE Codsucursal = ?
              AND Codzona = ?
            """, (
                str(row.sucursal_id),
                row.sucursal_id,
                row.codzona
            ))

            sqlserver.commit()

        except Exception as e:
            mysql.rollback()
            sqlserver.rollback()
            print(f"❌ Error sucursal {row.sucursal_id}: {e}")

    print("✅ Migración de zonas y sucursales finalizada")

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_sucursales()
