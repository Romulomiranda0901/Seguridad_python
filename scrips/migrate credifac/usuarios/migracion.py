from config import conectar_sqlserver, conectar_mysql
from helpers import get_user_id_by_persona, separar_nombre
from tqdm import tqdm
from datetime import datetime


def migrar_vendedores():

    sqlserver = conectar_sqlserver()
    mysql = conectar_mysql()

    ss = sqlserver.cursor()
    ss_update = sqlserver.cursor()
    my = mysql.cursor(dictionary=True)

    print("📥 Obteniendo vendedores...")

    ss.execute("""
    SELECT
        codvende,
        cedula,
        nombres,
        apellidos,
        telefono,
        telefono2,
        direccion,
        f_reg,
        F_nac,
        Codsucursal
    FROM vendedores
    WHERE user_id IS NULL
      AND ISNULL(migrate,0) = 0
    """)

    rows = ss.fetchall()
    print("Total vendedores:", len(rows))

    for row in tqdm(rows, desc="Migrando vendedores"):

        persona_id = f"DNI-{row.cedula}"

        try:

            # ==================================================
            # 🔍 BUSCAR SI YA EXISTE EN USERS
            # ==================================================
            user_existente = get_user_id_by_persona(my, persona_id, default=None)

            if user_existente:
                # ✔ YA EXISTE → SOLO UPDATE SQL SERVER
                ss_update.execute("""
                UPDATE vendedores
                SET migrate = 1,
                    user_id = ?
                WHERE codvende = ?
                """, (
                    persona_id,
                    row.codvende
                ))

                sqlserver.commit()
                continue

            # ==================================================
            # 🆕 NO EXISTE → CREAR TODO
            # ==================================================

            created_at = row.f_reg or datetime.now()

            primer_nombre, segundo_nombre = separar_nombre(row.nombres)
            primer_apellido, segundo_apellido = separar_nombre(row.apellidos)

            # ---------------- PERSONAS ----------------
            my.execute("""
            INSERT INTO personas (
                id,
                pais_nacimiento_id,
                tipo_documento_id,
                numero_documento,
                tipo_persona,
                es_pep,
                estado,
                verf_direccion,
                verf_tipo_documento,
                verf_numero_documento,
                verf_tipo_persona,
                verf_es_pep,
                verf_foto_anverso,
                verf_foto_reverso,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (
                %s,'HND','DNI',%s,1,2,1,1,1,1,1,1,1,1,%s,%s,1,1
            )
            """, (persona_id, row.cedula, created_at, created_at))

            # ---------------- PERSONAS NATURALES ----------------
            my.execute("""
            INSERT INTO personas_naturales (
                persona_id,
                primer_nombre,
                segundo_nombre,
                primer_apellido,
                segundo_apellido,
                oficio_profesion_id,
                sexo,
                fecha_nacimiento,
                estado_civil,
                verf_primer_nombre,
                verf_segundo_nombre,
                verf_primer_apellido,
                verf_segundo_apellido,
                verf_oficio_profesion_id,
                verf_sexo,
                verf_fecha_nacimiento,
                verf_estado_civil,
                verf_foto_rostro,
                verf_rtn_numero,
                verf_rtn_foto_anverso,
                verf_rtn_foto_reverso,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (
                %s,%s,%s,%s,%s,1,1,%s,2,
                1,1,1,1,1,1,1,1,1,1,1,1,
                %s,%s,1,1
            )
            """, (
                persona_id,
                primer_nombre,
                segundo_nombre,
                primer_apellido,
                segundo_apellido,
                row.F_nac,
                created_at,
                created_at
            ))

            # ---------------- TELÉFONOS ----------------
            for tel in [row.telefono, row.telefono2]:
                if tel:
                    my.execute("""
                    INSERT INTO telefonos (
                        persona_id,
                        tipo,
                        numero,
                        codigo_verificacion,
                        estado,
                        created_at,
                        updated_at,
                        user_id_created,
                        user_id_updated
                    ) VALUES (%s,1,%s,123456,1,%s,%s,1,1)
                    """, (
                        persona_id,
                        str(tel).strip(),
                        created_at,
                        created_at
                    ))

            # ---------------- COMENTARIO ----------------
            if row.direccion:
                my.execute("""
                INSERT INTO personas_comentarios (
                    persona_id,
                    comentario,
                    created_at,
                    updated_at,
                    user_id_created,
                    user_id_updated
                ) VALUES (%s,%s,%s,%s,1,1)
                """, (
                    persona_id,
                    row.direccion,
                    created_at,
                    created_at
                ))

            # ---------------- EMPLEADO ----------------
            email = f"{row.codvende}@correo.com"
            numero_cuenta = f"CTA-{persona_id}"
            my.execute("""
            INSERT INTO empleados (
                persona_id,
                departamento_credifac_id,
                centro_costo_id,
                institucion_financiera_id,
                numero_constancia_platf,
                numero_cuenta,
                numero,
                email,
                numero_vacante,
                estado,
                created_at,
                updated_at,
                user_id_created,
                user_id_updated
            ) VALUES (
                %s,2,%s,1,1,%s,1,%s,1,1,%s,%s,1,1
            )
            """, (
                persona_id,
                row.Codsucursal or 0,
                numero_cuenta,
                email,
                created_at,
                created_at
            ))


            # ---------------- USERS ----------------
            my.execute("""
            INSERT INTO users (
                persona_id,
                email,
                estado,
                email_verified_at,
                password,
                created_at,
                updated_at
            ) VALUES (
                %s,%s,1,NOW(),'12345678',%s,%s
            )
            """, (
                persona_id,
                email,
                created_at,
                created_at
            ))

            mysql.commit()

            # ---------------- UPDATE SQL SERVER ----------------
            ss_update.execute("""
            UPDATE vendedores
            SET migrate = 1,
                user_id = ?
            WHERE codvende = ?
            """, (
                persona_id,
                row.codvende
            ))

            sqlserver.commit()

        except Exception as e:
            mysql.rollback()
            sqlserver.rollback()
            print(f"❌ Error vendedor {row.codvende}: {e}")

    print("✅ Migración de vendedores finalizada")

    mysql.close()
    sqlserver.close()


if __name__ == "__main__":
    migrar_vendedores()
