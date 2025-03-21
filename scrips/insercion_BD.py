import psycopg2
import sys

def main():
    try:
        # Configuración de la conexión a la base de datos
        conn = psycopg2.connect(
            dbname='dbname',  # Reemplazar
            user='user',           # Reemplazar
            password='password',    # Reemplazar
            host='localhost',            # o la dirección de tu servidor
            port='5432'                  # puerto por defecto de PostgreSQL
        )
    except Exception as e:
        print(f"Error de conexión a la base de datos: {e}")
        sys.exit(1)

    # Crear un cursor para ejecutar consultas
    cur = conn.cursor()

    # Nueva consulta para obtener los empleados con el departamento
    query_empleados = """
    SELECT
        em.id,
        em.primer_nombre,
        em.segundo_nombre,
        em.primer_apellido,
        em.segundo_apellido,
        hl.id AS id_historial_laboral,
        hl.id_cargo,
        cg.nombre AS cargo,
        dpto.id AS id_departamento_rrhh,
        dpto.nombre AS departamento
    FROM
        rrhh.empleados AS em
    JOIN
        rrhh.historial_laboral AS hl ON em.id = hl.id_empleados
    JOIN
        general.cargos AS cg ON hl.id_cargo = cg.id
    JOIN
        general.departamentos_rrhh AS dpto ON hl.id_departamento = dpto.id
    WHERE
        hl.id_cargo IN (125, 160, 151, 94, 78, 77, 86, 91, 95, 92, 93, 18, 127, 81, 129, 74,
                        126, 138, 115, 128, 136, 117, 7, 34, 116, 98, 96, 89, 100, 6, 114, 112,
                        133, 17, 79, 104, 119, 161, 40, 147, 82, 80, 62, 85, 99, 141, 24, 83,
                        88, 76, 137, 97, 90, 84, 162, 28)
    AND
        em.activo = 'SI'
    AND
        em.eliminado = 'NO'
    AND
        hl.activo = 'SI'
    AND
        hl.eliminado = 'NO'
    GROUP BY
        em.id, em.primer_nombre, em.segundo_nombre, em.primer_apellido, em.segundo_apellido,
        hl.id, hl.id_cargo, cg.nombre, dpto.id, dpto.nombre
    ORDER BY
        em.primer_nombre, em.primer_apellido;
    """

    try:
        # Ejecutar la consulta
        cur.execute(query_empleados)
        empleados = cur.fetchall()

        # Validar si se obtuvieron resultados
        if not empleados:
            print("No se encontraron empleados que cumplan con los criterios.")
            return

        # Insertar los datos en la tabla rrhh.departamento_cargo_jefe
        insert_query = """
        INSERT INTO rrhh.departamento_cargo_jefe (id_departamento_rrhh, id_historial_laboral, activo, eliminado)
        VALUES (%s, %s, %s, %s);
        """

        for empleado in empleados:
            id_historial_laboral = empleado[5]  # hl.id
            id_departamento_rrhh = empleado[8]   # dpto.id
            activo = 'SI'
            eliminado = 'NO'

            try:
                # Ejecutar la inserción
                print(f"Datos: Historial Laboral ID: {id_historial_laboral}, Departamento RRHH ID: {id_departamento_rrhh} - Registro insertado exitosamente.")
                cur.execute(insert_query, (id_departamento_rrhh, id_historial_laboral, activo, eliminado))
            except Exception as e:
                print(f"Error al insertar el registro para el historial laboral ID {id_historial_laboral}: {e}")

        # Hacer commit de las transacciones
        conn.commit()
        print("Datos insertados exitosamente en la tabla rrhh.departamento_cargo_jefe.")

    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
    finally:
        # Cerrar el cursor y la conexión
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()