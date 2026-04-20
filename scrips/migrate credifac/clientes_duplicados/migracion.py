from config import conectar_sqlserver
from tqdm import tqdm


TABLAS = [
    "Solicitudes",
    "SolicitudPrestamo",
    "HistorialSolicitud",
    "Prestamos",
    "Planpagos",
    "cxcmov",
    "cxcdist"
]


# =========================================================
# 🔥 OBTENER CLIENTE MASTER (más solicitudes)
# =========================================================
def obtener_cliente_master(cursor, codigos):

    master = None
    max_solicitudes = -1

    for cod in codigos:
        cursor.execute("""
            SELECT COUNT(*)
            FROM Solicitudes
            WHERE codclte = ?
        """, cod)

        total = cursor.fetchone()[0]

        if total > max_solicitudes:
            max_solicitudes = total
            master = cod

    return master


# =========================================================
# 🔥 REASIGNAR DATOS AL MASTER
# =========================================================
def reasignar_cliente(cursor, origen, destino):

    for tabla in TABLAS:
        sql = f"""
            UPDATE {tabla}
            SET codclte = ?
            WHERE codclte = ?
        """
        cursor.execute(sql, destino, origen)


# =========================================================
# 🔥 VERIFICAR SI CLIENTE NO TIENE ACTIVIDAD
# =========================================================
def cliente_sin_movimientos(cursor, codclte):

    for tabla in TABLAS:
        sql = f"SELECT TOP 1 1 FROM {tabla} WHERE codclte = ?"
        cursor.execute(sql, codclte)

        if cursor.fetchone():
            return False

    return True


# =========================================================
# 🚀 SCRIPT PRINCIPAL
# =========================================================
def consolidar_clientes_duplicados():

    cn = conectar_sqlserver()
    cur = cn.cursor()

    # =========================================================
    # 🔧 CREAR TABLA ClientesEliminados SI NO EXISTE
    # =========================================================
    print("🔧 Verificando tabla ClientesEliminados...")

    cur.execute("""
        IF OBJECT_ID('ClientesEliminados', 'U') IS NULL
        BEGIN
            SELECT TOP 0 *
            INTO ClientesEliminados
            FROM Clientes
        END
    """)

    # =========================================================
    # 🔍 BUSCAR CÉDULAS DUPLICADAS
    # =========================================================
    print("🔍 Buscando cédulas duplicadas...")

    cur.execute("""
        SELECT cedula
        FROM Clientes
        GROUP BY cedula
        HAVING COUNT(*) > 1
    """)

    cedulas = [r.cedula for r in cur.fetchall()]

    print("Total cédulas duplicadas:", len(cedulas))

    # =========================================================
    # 🔥 CONSOLIDAR DUPLICADOS
    # =========================================================
    for cedula in tqdm(cedulas, desc="Consolidando duplicados"):

        cur.execute("""
            SELECT codclte
            FROM Clientes
            WHERE cedula = ?
        """, cedula)

        codigos = [r.codclte for r in cur.fetchall()]

        if len(codigos) < 2:
            continue

        master = obtener_cliente_master(cur, codigos)

        for cod in codigos:

            if cod == master:
                continue

            reasignar_cliente(cur, cod, master)

    # =========================================================
    # 🔎 ELIMINAR CLIENTES SIN ACTIVIDAD
    # =========================================================
    print("\n🔎 Buscando clientes sin actividad...")

    cur.execute("SELECT codclte FROM Clientes")
    todos_clientes = [r.codclte for r in cur.fetchall()]

    eliminados = 0

    for cod in tqdm(todos_clientes, desc="Eliminando clientes vacíos"):

        if cliente_sin_movimientos(cur, cod):

            # 🔥 Guardar en historial
            cur.execute("""
                INSERT INTO Clientes_Eliminado
                SELECT *
                FROM Clientes
                WHERE codclte = ?
            """, cod)

            # 🔥 Eliminar de Clientes
            cur.execute("""
                DELETE FROM Clientes
                WHERE codclte = ?
            """, cod)

            eliminados += 1

    # =========================================================
    # 🧼 LIMPIAR CÉDULAS
    # =========================================================
    print("\n🧼 Limpiando cédulas...")

    cur.execute("""
        UPDATE Clientes
        SET cedula =
            REPLACE(
            REPLACE(
            REPLACE(
            REPLACE(
            REPLACE(
            REPLACE(
            LTRIM(RTRIM(cedula)),
            ' ', ''),
            '-', ''),
            '.', ''),
            ',', ''),
            '(', ''),
            ')', '')
        WHERE cedula IS NOT NULL
    """)

    cn.commit()
    cn.close()

    print("\n✅ Proceso finalizado")
    print(f"🧹 Clientes eliminados sin actividad: {eliminados}")