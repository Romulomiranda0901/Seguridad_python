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

    return master, max_solicitudes


def reasignar_cliente(cursor, tablas, origen, destino):
    for tabla in tablas:
        sql = f"""
            UPDATE {tabla}
            SET codclte = ?
            WHERE codclte = ?
        """
        cursor.execute(sql, destino, origen)


def cliente_sin_movimientos(cursor, codclte):

    tablas = [
        "Solicitudes",
        "SolicitudPrestamo",
        "HistorialSolicitud",
        "Prestamos",
        "Planpagos",
        "cxcmov",
        "cxcdist"
    ]

    for tabla in tablas:
        sql = f"SELECT TOP 1 1 FROM {tabla} WHERE codclte = ?"
        cursor.execute(sql, codclte)

        if cursor.fetchone():
            return False

    return True