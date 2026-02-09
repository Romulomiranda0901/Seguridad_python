# ================= UTILIDADES GENERALES =================

def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None

def estado_historial_val(v):
    if not v:
        return 1  # PENDIENTE por defecto

    v = str(v).strip().upper()

    mapa = {
        "PENDIENTE": 1,
        "APROBADO": 2,
        "APROBADA": 2,
        "ACEPTADO": 2,
        "RECHAZADO": 3,
        "RECHAZADA": 3
    }

    return mapa.get(v, 1)

def get_user_id_by_persona(my, persona_id, default=1):
    if not persona_id:
        return default

    my.execute(
        "SELECT id FROM users WHERE persona_id = %s",
        (persona_id,)
    )
    r = my.fetchone()
    return r["id"] if r else default