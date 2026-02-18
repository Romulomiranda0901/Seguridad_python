# ================= UTILIDADES GENERALES =================

def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None

def get_empleado_id_by_persona(my, persona_id, default=1):
    if not persona_id:
        return default

    my.execute(
        "SELECT id FROM empleados WHERE persona_id = %s",
        (persona_id,)
    )
    r = my.fetchone()
    return r["id"] if r else default


def get_user_id_by_persona(my, persona_id, default=1):
    if not persona_id:
        return default

    my.execute(
        "SELECT id FROM users WHERE persona_id = %s",
        (persona_id,)
    )
    r = my.fetchone()
    return r["id"] if r else default


def safe_int(value):
    if value is None:
        return 0

    try:
        v = int(value)
        if v > 2147483647:
            v = int(str(v)[-9:])
        return v
    except:
        return 0

