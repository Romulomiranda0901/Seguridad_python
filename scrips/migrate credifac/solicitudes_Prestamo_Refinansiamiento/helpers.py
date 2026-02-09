# ================= UTILIDADES GENERALES =================

def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None



def get_user_id_by_persona(my, persona_id, default=1):
    if not persona_id:
        return default

    my.execute(
        "SELECT id FROM users WHERE persona_id = %s",
        (persona_id,)
    )
    r = my.fetchone()
    return r["id"] if r else default