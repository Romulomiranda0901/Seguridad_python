# ================= UTILIDADES GENERALES =================

def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None


# ================= SOLICITUDES =================

# tipo solicitud:
# NULL -> 1
# cualquier valor -> 3
def tipo_solicitud_val(v):
    return 1 if v is None else 3


def estado_val(v):
    """
    estado solicitud:
    BORRADOR=1
    SOLICITADO=2
    ANALIZADO=3
    EN_REVISION=4
    PARA_CORREGIR=5
    CORREGIDO=6
    APROBADO=7
    RECHAZADO=8
    DESEMBOLSADO=9
    READECUADO=10

    También soporta texto:
    APROBADA
    RECHAZADA
    DESEMBOLSADA
    """

    if not v:
        return 1

    # si ya viene numérico desde SQL Server
    if isinstance(v, (int, float)):
        return int(v)

    v = str(v).strip().upper()

    mapa = {
        "APROBADA": 7,
        "RECHAZADA": 8,
        "DESEMBOLSADA": 9
    }

    return mapa.get(v, 1)


def estado_verificacion_val(v):
    """
    verificación:
    1=PENDIENTE
    2=EN PROCESO
    3=INCOMPLETO
    4=VERIFICADO

    También soporta texto:
    NO VERIFICADA
    VERIFICADA
    """

    if not v:
        return 1

    if isinstance(v, (int, float)):
        return int(v)

    v = str(v).strip().upper()

    mapa = {
        "NO VERIFICADA": 1,
        "VERIFICADA": 4,
        "DESEMBOLSADA": 4,
        "RECHAZADA": 1,
        "APROBADA": 2
    }

    return mapa.get(v, 1)
