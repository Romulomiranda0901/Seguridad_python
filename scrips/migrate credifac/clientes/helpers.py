def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None


def dividir_nombre(t):
    t = clean(t)
    if not t:
        return None, None
    p = t.split()
    return p[0], " ".join(p[1:]) if len(p) > 1 else None


def dividir_dir(t, lim=255):
    t = clean(t)
    if not t:
        return "SIN DIRECCION", ""
    return t[:lim], t[lim:] if len(t) > lim else ""


def tel(t):
    t = clean(t)
    if not t:
        return None
    return t.replace(" ", "").replace("-", "")


def sexo_val(v):
    if not v:
        return 1
    v = str(v).upper()
    return 1 if v.startswith("M") else 2
