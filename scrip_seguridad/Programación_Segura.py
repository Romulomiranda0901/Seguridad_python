import re

def sanitize_input(user_input):
    # Eliminamos caracteres peligrosos y validamos direcciones de email
    sanitized = re.sub(r'[<>]', '', user_input)  # Eliminamos < y >
    return sanitized.strip()

if __name__ == "__main__":
    user_input = input("Introduce un texto (se eliminarán caracteres peligrosos): ")
    safe_input = sanitize_input(user_input)
    print(f"Entrada sanitizada: {safe_input}")