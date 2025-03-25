import os
import base64

def generate_access_token(length=32):
    token = base64.b64encode(os.urandom(length)).decode('utf-8')
    return token

if __name__ == "__main__":
    token_length = int(input("Introduce la longitud del token: "))
    token = generate_access_token(token_length)
    print(f"Token de acceso generado: {token}")