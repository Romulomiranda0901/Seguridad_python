import random
import string

def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    password = ''.join(random.choice(characters) for _ in range(length))
    return password

if __name__ == "__main__":
    password_length = int(input("Introduce la longitud de la contraseña: "))
    password = generate_password(password_length)
    print(f"Contraseña generada: {password}")