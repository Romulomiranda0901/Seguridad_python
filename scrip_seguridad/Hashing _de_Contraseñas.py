import hashlib

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

if __name__ == "__main__":
    password = input("Introduce la contraseña a hashear: ")
    hashed_password = hash_password(password)
    print(f"Contraseña hasheada: {hashed_password}")