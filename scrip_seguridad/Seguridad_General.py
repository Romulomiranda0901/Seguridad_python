import hashlib

def hash_file(filename):
    """Genera el hash SHA-256 del archivo"""
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as f:
        # Leer el archivo en bloques de 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

if __name__ == "__main__":
    file_path = input("Introduce la ruta del archivo a comprobar: ")
    file_hash = hash_file(file_path)
    print(f"El hash SHA-256 del archivo es: {file_hash}")