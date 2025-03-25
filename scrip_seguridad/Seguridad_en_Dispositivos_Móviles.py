from androguard.core import androconf
from androguard.core.bytecodes.apk import APK

def scan_apk_permissions(apk_path):
    apk = APK(apk_path)
    permissions = apk.get_permissions()

    print(f"Permisos solicitados por {apk.get_app_name()}:")
    for perm in permissions:
        print(perm)

if __name__ == "__main__":
    apk_file = input("Introduce la ruta del APK: ")
    scan_apk_permissions(apk_file)