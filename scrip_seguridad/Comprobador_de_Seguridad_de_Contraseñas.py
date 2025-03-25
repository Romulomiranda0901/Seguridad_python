def check_password_security(password):
    common_passwords = ['123456', 'password', '123456789', '12345', '12345678', 'qwerty', 'abc123']
    if password in common_passwords:
        print("La contraseña es débil. Usa una contraseña más segura.")
    else:
        print("La contraseña parece segura.")

if __name__ == "__main__":
    user_password = input("Introduce la contraseña a comprobar: ")
    check_password_security(user_password)