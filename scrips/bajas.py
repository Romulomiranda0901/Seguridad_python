import pandas as pd
import psycopg2
import unicodedata

def clean_name(name):
    """ Limpia el nombre eliminando acentos, espacios y convirtiendo a mayúsculas. """
    if isinstance(name, str):
        name = ''.join((c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn'))
        return name.strip().upper()  # Convertir a mayúsculas
    return ''  # Devolver una cadena vacía si no es una cadena

def clean_apellidos(apellido):
    """ Elimina textos no deseados de los apellidos. """
    unwanted_texts = [
        '( EXT. UDO ) AUXILIAR ENFERMERIA',
        '(ext.udo AUXILIAR)',
        '(EXT. UDO LA GUINEA)',
        '(EXT. UDO)',
        '( EXT. UDO)',
        '(UDO)'
    ]
    for text in unwanted_texts:
        apellido = apellido.replace(text, '')  # Reemplazar texto no deseado
    return clean_name(apellido)  # Limpiar el apellido

def update_apellidos(db_config):
    """ Limpia los apellidos en la base de datos eliminando textos no deseados. """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Obtener todos los apellidos
    cursor.execute("SELECT id, apellidos FROM general.clientes;")
    clientes = cursor.fetchall()

    for client in clientes:
        client_id = client[0]
        apellidos_limpios = clean_apellidos(client[1])  # Limpiar los apellidos
        # Actualizar los apellidos en la base de datos si han cambiado
        cursor.execute("""
            UPDATE general.clientes
            SET apellidos = %s
            WHERE id = %s AND apellidos != %s;
        """, (apellidos_limpios, client_id, apellidos_limpios))

    # Confirmar los cambios
    conn.commit()

    # Cerrar la conexión
    cursor.close()
    conn.close()

def extract_names_from_excel(file_path):
    """ Extrae y limpia nombres y apellidos desde un archivo de Excel. """
    xls = pd.ExcelFile(file_path)
    sheet1 = pd.read_excel(xls, sheet_name=0)  # Hoja 1
    names_sheet1 = sheet1['Nombres y Apellidos'][1:].tolist()  # Obtener nombres de D2 en adelante

    sheet2 = pd.read_excel(xls, sheet_name=1)  # Hoja 2
    names_sheet2 = sheet2['NOMBRES'][1:].tolist()  # Nombres de D2 en adelante
    surnames_sheet2 = sheet2['APELLIDOS'][1:].tolist()  # Apellidos de E2 en adelante

    # Combinar nombres y apellidos en hoja 2
    full_names_sheet2 = [f"{names_sheet2[i]} {clean_apellidos(surnames_sheet2[i])}" for i in range(len(names_sheet2))]

    all_names = names_sheet1 + full_names_sheet2
    cleaned_names = [clean_name(name) for name in all_names if clean_name(name)]
    return cleaned_names

def get_clients(db_config):
    """ Obtiene clientes de la base de datos y limpia los apellidos. """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("SELECT id, CONCAT(nombres, ' ', apellidos) AS full_name FROM general.clientes;")
    clients = cursor.fetchall()

    clients_dict = {clean_name(client[1]): client[0] for client in clients}  # {full_name: id}

    cursor.close()
    conn.close()

    return clients_dict

def update_clients(db_config, clients_dict, names):
    """ Actualiza los registros de clientes en la base de datos. """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    updated_count = 0
    not_found = []

    for name in names:
        client_id = clients_dict.get(name.strip())

        if client_id:
            update_query = """
            UPDATE general.clientes
            SET activo = 'NO', eliminado = 'SI'
            WHERE id = %s;
            """
            cursor.execute(update_query, (client_id,))
            updated_count += 1
        else:
            not_found.append(name)

    conn.commit()
    cursor.close()
    conn.close()

    return updated_count, not_found

def save_not_found_to_excel(not_found, filename='nombres_no_encontrados.xlsx'):
    """ Guarda los registros no encontrados en un archivo Excel. """
    df = pd.DataFrame(not_found, columns=['NOMBRES NO ENCONTRADOS'])  # Nombres de columna en mayúsculas
    df.to_excel(filename, index=False)

# Configuración de la base de datos
db_config = {
    'dbname': 'siscoga',
    'user': 'postgres',
    'password': 'postgres',
    'host': '192.168.100.36',  # Cambia esto si tu base de datos está en otro host
    'port': '5432'  # Cambia esto si usas un puerto diferente
}

# Primero, limpia los apellidos en la base de datos
update_apellidos(db_config)

# Ruta al archivo de Excel
file_path = 'Bajas.xlsx'

# Extraer los nombres de Excel
combined_names = extract_names_from_excel(file_path)

# Obtener los clientes de la base de datos
clients_dict = get_clients(db_config)

# Actualizar la base de datos usando los IDs de clientes
updated_count, not_found = update_clients(db_config, clients_dict, combined_names)

# Guardar los nombres no encontrados en un archivo Excel
save_not_found_to_excel(not_found, 'nombres_no_encontrados.xlsx')

# Resultados
print(f"Número de registros actualizados: {updated_count}")
print("Registros no encontrados:")
for name in not_found:
    print(name)