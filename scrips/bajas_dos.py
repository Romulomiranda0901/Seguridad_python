import pandas as pd
import psycopg2
import unicodedata

def clean_name(name):
    """ Limpia el nombre eliminando acentos, espacios y convirtiendo a minúsculas. """
    if isinstance(name, str):
        name = ''.join((c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn'))
        return name.strip().lower()  # Convertir a minúsculas
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

def get_clients(db_config):
    """ Obtiene clientes de la base de datos y devuelve un diccionario de nombres limpios. """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("SELECT id, CONCAT(apellidos, ' ', nombres) AS full_name FROM general.clientes;")
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

def save_not_found_to_excel(not_found, filename='nombres_no_encontrados_dos.xlsx'):
    """ Guarda los registros no encontrados en un archivo Excel en mayúsculas. """
    # Convertir los nombres a mayúsculas
    not_found_upper = [name.upper() for name in not_found]
    df = pd.DataFrame(not_found_upper, columns=['NOMBRES NO ENCONTRADOS'])  # Nombres de columna en mayúsculas
    df.to_excel(filename, index=False)

def read_not_found_from_excel(file_path):
    """ Lee los nombres no encontrados desde un archivo Excel. """
    df = pd.read_excel(file_path)
    return df['NOMBRES NO ENCONTRADOS'].tolist()  # Obtiene la lista de nombres no encontrados

# Configuración de la base de datos
db_config = {
    'dbname': 'siscoga',
    'user': 'postgres',
    'password': 'postgres',
    'host': '192.168.100.36',  # Cambia esto si tu base de datos está en otro host
    'port': '5432'  # Cambia esto si usas un puerto diferente
}


# Ruta al archivo de Excel de nombres no encontrados
not_found_file_path = 'nombres_no_encontrados.xlsx'

# Leer los nombres no encontrados desde el archivo Excel
not_found_names = read_not_found_from_excel(not_found_file_path)

# Crear una nueva lista de nombres para comparación
# Apellidos al inicio y nombres al final, en minúsculas
comparison_names = [
    clean_apellidos(name.split(' ')[0]) + ' ' + clean_name(' '.join(name.split(' ')[1:]))
    for name in not_found_names
]

# Obtener los clientes de la base de datos
clients_dict = get_clients(db_config)

# Actualizar la base de datos usando los IDs de clientes
updated_count, not_found = update_clients(db_config, clients_dict, comparison_names)

# Guardar los nombres no encontrados en un nuevo archivo Excel
save_not_found_to_excel(not_found, 'nombres_no_encontrados_dos.xlsx')

# Resultados finales
print(f"Número de registros actualizados: {updated_count}")
print("Registros no encontrados:")
for name in not_found:
    print(name)