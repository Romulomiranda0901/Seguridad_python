import json
from googletrans import Translator

def traducir_json(input_file, output_file):
    # Inicializa el traductor
    translator = Translator()

    # Abre y carga el JSON
    with open(input_file, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Función recursiva para traducir el contenido del JSON
    def traducir_contenido(dato):
        if isinstance(dato, dict):
            return {key: traducir_contenido(value) for key, value in dato.items()}
        elif isinstance(dato, list):
            return [traducir_contenido(item) for item in dato]
        elif isinstance(dato, str):
            if dato:  # Verifica que la cadena no esté vacía
                try:
                    return translator.translate(dato, dest='es').text
                except Exception as e:
                    print(f"Error al traducir '{dato}': {e}")
                    return dato  # Devuelve el texto original en caso de error
            return dato  # Si la cadena está vacía, la devuelve directamente
        else:
            return dato  # Otros tipos de datos se devuelven sin cambios

    # Traducir el contenido
    datos_traducidos = traducir_contenido(data)

    # Guardar el JSON traducido
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(datos_traducidos, file, ensure_ascii=False, indent=4)

    print(f"Archivo traducido guardado como '{output_file}'.")

# Usar la función
input_json = "ciudades_del_mundo.json"  # Cambia esto al nombre de tu archivo JSON
output_json = "datos_traducidos.json"  # Nombre del archivo de salida
traducir_json(input_json, output_json)