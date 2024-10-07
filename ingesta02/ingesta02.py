import os
import requests
import csv
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import json

S3_BUCKET = os.getenv("S3_BUCKET", "bucket-para-ingesta")
LIBROS_API_URL = "http://107.20.212.250:8080/api/libros"
CATEGORIAS_API_URL = "http://107.20.212.250:8080/api/categorias"
AUTORES_API_URL = "http://107.20.212.250:8080/api/autores"

def upload_to_s3(s3_client, file_name, bucket, folder, object_name=None):
    if file_name is None or not os.path.exists(file_name):
        print(f"Error: El archivo {file_name} no existe.")
        return

    try:
        object_name = f"{folder}/{file_name}" if folder else file_name
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Archivo {file_name} subido a {bucket}/{object_name}.")
    except Exception as e:
        print(f"Error al subir el archivo a S3: {e}")

def fetch_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos de la API: {e}")
        return None

def extract_first_attribute_if_json(value):
    try:
        # Intentar cargar el valor como JSON (si ya es un diccionario no hace falta convertir)
        if isinstance(value, str):
            value = json.loads(value)  # Convertir cadena a diccionario
        
        # Si es un diccionario, extraemos el primer atributo
        if isinstance(value, dict):
            return list(value.values())[0]
        else:
            return value  # Si no es un diccionario, devolver el valor tal cual
    except (ValueError, TypeError):  # Si no es un JSON válido, devolver el valor tal cual
        return value

def save_to_csv(data, filename):
    if data:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            writer.writerow(data[0].keys())
            
            for row in data:
                # Revisar cada valor de la fila y extraer el primer atributo si es un JSON
                new_row = [extract_first_attribute_if_json(value) for value in row.values()]
                writer.writerow(new_row)
        print(f"Datos guardados en formato CSV en {filename}.")
    else:
        print("No hay datos para guardar en CSV.")

def load_aws_credentials():
    try:
        session = boto3.Session()
        return session.client('s3')
    except NoCredentialsError:
        print("Error: No se encontraron credenciales.")
        return None
    except PartialCredentialsError:
        print("Error: Credenciales incompletas.")
        return None

def main():
    s3_client = load_aws_credentials()

    if s3_client:
        # Ingesta de libros
        libros_data = fetch_data(LIBROS_API_URL)
        if libros_data:  # Verificamos si se obtuvieron datos antes de continuar
            libros_csv_filename = 'libros_data.csv'
            save_to_csv(libros_data, libros_csv_filename)
            upload_to_s3(s3_client, libros_csv_filename, S3_BUCKET, folder="libro")

        # Ingesta de categorías
        categorias_data = fetch_data(CATEGORIAS_API_URL)
        if categorias_data:  # Verificamos si se obtuvieron datos antes de continuar
            categorias_csv_filename = 'categorias_data.csv'
            save_to_csv(categorias_data, categorias_csv_filename)
            upload_to_s3(s3_client, categorias_csv_filename, S3_BUCKET, folder="categoria")

        # Ingesta de autores
        autores_data = fetch_data(AUTORES_API_URL)
        if autores_data:  # Verificamos si se obtuvieron datos antes de continuar
            autores_csv_filename = 'autores_data.csv'
            save_to_csv(autores_data, autores_csv_filename)
            upload_to_s3(s3_client, autores_csv_filename, S3_BUCKET, folder="autor")
    else:
        print("No se pudo crear el cliente de S3. Verifica tus credenciales.")

if __name__ == "__main__":
    main()
