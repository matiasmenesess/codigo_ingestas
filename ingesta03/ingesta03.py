import requests
import csv
import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# URL del microservicio MongoDB API para obtener reviews
API_URL = "http://107.20.212.250:8000/reviews"

# Nombre del bucket de S3
S3_BUCKET = "bucket-para-ingesta"

# Cargar las credenciales de AWS automáticamente
def load_aws_credentials():
    try:
        session = boto3.Session()
        s3_client = session.client('s3')
        return s3_client
    except NoCredentialsError:
        print("No se encontraron credenciales.")
        return None
    except PartialCredentialsError:
        print("Las credenciales son parciales o están incompletas.")
        return None

def fetch_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos de la API: {e}")
        return None

def save_to_csv(data, filename):
    if data:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(data[0].keys())
            for row in data:
                processed_row = [
                    f'"{value}"' if isinstance(value, str) and ',' in value else value
                    for value in row.values()
                ]
                writer.writerow(processed_row)
        print(f"Datos guardados en formato CSV en {filename}.")
    else:
        print("No hay datos para guardar en CSV.")

def upload_to_s3(s3_client, file_name, bucket, object_name=None):
    if file_name is None or not os.path.exists(file_name):
        print(f"Error: El archivo {file_name} no existe.")
        return

    if s3_client is None:
        print("Error: No se pudo crear el cliente de S3.")
        return

    try:
        object_name = object_name or file_name
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Archivo {file_name} subido a {bucket}/{object_name}.")
    except Exception as e:
        print(f"Error al subir el archivo a S3: {e}")

def main():
    s3_client = load_aws_credentials()

    # Ingesta de reviews
    reviews_data = fetch_data()
    reviews_csv_filename = 'reviews_data.csv'
    save_to_csv(reviews_data, reviews_csv_filename)
    upload_to_s3(s3_client, reviews_csv_filename, S3_BUCKET)

if __name__ == "__main__":
    main()
