import os
import requests
import csv
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

S3_BUCKET = os.getenv("S3_BUCKET", "bucket-para-ingesta")
REVIEWS_API_URL = "http://107.20.212.250:8000/reviews"

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
        return response.json()  # Asume que la API devuelve JSON
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos de la API: {e}")
        return None

def save_to_csv(data, filename):
    if data:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # Escribir encabezados usando las llaves del primer elemento de la lista (si es un dict)
            writer.writerow(data[0].keys())
            
            # Escribir los registros (suponemos que cada registro es un diccionario)
            for row in data:
                writer.writerow(row.values())
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

    # Ingesta de reviews
    reviews_data = fetch_data(REVIEWS_API_URL)
    if reviews_data:  # Verificamos si se obtuvieron datos antes de continuar
        reviews_csv_filename = 'reviews_data.csv'
        save_to_csv(reviews_data, reviews_csv_filename)
        upload_to_s3(s3_client, reviews_csv_filename, S3_BUCKET, folder="review")
    else:
        print("No se obtuvieron datos de la API.")

if __name__ == "__main__":
    main()
