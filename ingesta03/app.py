
import requests
import csv
import boto3
import os

# URL del microservicio MongoDB API
API_URL = "http://107.20.212.250:8000/reviews"

# Configuración de AWS
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_FOLDER = "review/"

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
            # Escribir los registros
            for row in data:
                writer.writerow(row.values())
        print(f"Datos guardados en formato CSV en {filename}.")
    else:
        print("No hay datos para guardar en CSV.")

def upload_to_s3(file_name, bucket, object_name=None):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    try:
        if object_name is None:
            object_name = S3_FOLDER + file_name
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Archivo {file_name} subido a {bucket}/{object_name}.")
    except Exception as e:
        print(f"Error al subir el archivo a S3: {e}")

def main():
    data = fetch_data()

    csv_filename = 'reviews_data.csv'

    save_to_csv(data, csv_filename)

    upload_to_s3(csv_filename, S3_BUCKET)

if __name__ == "__main__":
    main()
