import requests
import csv
import boto3
import os

# URL del microservicio MongoDB API
API_URL = "http://107.20.212.250:8000/reviews"

# Configuración de AWS desde las variables de entorno
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY_ID = "ASIAQR5DRZIJVF35QDAN"
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
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
    """Función para guardar los datos en formato CSV"""
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

def upload_to_s3(file_name, bucket, object_name=None):
    """Función para cargar el archivo a S3"""
    
    if file_name is None or not os.path.exists(file_name):
        print(f"Error: El archivo {file_name} no existe o es None.")
        return
    
    if bucket is None:
        print("Error: El nombre del bucket es None.")
        return
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )
    
    try:
        if object_name is None:
            object_name = S3_FOLDER + file_name
        
        print(f"Subiendo el archivo {file_name} al bucket {bucket} como {object_name}.")
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
