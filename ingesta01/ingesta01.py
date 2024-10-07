import os
import requests
import csv
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

S3_BUCKET = os.getenv("S3_BUCKET", "bucket-para-ingesta")
CLIENTES_API_URL = "http://44.223.54.207:8001/clientes"
PEDIDOS_API_URL = "http://44.223.54.207:8001/pedidos"
DETALLE_PEDIDOS_API_URL = "http://44.223.54.207:8001/detalle_pedidos"

def upload_to_s3(s3_client, file_name, bucket, folder, object_name=None):
    if file_name is None or not os.path.exists(file_name):
        print(f"Error: El archivo {file_name} no existe.")
        return

    try:
        # Ajustar el nombre del archivo para que incluya la carpeta
        object_name = f"{folder}/{file_name}" if folder else file_name
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Archivo {file_name} subido a {bucket}/{object_name}.")
    except Exception as e:
        print(f"Error al subir el archivo a S3: {e}")

def fetch_data(api_url):
    """Función para obtener datos de la API y devolverlos en formato JSON"""
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Lanza una excepción si hay un error HTTP
        return response.json()  # Retorna los datos en formato JSON
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener los datos de la API: {e}")
        return None

def save_to_csv(data, filename):
    if data:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            first_key = list(data[0].keys())[0]  
            writer.writerow([first_key])
            
            for row in data:
                writer.writerow([row[first_key]])
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
        # Ingesta de clientes
        clientes_data = fetch_data(CLIENTES_API_URL)
        if clientes_data:
            clientes_csv_filename = 'clientes_data.csv'
            save_to_csv(clientes_data, clientes_csv_filename)
            upload_to_s3(s3_client, clientes_csv_filename, S3_BUCKET, folder="clientes")

        # Ingesta de pedidos
        pedidos_data = fetch_data(PEDIDOS_API_URL)
        if pedidos_data:
            pedidos_csv_filename = 'pedidos_data.csv'
            save_to_csv(pedidos_data, pedidos_csv_filename)
            upload_to_s3(s3_client, pedidos_csv_filename, S3_BUCKET, folder="pedidos")

        # Ingesta de detalles de pedidos
        detalle_pedidos_data = fetch_data(DETALLE_PEDIDOS_API_URL)
        if detalle_pedidos_data:
            detalle_pedidos_csv_filename = 'detalle_pedidos_data.csv'
            save_to_csv(detalle_pedidos_data, detalle_pedidos_csv_filename)
            upload_to_s3(s3_client, detalle_pedidos_csv_filename, S3_BUCKET, folder="detalle_pedidos")
    else:
        print("No se pudo crear el cliente de S3. Verifica tus credenciales.")

if __name__ == "__main__":
    main()
