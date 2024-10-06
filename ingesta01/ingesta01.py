import requests
import csv
import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# URL de las APIs de clientes, pedidos y detalle de pedidos
CLIENTES_API_URL = "http://44.223.54.207:8001/clientes"
PEDIDOS_API_URL = "http://44.223.54.207:8001/pedidos"
DETALLE_PEDIDOS_API_URL = "http://44.223.54.207:8001/detalle_pedidos"

# Nombre del bucket de S3
S3_BUCKET = "bucket-para-ingesta"

# Cargar las credenciales de AWS automáticamente
def load_aws_credentials():
    try:
        # Crear una sesión de boto3
        session = boto3.Session()

        # Crear un cliente de S3
        s3_client = session.client('s3')
        return s3_client

    except NoCredentialsError:
        print("No se encontraron credenciales.")
        return None

    except PartialCredentialsError:
        print("Las credenciales son parciales o están incompletas.")
        return None

def fetch_data(api_url):
    try:
        response = requests.get(api_url)
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
                writer.writerow(row.values())
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

    # Ingesta de clientes
    clientes_data = fetch_data(CLIENTES_API_URL)
    clientes_csv_filename = 'clientes_data.csv'
    save_to_csv(clientes_data, clientes_csv_filename)
    upload_to_s3(s3_client, clientes_csv_filename, S3_BUCKET)

    # Ingesta de pedidos
    pedidos_data = fetch_data(PEDIDOS_API_URL)
    pedidos_csv_filename = 'pedidos_data.csv'
    save_to_csv(pedidos_data, pedidos_csv_filename)
    upload_to_s3(s3_client, pedidos_csv_filename, S3_BUCKET)

    # Ingesta de detalles de pedidos
    detalle_pedidos_data = fetch_data(DETALLE_PEDIDOS_API_URL)
    detalle_pedidos_csv_filename = 'detalle_pedidos_data.csv'
    save_to_csv(detalle_pedidos_data, detalle_pedidos_csv_filename)
    upload_to_s3(s3_client, detalle_pedidos_csv_filename, S3_BUCKET)

if __name__ == "__main__":
    main()
