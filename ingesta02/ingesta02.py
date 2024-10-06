import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

S3_BUCKET = os.getenv("S3_BUCKET", "bucket-para-ingesta")
LIBROS_API_URL = "http://107.20.212.250:8080/api/libros"
CATEGORIAS_API_URL = "http://107.20.212.250:8080/api/categorias"
AUTORES_API_URL = "http://107.20.212.250:8080/api/autores"

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
    # Implementa tu lógica para obtener datos de la API
    pass

def save_to_csv(data, filename):
    # Implementa tu lógica para guardar datos en CSV
    pass

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

    # Ingesta de libros
    libros_data = fetch_data(LIBROS_API_URL)
    libros_csv_filename = 'libros_data.csv'
    save_to_csv(libros_data, libros_csv_filename)
    upload_to_s3(s3_client, libros_csv_filename, S3_BUCKET, folder="libro")

    # Ingesta de categorías
    categorias_data = fetch_data(CATEGORIAS_API_URL)
    categorias_csv_filename = 'categorias_data.csv'
    save_to_csv(categorias_data, categorias_csv_filename)
    upload_to_s3(s3_client, categorias_csv_filename, S3_BUCKET, folder="categoria")

    # Ingesta de autores
    autores_data = fetch_data(AUTORES_API_URL)
    autores_csv_filename = 'autores_data.csv'
    save_to_csv(autores_data, autores_csv_filename)
    upload_to_s3(s3_client, autores_csv_filename, S3_BUCKET, folder="autor")

if __name__ == "__main__":
    main()
