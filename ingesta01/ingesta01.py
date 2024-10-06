def upload_to_s3(s3_client, file_name, bucket, folder, object_name=None):
    if file_name is None or not os.path.exists(file_name):
        print(f"Error: El archivo {file_name} no existe.")
        return

    if s3_client is None:
        print("Error: No se pudo crear el cliente de S3.")
        return

    try:
        # Ajustar el nombre del archivo para que incluya la carpeta
        object_name = f"{folder}/{file_name}" if folder else file_name
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
    upload_to_s3(s3_client, clientes_csv_filename, S3_BUCKET, folder="clientes")

    # Ingesta de pedidos
    pedidos_data = fetch_data(PEDIDOS_API_URL)
    pedidos_csv_filename = 'pedidos_data.csv'
    save_to_csv(pedidos_data, pedidos_csv_filename)
    upload_to_s3(s3_client, pedidos_csv_filename, S3_BUCKET, folder="pedidos")

    # Ingesta de detalles de pedidos
    detalle_pedidos_data = fetch_data(DETALLE_PEDIDOS_API_URL)
    detalle_pedidos_csv_filename = 'detalle_pedidos_data.csv'
    save_to_csv(detalle_pedidos_data, detalle_pedidos_csv_filename)
    upload_to_s3(s3_client, detalle_pedidos_csv_filename, S3_BUCKET, folder="detalle_pedidos")

if __name__ == "__main__":
    main()
