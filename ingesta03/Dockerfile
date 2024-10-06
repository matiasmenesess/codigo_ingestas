# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar el archivo de requisitos y el código de la aplicación al contenedor
COPY requirements.txt requirements.txt
COPY ingesta03.py ingesta03.py

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Ejecutar el script Python
CMD ["python", "ingesta03.py"]
