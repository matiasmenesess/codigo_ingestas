# Usar una imagen base de Python
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
COPY ingesta03.py ingesta03.py

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Ejecutar el script Python
CMD ["python", "ingesta03.py"]
