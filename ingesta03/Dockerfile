# Usar una imagen base de Python
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
COPY ingesta02.py ingesta02.py

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Ejecutar el script Python
CMD ["python", "ingesta02.py"]
