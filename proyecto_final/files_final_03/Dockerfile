# Servicio de scoring + guardado en BigQuery, para Cloud Run
FROM python:3.12-slim

WORKDIR /app

# Instalar dependencias primero (mejor cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el codigo
COPY main.py .

# Cloud Run inyecta el puerto en PORT (default 8080)
ENV PORT=8080
EXPOSE 8080

# gunicorn sirve la app Flask en produccion (mas robusto que flask run)
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 60 main:app
