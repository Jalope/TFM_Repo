# etl/Dockerfile
FROM python:3.10-slim

# Directorio de trabajo
WORKDIR /app

# Instala dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código ETL
COPY . .

# Por defecto, no arrancamos ningún script (“CMD” se sobreescribe en docker-compose)
CMD ["sleep", "infinity"]