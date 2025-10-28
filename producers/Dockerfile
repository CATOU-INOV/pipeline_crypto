# Dockerfile pour crypto-producer
FROM python:3.11-slim

# Installer les dépendances système pour psycopg2
RUN apt-get update && \
    apt-get install -y build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Définir le répertoire de travail
WORKDIR /app

# Copier le code
COPY . /app

# Installer les dépendances Python
RUN pip install --no-cache-dir \
    kafka-python \
    requests \
    psycopg2-binary

# Point d'entrée
CMD ["python", "crypto_collector.py"]
