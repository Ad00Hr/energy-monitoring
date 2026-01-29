# Dockerfile pour le producteur Kafka
FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code et les données
COPY producer.py .
COPY data/ ./data/

# Commande par défaut
CMD ["python", "producer.py"]
