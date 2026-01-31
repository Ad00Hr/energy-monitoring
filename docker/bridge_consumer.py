import json
import os
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# --- CONFIGURATION ---
DB_PARAMS = "dbname=energy_db user=admin password=password123 host=localhost port=5432"
KAFKA_SERVER = "localhost:9092" 
TOPIC = 'energy_raw'

# Chemin vers ton architecture Médaillon (on remonte d'un cran car on est dans le dossier docker)
BRONZE_PATH = os.path.join("..", "lakehouse", "bronze")

# Créer le dossier bronze s'il n'existe pas encore
if not os.path.exists(BRONZE_PATH):
    os.makedirs(BRONZE_PATH)

try:
    # 1. Connexion à TimescaleDB
    conn = psycopg2.connect(DB_PARAMS)
    cur = conn.cursor()
    print("✅ Connecté à TimescaleDB !")

    # 2. Connexion à Kafka (auto_offset_reset='earliest' permet de tout récupérer)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest', 
        group_id='storage-group', # Identifiant pour ne pas perdre le fil
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("🚀 Pont activé : Kafka ➡️ TimescaleDB + Lakehouse Bronze...")

    for msg in consumer:
        data = msg.value
        
        # --- PARTIE 1 : STOCKAGE TIMESCALEDB ---
        cur.execute(
            "INSERT INTO energy_data (time, meter_id, region, power_kw, source) VALUES (%s, %s, %s, %s, %s)",
            (data['timestamp'], data['meter_id'], data['region'], data['power_kw'], data['source'])
        )
        conn.commit()

        # --- PARTIE 2 : STOCKAGE LAKEHOUSE (BRONZE) ---
        # On crée un fichier par jour pour organiser la couche Bronze
        date_str = data['timestamp'][:10] # Extrait "2020-01-01"
        file_name = f"raw_energy_{date_str}.json"
        full_path = os.path.join(BRONZE_PATH, file_name)

        with open(full_path, "a") as f:
            f.write(json.dumps(data) + "\n")

        print(f"📥 Archivé & Stocké : {data['timestamp']} | {data['power_kw']} kW")

except Exception as e:
    print(f"❌ Erreur critique : {e}")
finally:
    if 'conn' in locals():
        conn.close()