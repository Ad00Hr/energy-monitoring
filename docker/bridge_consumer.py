import json
import os
import psycopg2
from kafka import KafkaConsumer
import time

# --- CONFIGURATION DOCKER (CRITIQUE) ---
# On utilise les noms des services du docker-compose
DB_PARAMS = "dbname=energy_db user=admin password=password123 host=timescaledb port=5432"
KAFKA_SERVER = "kafka:29092" 
TOPIC = 'energy_raw'

# Chemin interne au conteneur (/app/lakehouse/...)
BRONZE_PATH = "lakehouse/bronze"

# Création du dossier si inexistant
os.makedirs(BRONZE_PATH, exist_ok=True)

def demarrer_pont():
    print("⏳ Attente de 15s pour le démarrage des services...")
    time.sleep(15) 

    try:
        # 1. Connexion TimescaleDB
        conn = psycopg2.connect(DB_PARAMS)
        cur = conn.cursor()
        print("✅ Connecté à TimescaleDB !")

        # 2. Connexion Kafka
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            auto_offset_reset='earliest', 
            group_id='storage-group-v2', # Changé pour forcer la relecture
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"🚀 Bridge Consumer activé : Écoute de {TOPIC}...")

        for msg in consumer:
            data = msg.value
            
            # A. Insertion TimescaleDB (Temps réel)
            try:
                cur.execute(
                    "INSERT INTO energy_data (time, meter_id, region, power_kw, source) VALUES (%s, %s, %s, %s, %s)",
                    (data['timestamp'], data['meter_id'], data['region'], data['power_kw'], data['source'])
                )
                conn.commit()
            except Exception as e:
                # On ignore les doublons (Primary Key violation) pour ne pas crasher
                conn.rollback()

            # B. Sauvegarde Lakehouse Bronze (Historique)
            date_str = data['timestamp'][:10] 
            file_name = f"raw_{date_str}.json"
            full_path = os.path.join(BRONZE_PATH, file_name)

            with open(full_path, "a") as f:
                f.write(json.dumps(data) + "\n")

            # Petit log pour dire que ça marche (1 message sur 10 pour pas spammer)
            # print(f"📥 Reçu : {data['timestamp']}")

    except Exception as e:
        print(f"❌ Erreur critique Bridge : {e}")

if __name__ == "__main__":
    demarrer_pont()