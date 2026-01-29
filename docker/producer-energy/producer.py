import json
import time
import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:29092"
TOPIC = "energy_raw"

# Charger le CSV
df = pd.read_csv("./data/time_series_60min_singleindex.csv")

# Colonne consommation pour l'Allemagne
country_col = "DE_load_actual_entsoe_transparency"
required_cols = ["utc_timestamp", country_col]

# Vérifier colonnes
for col in required_cols:
    if col not in df.columns:
        raise Exception(f"Colonne manquante : {col}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Envoyer chaque ligne comme événement Kafka
for _, row in df.iterrows():
    if pd.isna(row[country_col]):
        continue

    event = {
        "meter_id": "DE",
        "region": "DE",
        "timestamp": row["utc_timestamp"],
        "power_kw": row[country_col],
        "source": "OPSD"
    }

    producer.send(TOPIC, event)
    print(f"Envoyé: {event}")
    time.sleep(0.2)  # simuler le flux temps réel

producer.flush()
producer.close()
