import os
import json
import pandas as pd

# Chemins
BRONZE_PATH = "../lakehouse/bronze/"
SILVER_PATH = "../lakehouse/silver/"

if not os.path.exists(SILVER_PATH):
    os.makedirs(SILVER_PATH)

print("✨ Transformation Bronze ➡️ Silver en cours...")

# Parcourir les fichiers de la couche Bronze
for file in os.listdir(BRONZE_PATH):
    if file.endswith(".json"):
        # 1. Lecture
        with open(os.path.join(BRONZE_PATH, file), 'r') as f:
            data = [json.loads(line) for line in f]
        
        df = pd.DataFrame(data)

        # 2. Nettoyage (Data Engineering)
        # Convertir le timestamp en vrai format Date
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Supprimer les lignes avec des valeurs de puissance vides ou négatives
        df = df.dropna(subset=['power_kw'])
        df = df[df['power_kw'] > 0]

        # 3. Écriture en format Parquet (Standard du Big Data pour la performance)
        file_name = file.replace(".json", ".parquet")
        df.to_parquet(os.path.join(SILVER_PATH, file_name), index=False)
        print(f"✅ Fichier nettoyé et converti : {file_name}")