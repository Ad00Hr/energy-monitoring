import pandas as pd
import os

# Chemins
SILVER_PATH = "../lakehouse/silver/"
GOLD_PATH = "../lakehouse/gold/"

if not os.path.exists(GOLD_PATH):
    os.makedirs(GOLD_PATH)

print("🏆 Génération de la Couche Gold (Indicateurs Business)...")

# 1. Lire tous les fichiers Parquet de la couche Silver
all_files = [os.path.join(SILVER_PATH, f) for f in os.listdir(SILVER_PATH) if f.endswith('.parquet')]
df_list = [pd.read_parquet(f) for f in all_files]
df_silver = pd.concat(df_list)

# 2. Transformation : Agrégation par jour
# On extrait la date (jour) du timestamp
df_silver['day'] = df_silver['timestamp'].dt.date

gold_df = df_silver.groupby('day').agg({
    'power_kw': ['mean', 'min', 'max', 'sum'],
    'meter_id': 'first'
}).reset_index()

# Renommer les colonnes pour que ce soit plus clair
gold_df.columns = ['date', 'avg_power', 'min_power', 'max_power', 'total_consumption', 'country']

# 3. Sauvegarde
gold_df.to_parquet(os.path.join(GOLD_PATH, "daily_energy_stats.parquet"), index=False)

print(f"✅ Couche Gold terminée ! Fichier généré : {os.path.join(GOLD_PATH, 'daily_energy_stats.parquet')}")
print(gold_df.head()) # Affiche les premières lignes pour vérifier