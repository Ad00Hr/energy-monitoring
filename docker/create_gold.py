import pandas as pd
import os
import glob

SILVER_PATH = "lakehouse/silver/"
GOLD_PATH = "lakehouse/gold/"

os.makedirs(GOLD_PATH, exist_ok=True)

def run_gold():
    print("🏆 Transformation Silver ➡️ Gold...")

    parquet_files = glob.glob(os.path.join(SILVER_PATH, "*.parquet"))
    
    if not parquet_files:
        print("⚠️ Silver vide.")
        return

    df_list = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(df_list, ignore_index=True)

    # Agrégation par JOUR
    df['date'] = df['timestamp'].dt.date
    
    df_gold = df.groupby(['date', 'region']).agg(
        avg_power=('power_kw', 'mean'),
        max_power=('power_kw', 'max'),
        total_records=('power_kw', 'count')
    ).reset_index()

    output_file = os.path.join(GOLD_PATH, "daily_stats.parquet")
    df_gold.to_parquet(output_file, index=False)
    
    print(f"✅ Gold mis à jour : {len(df_gold)} lignes.")

if __name__ == "__main__":
    run_gold()