import subprocess
import time
import os

def run_full_pipeline():
    print(f"\n--- 🚀 Démarrage du Pipeline : {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    try:
        # 1. Passage Bronze -> Silver (Nettoyage)
        print("🛠️ Étape 1 : Nettoyage (Bronze ➡️ Silver)...")
        subprocess.run(["python", "transform_silver.py"], check=True)
        
        # 2. Passage Silver -> Gold (Agrégation)
        print("🏆 Étape 2 : Agrégation (Silver ➡️ Gold)...")
        subprocess.run(["python", "create_gold.py"], check=True)
        
        print("✅ Cycle terminé avec succès !")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur durant le pipeline : {e}")

# Boucle d'automatisation (ex: toutes les 5 minutes)
if __name__ == "__main__":
    while True:
        run_full_pipeline()
        print("\n😴 En attente du prochain cycle (5 minutes)...")
        time.sleep(300)