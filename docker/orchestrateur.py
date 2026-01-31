import time
import subprocess
import os
from datetime import datetime

def run_spark_job():
    print(f"\n[{datetime.now()}] 🚀 Lancement du Job Spark (Bronze -> Silver)...")
    
    # La commande EXACTE pour l'image Apache Spark officielle
    command = [
        "docker", "exec", "spark-master", 
        "/opt/spark/bin/spark-submit", 
        "--packages", "io.delta:delta-spark_2.12:3.0.0", 
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", 
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", 
        "/app/job_silver_delta.py"
    ]
    
    try:
        print("⏳ Traitement en cours (cela peut prendre 30 à 60 secondes)...")
        
        # --- CORRECTION ICI ---
        # On force l'UTF-8 et on dit à Python de remplacer les erreurs par des "?" au lieu de crasher
        result = subprocess.run(
            command, 
            capture_output=True, 
            encoding='utf-8', 
            errors='replace'  # C'est ça qui sauve ton script sur Windows
        )
        
        if result.returncode == 0:
            print("✅ SUCCÈS : Le Job Spark Delta Lake est terminé !")
            # On vérifie que stdout n'est pas vide avant d'afficher
            if result.stdout:
                print("--- Logs Spark (Dernières lignes) ---")
                print(result.stdout[-500:]) 
        else:
            print("❌ ERREUR Spark :")
            print(result.stderr)
            
    except Exception as e:
        print(f"⚠️ Erreur système : {e}")

def main():
    print("🤖 Démarrage de l'Orchestrateur (Membre 3)")
    print("📝 Tâche : Ingestion Spark + Delta Lake")
    print("📅 Fréquence : Toutes les 60 secondes\n")
    
    while True:
        # 1. Lancer le traitement
        run_spark_job()
        
        # 2. Attendre 60 secondes
        print("\n💤 Pause de 60 secondes avant le prochain cycle...")
        time.sleep(60)

if __name__ == "__main__":
    main()