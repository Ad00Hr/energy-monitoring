# Partie Ingestion – Supervision Énergétique (Temps Réel)

## Objectif

Cette partie du projet consiste à **ingérer les données de consommation électrique** depuis le dataset OPSD et à les pousser en **temps réel vers Kafka**. L’objectif est de simuler un flux continu de mesures pour la consommation instantanée et historique, prêt pour le calcul d’indicateurs et dashboards.

---


## Technologies Utilisées

- **Python 3.11** : producteur Kafka  
- **Kafka / Zookeeper (Confluent)** : bus de messages pour ingestion en temps réel  
- **Docker / Docker Compose** : containerisation et orchestration des services  
- **Kafka UI** : visualisation du flux et vérification des messages  
- **Pandas** : traitement initial des CSV OPSD  

---

## Architecture et Composants

### 1. Producteur Python (`producer.py`)

- Lit le fichier CSV `time_series_60min_singleindex.csv` depuis le dossier `data/`.  
- Sélectionne les colonnes de consommation électrique par pays (ex. Allemagne : `DE_load_actual_entsoe_transparency`).  
- Transforme chaque ligne en message JSON pour Kafka :

```json
{
  "meter_id": "DE",
  "region": "DE",
  "timestamp": "2020-01-01T00:00:00",
  "power_kw": 52341.2,
  "source": "OPSD"
}


# Docker Compose : Lancer Kafka, Zookeeper, Kafka UI et le producteur
docker compose up --build -d

# Vérifier que les conteneurs tournent
docker ps

# Construire l'image Docker du producteur
docker build -t producer-energy ./producer-energy

# Lancer le producteur en réseau Docker pour communiquer avec Kafka
docker run --network=energy-monitoring-realtime_default producer-energy

# Vérifier les logs du producteur en direct
docker logs -f producer-energy

# Entrer dans le conteneur Kafka pour consommer les messages
docker exec -it kafka bash

# Lire les messages du topic 'energy_raw' (consommation réelle) depuis Kafka
kafka-console-consumer --bootstrap-server kafka:29092 --topic energy_raw --from-beginning --max-messages 5

# Stopper tous les conteneurs Docker
docker compose down
```


Architecture de Stockage & Data Lakehouse (Membre 3)
Cette section détaille l'infrastructure mise en place pour la persistance, le nettoyage et la transformation des données énergétiques.

1. Architecture Médaillon (Data Lakehouse)
L'implémentation repose sur trois couches logiques pour garantir la qualité des données:


Bronze (/lakehouse/bronze) : Stockage immuable des données brutes extraites de Kafka au format JSON.

Silver (/lakehouse/silver) : Données nettoyées, typées et historisées. Nous utilisons ici le format Delta Lake pour permettre des transactions ACID et une meilleure fiabilité que le simple Parquet.


Gold (/lakehouse/gold) : Agrégations métier prêtes pour l'analyse (ex: daily_stats.parquet). C'est cette couche qui alimente les tableaux de bord du Membre 4.

2. Technologies de Transformation & Stockage

Apache Spark 3.5.1 : Utilisé comme moteur de calcul principal pour les transformations lourdes entre les couches.


Delta Lake : Pour la gestion de la couche Silver, permettant le "time travel" et l'intégrité des données.


TimescaleDB : Base de données relationnelle (PostgreSQL 15) utilisée pour le monitoring en temps réel.


Parquet : Format de stockage colonnaire utilisé dans la couche Gold pour optimiser les requêtes analytiques.

3. Pipeline d'Automatisation (Orchestration)
Un orchestrateur central (orchestrateur.py) pilote l'ensemble du flux de données:

Il surveille l'arrivée des nouveaux fichiers JSON en Bronze.

Il déclenche automatiquement le job Spark job_silver_delta.py pour alimenter la couche Silver.

Il finalise le cycle en mettant à jour les statistiques dans la couche Gold.

Guide d'exécution 
Pour lancer le pipeline de transformation et l'automatisation du Lakehouse, suivez cet ordre précis :
# 1. Se positionner dans le dossier docker
cd docker

# 2. Lancer l'infrastructure
docker compose up -d

# 3. Lancer le Bridge (Capture Kafka -> Bronze JSON)
# Note : On l'exécute DANS le conteneur pour garantir la connexion à TimescaleDB
docker compose exec analytics-engine python bridge_consumer.py

# 4. Lancer l'Orchestrateur (Transformation Bronze -> Silver -> Gold)
# Ouvrez un NOUVEAU terminal et lancez le pilotage Spark :
python orchestrateur.py