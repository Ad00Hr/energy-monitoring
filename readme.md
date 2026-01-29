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
