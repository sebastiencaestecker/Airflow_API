# Pipeline ETL pour Citymapper avec Apache Airflow

**Automatisation de l'acquisition et du traitement des données de location de vélos et météorologiques à Paris**

---

## 🏙️ **Contexte**

Vous êtes ingénieur de données chez Citymapper, une application qui aide les utilisateurs à planifier leurs déplacements dans les grandes villes du monde. Vous travaillez avec l'équipe parisienne (basée à Londres) pour construire un pipeline ETL qui extrait les données de l'API et les charge dans une base de données Postgres. Ce pipeline permettra aux analystes de données d'analyser le trafic de location de vélos à Paris en tenant compte des conditions météorologiques.

---

## 🎯 **Objectif**

Construire un pipeline ETL avec Apache Airflow pour :

- **Extraire les données** : Récupérer les données météorologiques et l'état des stations de vélos à partir des API.
- **Transformer les données** : Convertir les données JSON en fichiers CSV adaptés à la base de données Postgres.
- **Charger les données** : Insérer les données transformées dans la base de données Postgres toutes les heures.

---

## 🛠️ **Technologies Utilisées**

- **Apache Airflow** : Pour l'orchestration des workflows et l'automatisation des pipelines de données.
- **Python** : Pour le développement des scripts de traitement des données.
- **Pandas** : Pour la manipulation et l'analyse des données.
- **PostgreSQL** : Pour le stockage des données transformées.
- **AWS S3** : Pour le stockage intermédiaire des fichiers de données.
- **WeatherBit API** : Pour l'acquisition des données météorologiques.

---

## 📊 **Pipeline de Données**

Le pipeline ETL comprendra deux branches principales :

1. **Branche Météo** :
   - Récupérer les données météorologiques de Paris via l'API WeatherBit.
   - Transformer les données JSON en fichiers CSV.
   - Charger les données dans une table Postgres nommée `weather_data`.

2. **Branche État des Stations** :
   - Récupérer les données d'état des stations de vélos via l'API.
   - Transformer les données JSON en fichiers CSV.
   - Charger les données dans une table Postgres nommée `station_status`.

---
![image](https://github.com/user-attachments/assets/384e9027-f50f-4c12-8c0d-f1374e6eef24)


