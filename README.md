<div align="center">


<img src="logoueve.jpg" alt="Université d'Évry Paris-Saclay" width="280"/>

<br/>

**Université d'Évry Paris-Saclay**  
Master 2 — Innovation, Marché et Science des Données (IMSD)  
Cours : Multicore System Tools

---

# Détection du Greenwashing ESG  
### Data Factory sur Onyxia — Équipe 4 : Scalable Data Ops

*Projet fil rouge — 28 avril → 15 juin 2026*

</div>

---

## Présentation du projet

Ce projet construit une **Data Factory complète** sur la plateforme Onyxia de l'INSEE (SSPCloud) pour détecter les signaux de greenwashing ESG dans les secteurs de l'énergie, des services aux collectivités et de l'industrie.

À partir d'un jeu de données Kaggle couvrant **30 entreprises internationales sur la période 2010-2024**, le pipeline ingère, nettoie, enrichit et modélise les données pour produire un modèle de classification supervisée identifiant les entreprises présentant un risque de greenwashing.

---

## Équipe

| Rôle | Nom | Responsabilité |
|---|---|---|
| Data Architect | Sarangan Uthayan | Infrastructure S3, bootstrap, pipeline de base |
| Data Engineer | Amadou | Nettoyage Bronze → Silver (`src/ingestion/`) |
| Data Engineer | Thu | Feature Engineering — features 1 à 4 (`src/engineering/`) |
| Data Engineer | Romeo Koné | Window Functions, Silver finale, validation (`src/engineering/`) |
| Data Scientist | Yassir | Modélisation ML Spark MLlib (`notebooks/`) |
| Data Analyst | Zouin | Dashboard PowerBI — KPI greenwashing |
| PMO | Xinyi DU | Coordination, planning, jalons |

---

## Source de données

**Dataset Kaggle :** [ESG Greenwashing — Energy & Industrials](https://www.kaggle.com/datasets/alitaqishah/esg-greenwashing-energy-and-industrials)

| Attribut | Valeur |
|---|---|
| Entreprises | 30 (ExxonMobil, Shell, BP, Siemens...) |
| Secteurs | Energy / Utilities / Industrials |
| Période | 2010 → 2024 (15 ans) |
| Observations | 450 lignes × 19 colonnes brutes |
| Cible ML | `greenwashing_flag` — 0 = OK, 1 = greenwashing |
| Déséquilibre | 85,6 % négatif / 14,4 % positif |

---

## Architecture Médaillon

Le pipeline repose sur le **pattern Médaillon**, standard de l'industrie pour les lacs de données distribués.

```
s3a://sarangan/
│
├── bronze/                          # Données brutes Kaggle — lecture seule
│   └── kaggle_esg/
│       └── esg_greenwashing.csv
│
├── silver/                          # Données nettoyées et enrichies — Parquet
│   ├── esg_clean/                   # Silver finale — 450 lignes × 26 colonnes
│   │   └── sector=*/year=*/*.parquet
│   └── esg_features_step1/          # Intermédiaire Thu → Romeo
│       └── sector=*/year=*/*.parquet
│
└── gold/                            # Données prêtes à l'usage
    ├── ml/                          # Prédictions Random Forest
    ├── dashboard/                   # CSV PowerBI + rapport qualité JSON
    └── models/                      # Modèles sérialisés
```

---

## Structure du dépôt

```
esg-greenwashing-scalable-data-ops/
│
├── src/
│   ├── ingestion/
│   │   └── bronze_to_silver.py          # ETL nettoyage — Amadou
│   └── engineering/
│       ├── feature_engineering.py       # Features 1-4 — Thu
│       ├── silver_final.py              # Features 5-6 + validation — Romeo
│       └── export_csv_powerbi.py        # Export CSV pour PowerBI — Romeo
│
├── notebooks/
│   └── 01_ml_greenwashing.py            # Modèle Random Forest — Yassir
│
├── bootstrap.sh                         # Reconstruction complète de l'environnement
├── bronze_to_silver.py                  # Version originale Architecte
├── silver_to_gold.py                    # KPI agrégés dashboard
├── spark_config.py                      # Configuration Spark partagée
└── ONBOARDING.md                        # Guide de démarrage complet
```

> **Convention :** les scripts ETL sont des fichiers `.py` modulaires dans `src/`. Les notebooks sont réservés à l'exploration et au modeling.

---

## Démarrage rapide

### 1. Prérequis

- Compte sur [datalab.sspcloud.fr](https://datalab.sspcloud.fr)
- Token Kaggle (`KGAT_...`) depuis [kaggle.com/settings](https://www.kaggle.com/settings)

### 2. Lancer le service sur Onyxia

```
Catalogue → Jupyter PySpark → Lancer
  Onglet S3       : Activer ✅
  Onglet Resources : 4 CPU / 8 Go RAM
```

### 3. Cloner le dépôt

```bash
cd ~/work
git clone https://github.com/Kone320/esg-greenwashing-scalable-data-ops.git
cd esg-greenwashing-scalable-data-ops
```

### 4. Configurer Kaggle

```bash
mkdir -p ~/.kaggle
echo "VOTRE_TOKEN_KGAT" > ~/.kaggle/access_token
chmod 600 ~/.kaggle/access_token
```

### 5. Lancer le bootstrap

```bash
bash bootstrap.sh
```

### 6. Vérifier l'accès S3

```bash
aws s3 ls s3://sarangan/ \
  --endpoint-url https://minio.lab.sspcloud.fr \
  --no-sign-request
```

---

## Pipeline Data Engineering

### Étape 1 — Nettoyage Bronze → Silver (Amadou)

```bash
python src/ingestion/bronze_to_silver.py
```

Transformations appliquées :

- **Imputation** de `yoy_scope1_change_pct` par la médiane (30 nulls — première année de chaque entreprise)
- **Encodage ordinal** du score CDP : A=8, A-=7, B=6, B-=5, C=4, C-=3, D=2, F=1 (ordre climatique respecté)
- **Encodage binaire** Yes/No → 1/0 pour les 4 colonnes d'engagements (compatibilité Spark MLlib)

### Étape 2 — Feature Engineering (Thu)

```bash
python src/engineering/feature_engineering.py
```

| Feature | Description |
|---|---|
| `commitment_score` | Somme des 4 engagements climatiques déclarés (0 à 4) |
| `esg_cdp_gap` | Écart ESG auto-déclaré vs score CDP externe — signal clé du greenwashing |
| `scope3_share` | Part des émissions scope 3 dans le total — possible sous-déclaration |
| `log_carbon_intensity` | log1p de l'intensité carbone — corrige l'asymétrie de la distribution |

### Étape 3 — Silver finale + Validation (Romeo Koné)

```bash
python src/engineering/silver_final.py
```

| Feature | Description | Complexité |
|---|---|---|
| `credibility_gap` | `commitment_score × (1 - third_party_verified_enc)` | Moyenne |
| `emission_trend_3y` | Moyenne glissante 3 ans via **Window Function Spark** | Élevée |

Produit en sortie :
- Silver finale partitionnée `sector × year` — **450 lignes, 26 colonnes, 0 valeur manquante**
- Rapport qualité JSON → `gold/dashboard/rapport_qualite.json`

---

## Variables de la Silver finale

### Identifiants

| Variable | Type | Description |
|---|---|---|
| `year` | int | Année d'observation (2010-2024) |
| `company` | string | Nom de l'entreprise |
| `ticker` | string | Symbole boursier |
| `sector` | string | Energy / Utilities / Industrials |
| `country` | string | Pays du siège social |
| `revenue_usd_bn` | float | Chiffre d'affaires (milliards USD) |

### Émissions carbone

| Variable | Type | Description |
|---|---|---|
| `scope1_emissions_mt_co2e` | float | Émissions directes (MtCO2e) |
| `scope2_emissions_mt_co2e` | float | Émissions indirectes énergie |
| `scope3_emissions_mt_co2e` | float | Émissions chaîne de valeur |
| `total_s1_s2_mt_co2e` | float | Total scope 1+2 |
| `yoy_scope1_change_pct` | float | Variation annuelle scope 1 (%) |
| `carbon_intensity_tco2e_per_musd` | float | Intensité carbone par M$ de CA |

### Variables ESG encodées

| Variable | Type | Description |
|---|---|---|
| `esg_score_0_100` | float | Score ESG auto-déclaré (0-100) |
| `cdp_score_encoded` | int | Score CDP ordinal : A=8 → F=1 |
| `net_zero_target_set_enc` | int | Engagement Net Zéro : 1=Oui / 0=Non |
| `sbti_committed_enc` | int | Engagement SBTi : 1=Oui / 0=Non |
| `emissions_disclosed_enc` | int | Émissions publiées : 1=Oui / 0=Non |
| `third_party_verified_enc` | int | Vérification tierce : 1=Oui / 0=Non |

### Features dérivées greenwashing

| Variable | Type | Description |
|---|---|---|
| `commitment_score` | float | Nombre d'engagements déclarés (0-4) |
| `esg_cdp_gap` | float | Écart ESG interne vs CDP externe |
| `scope3_share` | float | Part scope 3 dans le total |
| `log_carbon_intensity` | float | Log de l'intensité carbone |
| `credibility_gap` | float | Engagements sans vérification tierce |
| `emission_trend_3y` | float | Tendance émissions sur 3 ans glissants |

### Variable cible

| Variable | Type | Description |
|---|---|---|
| `greenwashing_flag` | int | **0 = pas de greenwashing \| 1 = greenwashing détecté** |

---

## Notes pour le Data Scientist

```
Chemin Silver finale : s3a://sarangan/silver/esg_clean/
Cible               : greenwashing_flag
Déséquilibre        : 85,6 % / 14,4 % → utiliser weightCol dans Spark MLlib
Split temporel      : train ≤ 2020  |  test > 2020  (ne pas effectuer de random split)
Métriques           : AUC-ROC et F1-score — ne pas utiliser l'accuracy
Rapport qualité     : s3a://sarangan/gold/dashboard/rapport_qualite.json
```

---

## Workflow Git

Chaque ingénieur travaille sur une branche dédiée :

```
main
├── feature/engineer-amadou-ingestion       # Amadou
├── feature/engineer-thu-features           # Thu
├── feature/engineer-romeo-silver-final     # Romeo
├── feature/scientist-ml-greenwashing       # Yassir
└── feature/analyst-powerbi-dashboard       # Zouin
```

**Convention de commits :** [Conventional Commits](https://www.conventionalcommits.org/)  
`feat:`, `fix:`, `refactor:`, `docs:`

---

## Conventions du projet

| Élément | Convention |
|---|---|
| Colonnes | `snake_case` |
| Fichiers Python | `snake_case.py` |
| Branches Git | `feature/<role>-<sujet>` |
| Format Silver/Gold | Parquet partitionné |
| Accès S3 | Anonyme (`AnonymousAWSCredentialsProvider`) |

---
## Power BI Dashboard

The final Power BI dashboard developed during the Data Analytics phase is available in:

- `ESG Greenwashing dashboard.pbix`

### Dashboard Structure

The dashboard follows a three-page storytelling approach:

#### Executive Overview
**Question:** What is happening?

- Total Companies
- Total Observations
- Greenwashing Rate
- Greenwashing Exposure by Sector
- Greenwashing Trend Over Time

#### Greenwashing Analysis
**Question:** Why is it happening?

- ESG Score vs CDP Score
- Companies by Credibility Gap
- Average Emission Trend by Commitment Score
- Average Carbon Intensity by Sector

#### Predictive Analytics
**Question:** What should we do?

- High-Risk Cases
- Highest Risk Score
- Average Risk Score
- Top High-Risk Companies
- Top Model Drivers
- Average Predicted Greenwashing Risk by Sector
---
## Contact

- **Architecture / S3 / infrastructure** : Sarangan Uthayan
- **Pipeline Data Engineering** : Romeo Koné
- **Coordination / planning** : Xinyi DU (PMO)

---

<div align="center">

*Équipe 4 — Scalable Data Ops · M2 IMSD · Université d'Évry Paris-Saclay · 2026*

</div>
