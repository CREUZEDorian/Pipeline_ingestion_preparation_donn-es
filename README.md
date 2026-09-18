# Pipeline_ingestion_preparation_donn-es AWS
contexte: Projet réalisé en préparation à une certification AWS

Objectif : ingérer, cataloguer, transformer et valider un jeu de données avant modélisation, en pratiquant les services AWS correspondants (S3, Glue Crawler, Athena, Glue ETL, DynamoDB), avec deux approches comparées pour la partie nettoyage (notebook pandas/scikit-learn vs job Glue ETL managé).

## Dataset:

Telco Customer Churn (IBM, via Kaggle) — 7 043 clients, 21 colonnes, tâche de classification binaire (churn oui/non). Choisi car il contient des valeurs manquantes cachées (TotalCharges) et des variables catégorielles variées, utile pour pratiquer l'imputation et l'encodage.

## Contenu du repo

| Fichier | Rôle |
|---|---|
| `data/WA_Fn-UseC_-Telco-Customer-Churn.csv` | Jeu de données brut (source Kaggle) |
| `data/telco_clean.csv` | Jeu de données nettoyé et encodé, prêt pour l'entraînement |
| `cleaning_data_from_s3.ipynb` | Lecture depuis S3, nettoyage, imputation, encodage — approche notebook |
| `process_telco.py` | Job **AWS Glue ETL** (PySpark) : filtrage des données via Glue managé |
| `insert_into_dynamo_db.py` | Insertion du CSV nettoyé dans une table DynamoDB (feature store léger) |
| `requirements.txt` | Dépendances Python |

## Fonctionnement:
"cleaning_data_from_s3.ipynb" et "insert_into_dynamo_db.py" peuvent fonctionner depuis un ordinateur en local tant que aws cli est installé https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
"process_telco.py" ne peut pas être utilisé en local, il faut l'importer dans aws glue ETL jobs.

## Étapes réalisées

### 1. Ingestion et catalogage
- Upload du CSV brut dans un bucket S3 (`raw/`).
- Création d'un **Glue Crawler** qui scanne ce bucket et génère automatiquement le schéma dans le **Glue Data Catalog** (base `telco_db`, table `telco_raw`).
- Vérification via une requête **Athena** :
  ```sql
  SELECT * FROM telco_db.telco_raw LIMIT 10;
  ```

### 2. Nettoyage et préparation (`cleaning_data_from_s3.ipynb`)
- Lecture du CSV depuis S3 via `awswrangler`.
- Conversion de `TotalCharges` en numérique, révélant 11 valeurs manquantes cachées.
- Comparaison de deux stratégies d'imputation : `SimpleImputer` (médiane) vs `KNNImputer` (n_neighbors=5) — la seconde retenue comme version finale.
- Encodage des variables catégorielles avec `OneHotEncoder` (`drop="first"`, `dtype=int`) plutôt que `pd.get_dummies`, pour garantir un typage `int` cohérent (et non `bool`).
- Écriture du résultat dans s3.

### 3. Job Glue ETL managé (`process_telco.py`)
- Lecture directe du CSV nettoyé depuis S3 (`create_dynamic_frame.from_options`).
- Filtrage via requête Spark SQL (`WHERE tenure != 0`).
- `coalesce(1)` pour forcer un seul fichier de sortie, puis renommage automatique du fichier (`part-*` → `telco_filtered.csv`) via `boto3`, car Glue ne conserve pas l'extension `.csv` par défaut.
- Écriture dans s3.

### 4. Feature store léger (`insert_into_dynamo_db.py`)
- Lecture du CSV nettoyé depuis S3.
- Création d'une table DynamoDB `telco-features`, clé de partition `customerID`.
- Insertion de chaque ligne via `batch_writer()`, avec conversion des `float` en `Decimal`.
- Vérification finale via `get_item()` sur un `customerID`.

## Reproduire ce projet

### Prérequis
- Un compte AWS avec un utilisateur IAM dédié (permissions limitées à S3, Glue, DynamoDB — pas le compte root).
- AWS CLI installé et configuré (`aws configure`).
- Python 3.10+ et les dépendances : `pip install -r requirements.txt`.

### Étapes
1. Créer 2 buckets S3 (`<prefix>-raw`, `<prefix>-processed`), uploader le CSV brut dans `raw/`.
2. Créer et lancer un Glue Crawler sur `raw/` (base `telco_db`).
3. Exécuter `cleaning_data_from_s3.ipynb` (adapter les chemins S3 en haut du notebook).
4. Déployer `process_telco.py` comme job Glue ETL (console ou `aws glue create-job`), puis lancer avec :
   ```bash
   aws glue start-job-run --job-name <nom-du-job>
   ```
5. Exécuter `insert_into_dynamo_db.py` en local .

## Difficultés rencontrées et solutions

- **`pd.get_dummies` retournait des `True`/`False` au lieu de `0`/`1`** dans les versions récentes de pandas → remplacé par `OneHotEncoder(dtype=int)`.
- **Les fichiers écrits par Glue via `write_dynamic_frame` n'ont pas d'extension `.csv`**  ajout d'un `coalesce(1)` + renommage `boto3` post-écriture (`copy_object` + `delete_object`).
- **DynamoDB refuse les `float` Python natifs** → conversion explicite en `Decimal` avant insertion via `batch_writer()`.
