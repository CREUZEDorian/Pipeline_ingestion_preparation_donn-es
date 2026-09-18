import boto3 
import awswrangler as wr
import pandas as pd

"s3://telco-processed-611284995507-eu-north-1-an/processed/"

# --- Configuration ---
BUCKET = "telco-processed-611284995507-eu-north-1-an"
KEY_CLEAN_CSV = "processed/telco_filtered.csv"  
TABLE_NAME = "telco-features"
REGION = "eu-north-1" 

# --- 1. Lecture du CSV nettoyé depuis S3 ---
path_clean = f"s3://{BUCKET}/{KEY_CLEAN_CSV}"
df = wr.s3.read_csv(path_clean)
print(f"Lignes chargées depuis S3 : {df.shape[0]}, colonnes : {df.shape[1]}")

if "customerID" not in df.columns:
    raise ValueError("La colonne 'customerID' est absente du fichier — corrige le notebook 1.3 avant de continuer.")

# --- 2. Création de la table DynamoDB (si elle n'existe pas déjà) ---
dynamodb = boto3.resource("dynamodb", region_name=REGION)
client = boto3.client("dynamodb", region_name=REGION)

existing_tables = client.list_tables()["TableNames"]

table = dynamodb.Table(TABLE_NAME)
print(f"Table '{TABLE_NAME}'")


# --- 3. Insertion des lignes via batch_writer ---
# DynamoDB n'accepte pas les NaN/float natifs Python de la même façon que pandas :
# on convertit les valeurs en types compatibles (str pour l'ID, float/int/Decimal pour le reste)
from decimal import Decimal

def clean_item(row):
    item = {}
    for col, val in row.items():
        if pd.isna(val):
            continue  # on n'insère pas les valeurs manquantes (ne devrait plus arriver après l'étape 1.3)
        if isinstance(val, (int,)):
            item[col] = val
        elif isinstance(val, float):
            item[col] = Decimal(str(val))  # DynamoDB exige Decimal pour les floats, pas de float natif
        else:
            item[col] = str(val)
    return item

with table.batch_writer() as batch:
    for _, row in df.iterrows():
        item = clean_item(row.to_dict())
        batch.put_item(Item=item)

print(f"Insertion terminée : {len(df)} items écrits dans '{TABLE_NAME}'.")

# --- 4. Vérification avec get_item sur un customerID connu ---
exemple_id = df["customerID"].iloc[0]
response = table.get_item(Key={"customerID": exemple_id})

if "Item" in response:
    print(f"\nVérification réussie pour customerID={exemple_id} :")
    print(response["Item"])
else:
    print(f"Aucun item trouvé pour customerID={exemple_id} — vérifie l'insertion.")