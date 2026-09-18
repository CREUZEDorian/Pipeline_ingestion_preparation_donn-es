# Pipeline_ingestion_preparation_donn-es
contexte: ingérer, cataloguer, transformer et valider des données avant modélisation. Projet réalisé en préparation à une certification AWS

Architecture:
Dans le dossier data se trouve 2 fichier CSV le "WA_Fn-UseC_-Telco-Customer-Churn.csv" est le jeu de données de base et "telco_clean (5).csv" est le jeu de données nettoyé.

le notebook "cleaning_data_from_s3.ipynb" est utilisé pour prendre les données depuis le S3 dans lequel elles se trouvent. Nettoyer les données, changer le type de certains objets pour que les données soient utilisable pour entrainement de modèles plus tard (les objets passent au numérique, les catégories sont encodés...) et sont réécris dans un nouveau csv.

Le script "insert_into_dynamo_db.py" est un script qui vient prend des données du csv nettoyés et les insérer dans un base de données dynamo db pour être utilisé plus tard.

Le script "process_telco.py" est un script qui vient prendre les données du csv les nettoyés et les réécrire dans un autre csv du s3. Comparé au "cleaning_data_from_s3.ipynb" ce script vient utilisé aws glue pour lancer des jobs ETL pour faire le nettoyage.


Fonctionnement:
"cleaning_data_from_s3.ipynb" et "insert_into_dynamo_db.py" peuvent fonctionner depuis un ordinateur en local tant que aws cli est installé https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
"process_telco.py" ne peut pas être utilisé en local, il faut l'importer dans aws glue ETL jobs.