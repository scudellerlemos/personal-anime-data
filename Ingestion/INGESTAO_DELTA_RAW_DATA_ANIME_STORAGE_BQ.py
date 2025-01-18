from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max
from google.cloud import storage
import os
from datetime import datetime

# Configurações do GCS e BigQuery
gcs_bucket_name = "NOME_BUCKET_GCP"
bq_project_id = "personal-anime-data-2024"
bq_dataset_id = "RAW_DATA"
bq_table_id = "RAW_ANIMELIST"
service_account_json = "#ACCOUNT SERVICE#"

# Define a variável de ambiente para a conta de serviço
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_json

# Inicializa a SparkSession
spark = SparkSession.builder \
    .appName("GCS to BigQuery Delta") \
    .getOrCreate()

# Inicializa o cliente do GCS
gcs_client = storage.Client()

# Encontra o arquivo mais recente no GCS
def get_latest_file(bucket_name):
    bucket = gcs_client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs())

    # Filtrar arquivos no formato "Anime_list_<Ano>_<Mês>.csv"
    files = [
        blob.name for blob in blobs if "Anime_list_" in blob.name and blob.name.endswith(".csv")
    ]
    if not files:
        raise FileNotFoundError("Nenhum arquivo válido encontrado no bucket.")

    # Ordenar pelos nomes dos arquivos para encontrar o mais recente
    files_sorted = sorted(
        files,
        key=lambda x: (int(x.split("_")[2]), int(x.split("_")[3][:2])),  # Ano, Mês
        reverse=True,
    )
    return files_sorted[0]

# Lê o arquivo mais recente do GCS como DataFrame do PySpark
def load_file_from_gcs(bucket_name, file_name):
    file_path = f"gs://{bucket_name}/{file_name}"
    df = spark.read.option("header", True).csv(file_path)
    return df

# Carrega os dados existentes do BigQuery como DataFrame do PySpark
def load_bq_table(project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    df_bq = spark.read \
        .format("bigquery") \
        .option("table", table_ref) \
        .load()
    return df_bq

# Insere novos registros no BigQuery
def insert_into_bq(project_id, dataset_id, table_id, df):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    df.write \
        .format("bigquery") \
        .option("table", table_ref) \
        .mode("append") \
        .save()
    print(f"{df.count()} registros inseridos na tabela {table_ref}.")

# Remove registros antigos da tabela RAW do BigQuery
def clean_bq_table(project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    query = f"""
    DELETE FROM `{table_ref}`
    WHERE SEASON_YEAR < (
        SELECT MAX(SEASON_YEAR) - 2 FROM `{table_ref}`
    )
    """
    spark.sql(query)
    print(f"Registros antigos removidos da tabela {table_ref}.")

# Processo principal
# Passo 1: Encontra o arquivo mais recente no GCS
latest_file = get_latest_file(gcs_bucket_name)
print(f"Arquivo mais recente: {latest_file}")

# Passo 2: Carrega os dados do arquivo GCS
df_gcs = load_file_from_gcs(gcs_bucket_name, latest_file)

# Passo 3: Carrega os dados existentes do BigQuery
df_bq = load_bq_table(bq_project_id, bq_dataset_id, bq_table_id)

# Passo 4: Identifica novos registros com base em TITLE_ENGLISH
new_records = df_gcs.join(df_bq, df_gcs["TITLE_ENGLISH"] == df_bq["TITLE_ENGLISH"], "left_anti")

if new_records.count() > 0:
    # Passo 5: Insere novos registros no BigQuery
    insert_into_bq(bq_project_id, bq_dataset_id, bq_table_id, new_records)

# Passo 6: Remove registros antigos da tabela RAW do BigQuery
clean_bq_table(bq_project_id, bq_dataset_id, bq_table_id)

    
