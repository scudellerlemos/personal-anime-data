from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google.cloud import storage
import os

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
    .appName("GCS to BigQuery - Update RAW") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", service_account_json) \
    .getOrCreate()

# Inicializa o cliente do GCS
gcs_client = storage.Client()

# Função para encontrar o arquivo mais recente no GCS
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

# Função para carregar o arquivo mais recente do GCS como DataFrame do PySpark
def load_file_from_gcs(bucket_name, file_name):
    file_path = f"gs://{bucket_name}/{file_name}"
    df = spark.read.option("header", True).csv(file_path)
    print(f"Dados carregados do arquivo: {file_name}")
    return df

# Função para sobrescrever a tabela RAW do BigQuery
def overwrite_bq_table(project_id, dataset_id, table_id, df):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    df.write \
        .format("bigquery") \
        .option("table", table_ref) \
        .mode("overwrite") \
        .save()
    print(f"Tabela {table_ref} atualizada com os dados mais recentes.")

# Processo principal

# Passo 1: Encontra o arquivo mais recente no GCS
latest_file = get_latest_file(gcs_bucket_name)
print(f"Arquivo mais recente encontrado: {latest_file}")

# Passo 2: Carrega os dados do arquivo GCS
df_gcs = load_file_from_gcs(gcs_bucket_name, latest_file)

# Passo 3: Sobrescreve a tabela RAW no BigQuery
overwrite_bq_table(bq_project_id, bq_dataset_id, bq_table_id, df_gcs)

