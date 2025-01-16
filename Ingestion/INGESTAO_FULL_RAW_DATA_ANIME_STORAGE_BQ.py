from pyspark.sql import SparkSession
from google.cloud import storage
import os
import chardet
import pandas as pd  # Para detectar encoding

# Variáveis de Conta de Serviço e BigQuery
source_path = '#ACCOUNT SERVICE#'
bucket_name = '#NIME_BUCKET_GCP#'  # Nome do bucket GCS
file_name = '#FILE_NAME.csv#'  # Nome do arquivo no bucket
gcs_path = f"gs://{bucket_name}/{file_name}"  # Caminho completo no GCS
project_id = "#PROJECT_ID_BQ#"  # ID do projeto GCP
table_id = "#NME_DATASET.RAW_NME_TABELA#"

# Caminho do arquivo JSON da conta de serviço
service_account_json = source_path
# Define a variável de ambiente para a conta de serviço
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_json

# Inicializa a SparkSession
spark = SparkSession.builder.appName("GCS to BigQuery").getOrCreate()

# Função para detectar o encoding do conteúdo de um arquivo em fluxo
def detect_encoding_from_stream(file_stream):
    raw_data = file_stream.read(10000)  # Ler uma amostra do arquivo
    result = chardet.detect(raw_data)
    return result['encoding']

# Lê o arquivo do GCS
print(f"Lendo o arquivo de {gcs_path}...")

# Inicializa o cliente do Google Cloud Storage
client = storage.Client()
bucket = client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Baixa o arquivo para detectar o encoding
with blob.open("rb") as f:
    encoding = detect_encoding_from_stream(f)
    print(f"Encoding detectado: {encoding}")

# Recarrega o arquivo com o encoding detectado para um DataFrame PySpark
with blob.open("r", encoding=encoding) as f:
    pdf = pd.read_csv(f)  # Lê o arquivo em um DataFrame Pandas para conversão

df = spark.createDataFrame(pdf)  # Converte para DataFrame PySpark

# Salva os dados no BigQuery
print(f"Carregando dados na tabela {table_id} no BigQuery...")
df.write \
    .format("bigquery") \
    .option("table", table_id) \
    .option("temporaryGcsBucket", bucket_name) \
    .mode("overwrite") \
    .save()

print(f"Dados carregados com sucesso na tabela {table_id}!")
