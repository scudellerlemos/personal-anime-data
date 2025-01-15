import gcsfs
import pandas as pd
from google.cloud import storage, bigquery
from pandas_gbq import to_gbq
import os

# Variaveis de Conta de Serviço e Bigquery
source_path = '#ACCOUNT SERVICE#'
project_id = "#PROJECT_NAME_ID#"  # ID do projeto GCP
bucket_name = "#BUCKET_NAME#"  # Nome do bucket GCS
file_name = "#FILE_NAME.csv#"  # Nome do arquivo no bucket
gcs_path = f"gs://{bucket_name}/{file_name}"  # Caminho completo no GCS
table_id = "#NOME_DATASET#.#NOME_TABELA_RAW#" 

# Caminho do arquivo JSON da conta de serviço
service_account_json = source_path
# Define a variável de ambiente para a conta de serviço
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_json



# Carregar o arquivo diretamente do GCS em um DataFrame
print(f"Lendo o arquivo de {gcs_path}...")
fs = gcsfs.GCSFileSystem(project=project_id)  # Conecta ao GCS
with fs.open(gcs_path, "r") as f:
    df = pd.read_csv(f)

# Carregar o DataFrame no BigQuery
print(f"Carregando dados na tabela {table_id} no BigQuery...")
to_gbq(
    dataframe=df,
    destination_table=table_id,
    project_id=project_id,
    if_exists="replace",  # Cria a tabela ou substitui se ela já existir
)

print(f"Dados carregados com sucesso na tabela {table_id}!") 
 
