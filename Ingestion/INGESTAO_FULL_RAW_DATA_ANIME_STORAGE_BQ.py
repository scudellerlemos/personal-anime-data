import gcsfs
import pandas as pd
from google.cloud import storage, bigquery
from pandas_gbq import to_gbq
import os
import chardet  # Importando o chardet para detecção de encoding

# Variáveis de Conta de Serviço e Bigquery
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

# Função para detectar o encoding do conteúdo de um arquivo em fluxo
def detect_encoding_from_stream(file_stream):
    raw_data = file_stream.read(10000)  # Ler uma amostra do arquivo
    result = chardet.detect(raw_data)
    return result['encoding']

# Carregar o arquivo diretamente do GCS em um DataFrame
print(f"Lendo o arquivo de {gcs_path}...")

fs = gcsfs.GCSFileSystem(project=project_id)  # Conecta ao GCS
with fs.open(gcs_path, "rb") as f:
    # Detectar o encoding a partir do fluxo de dados do arquivo no GCS
    encoding = detect_encoding_from_stream(f)
    print(f"Encoding detectado: {encoding}")

    # Reposiciona o ponteiro do arquivo para o início após a leitura da amostra
    f.seek(0)

    # Agora, podemos carregar o arquivo com o encoding detectado
    df = pd.read_csv(f, encoding=encoding)

# Carregar o DataFrame no BigQuery
print(f"Carregando dados na tabela {table_id} no BigQuery...")
to_gbq(
    dataframe=df,
    destination_table=table_id,
    project_id=project_id,
    if_exists="replace",  # Cria a tabela ou substitui se ela já existir
)

print(f"Dados carregados com sucesso na tabela {table_id}!")
