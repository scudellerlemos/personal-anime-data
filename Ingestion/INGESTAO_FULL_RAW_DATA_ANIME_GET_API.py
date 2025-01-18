from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import os
from google.cloud import storage

# Inicializa a SparkSession
spark = SparkSession.builder.appName("AniList Data Processing").getOrCreate()

# Variáveis de Conta de Serviço e Storage
source_path = '#ACCOUNT SERVICE#'
local_file = '#FILE_NAME.csv#'
bucket_name = '#NIME_BUCKET_GCP#'
service_account_json = source_path

# Define a variável de ambiente para a conta de serviço
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_json

# URL da API da AniList para buscar a lista de animes assistidos de um usuário
url = 'https://graphql.anilist.co'

query = '''
query ($username: String) {
  MediaListCollection(userName: $username, type: ANIME, status: COMPLETED) {
    lists {
      entries {
        media {
          title {
            romaji
            english
          }
          episodes
          coverImage {
            large
          }
          averageScore
          genres
          startDate {
            year
          }
          studios(isMain: true) {
            nodes {
              name
            }
          }
          description
          popularity
          season
          seasonYear
          duration
          trending
          source
          favourites
        }
        completedAt {
          year
          month
          day
        }
        score
      }
    }
  }
}
'''

# Variáveis da consulta, incluindo o nome de usuário
variables = {
    'username': 'Gajs'
}

# Faz a requisição para a API da AniList
response = requests.post(url, json={'query': query, 'variables': variables})

# Verifica se a requisição foi bem-sucedida
if response.status_code == 200:
    data = response.json()
    entries = data['data']['MediaListCollection']['lists'][0]['entries']

    # Processa os dados em uma lista de dicionários
    anime_data = []
    for entry in entries:
        media = entry['media']
        anime_data.append({
            'TITLE_ROMAJI': media['title']['romaji'],
            'TITLE_ENGLISH': media['title']['english'],
            'EPISODES': media['episodes'],
            'COVER_IMAGE': media['coverImage']['large'] if media['coverImage'] else None,
            'AVERAGE_SCORE': media['averageScore'],
            'GENRES': ', '.join(media['genres']) if media['genres'] else None,
            'START_YEAR': media['startDate']['year'],
            'STUDIO': media['studios']['nodes'][0]['name'] if media['studios']['nodes'] else None,
            'DESCRIPTION': media['description'],
            'POPULARITY': media['popularity'],
            'SEASON': media['season'],
            'SEASON_YEAR': media['seasonYear'],
            'DURATION_MIN': media['duration'],
            'TRENDING': media['trending'],
            'SOURCE': media['source'],
            'FAVOURITES': media['favourites'],
            'COMPLETED_AT': f"{entry['completedAt']['year']}-{entry['completedAt']['month']}-{entry['completedAt']['day']}" if entry['completedAt'] else None,
            'SCORE': entry['score']
        })

    # Cria o DataFrame do PySpark
    schema = StructType([
        StructField('TITLE_ROMAJI', StringType(), True),
        StructField('TITLE_ENGLISH', StringType(), True),
        StructField('EPISODES', IntegerType(), True),
        StructField('COVER_IMAGE', StringType(), True),
        StructField('AVERAGE_SCORE', IntegerType(), True),
        StructField('GENRES', StringType(), True),
        StructField('START_YEAR', IntegerType(), True),
        StructField('STUDIO', StringType(), True),
        StructField('DESCRIPTION', StringType(), True),
        StructField('POPULARITY', IntegerType(), True),
        StructField('SEASON', StringType(), True),
        StructField('SEASON_YEAR', IntegerType(), True),
        StructField('DURATION_MIN', IntegerType(), True),
        StructField('TRENDING', IntegerType(), True),
        StructField('SOURCE', StringType(), True),
        StructField('FAVOURITES', IntegerType(), True),
        StructField('COMPLETED_AT', StringType(), True),
        StructField('SCORE', IntegerType(), True)
    ])

    df = spark.createDataFrame(anime_data, schema=schema)

    # Salva o DataFrame como um arquivo CSV no local especificado
    temp_path = f"/dbfs/tmp/{local_file}"
    df.write.csv(temp_path, header=True, mode='overwrite')

    print(f"Arquivo {local_file} criado com sucesso no Databricks.")

    # Inicializa o cliente do Google Cloud Storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(local_file)  # Mesma chave de arquivo para habilitar versionamento

    # Faz o upload do arquivo para o bucket
    blob.upload_from_filename(temp_path)

    print(f"Arquivo {local_file} enviado para o bucket {bucket_name}. Versionamento ativo.")

else:
    print(f"Erro na requisição: {response.status_code}")
