import requests
import pandas as pd
from google.cloud import storage
import os



# Variaveis
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
          relations {
            edges {
              node {
                title {
                  romaji
                  english
                }
                type
                siteUrl
                coverImage {
                  large
                }
              }
              relationType
            }
          }
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

    # Processa os dados em um DataFrame
    anime_data = []
    for entry in entries:
        media = entry['media']
        anime_data.append({
            'Title (Romaji)': media['title']['romaji'],
            'Title (English)': media['title']['english'],
            'Episodes': media['episodes'],
            'Cover Image': media['coverImage']['large'] if media['coverImage'] else None,
            'Average Score': media['averageScore'],
            'Genres': ', '.join(media['genres']) if media['genres'] else None,
            'Start Year': media['startDate']['year'],
            'Studio': media['studios']['nodes'][0]['name'] if media['studios']['nodes'] else None,
            'Description': media['description'],
            'Popularity': media['popularity'],
            'Season': media['season'],
            'Season Year': media['seasonYear'],
            'Duration (min)': media['duration'],
            'Trending': media['trending'],
            'Source': media['source'],
            'Favourites': media['favourites'],
            'Completed At': f"{entry['completedAt']['year']}-{entry['completedAt']['month']}-{entry['completedAt']['day']}" if entry['completedAt'] else None,
            'Score': entry['score']
        })

    # Cria o DataFrame
    df = pd.DataFrame(anime_data)
    df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True)

    # Salva o DataFrame como um arquivo CSV

    df.to_csv(local_file, index=False)

    print(f"Arquivo {local_file} criado com sucesso.")

    # Nome do bucket destino
    
    destination_blob_name = local_file

    # Inicializa o cliente do Google Cloud Storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Faz o upload do arquivo para o bucket
    blob.upload_from_filename(local_file)

    print(f"Arquivo {local_file} enviado para o bucket {bucket_name} como {destination_blob_name}.")
    os.remove(local_file)
else:
    print(f"Erro na requisição: {response.status_code}")
     