from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# URLs das consultas SQL no GitHub
TRU_QUERY_URL = "https://raw.githubusercontent.com/scudellerlemos/personal-anime-data/main/BigQuery/queries/TRU_Anime_Table.sql"
REF_QUERY_URL = "https://raw.githubusercontent.com/scudellerlemos/personal-anime-data/main/BigQuery/queries/REF_Anime_Table.sql"

# Função para buscar o conteúdo SQL do GitHub
def fetch_sql_from_github(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Erro ao acessar o arquivo: {url}, Status: {response.status_code}")

# Função Python para baixar e salvar as consultas no contexto da DAG
def get_queries(**kwargs):
    tru_query = fetch_sql_from_github(TRU_QUERY_URL)
    ref_query = fetch_sql_from_github(REF_QUERY_URL)
    
    # Salvando as consultas no contexto da DAG para serem reutilizadas
    kwargs['ti'].xcom_push(key='tru_query', value=tru_query)
    kwargs['ti'].xcom_push(key='ref_query', value=ref_query)

# Definição da DAG
with DAG(
    dag_id='pipeline-full-animelist',
    default_args=default_args,
    description='DAG para executar SQL no BigQuery',
    schedule_interval='30 22 * * *',  # Todos os dias às 22:30
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bigquery', 'composer', 'sql'],
) as dag:

    # Tarefa 1: Baixar consultas SQL do GitHub
    fetch_queries_task = PythonOperator(
        task_id='fetch_queries',
        python_callable=get_queries,
        provide_context=True,
    )

    # Tarefa 2: Executar a consulta TRU_Anime_Table.sql no BigQuery
    execute_tru_query_task = BigQueryInsertJobOperator(
        task_id='execute_tru_query',
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='fetch_queries', key='tru_query') }}",
                "useLegacySql": False,
            }
        },
    )

    # Tarefa 3: Executar a consulta REF_Anime_Table.sql no BigQuery
    execute_ref_query_task = BigQueryInsertJobOperator(
        task_id='execute_ref_query',
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='fetch_queries', key='ref_query') }}",
                "useLegacySql": False,
            }
        },
    )

    # Ordem das tarefas
    fetch_queries_task >> execute_tru_query_task >> execute_ref_query_task
