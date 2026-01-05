### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep
from datetime import datetime

### Importando scripts necessários
from politica_e_modelagem.criacao_propostas_automaticas.scripts.propostas_v3 import exporta_csv_politica_v3
from politica_e_modelagem.criacao_propostas_automaticas.scripts.propostas_v4 import exporta_csv_politica_v4


### Parâmetros de acesso
access_params = {          
    "endpoint_url_trusted": Variable.get("MINIO_TRUSTED_ENDPOINT"),
    "aws_access_key_id_trusted": Variable.get("MINIO_TRUSTED_ACCESS_KEY"),
    "aws_secret_access_key_trusted": Variable.get("MINIO_TRUSTED_SECRET_KEY"),
    "endpoint_url_refined": Variable.get("MINIO_REFINED_ENDPOINT"),
    "aws_access_key_id_refined": Variable.get("MINIO_REFINED_ACCESS_KEY"),
    "aws_secret_access_key_refined": Variable.get("MINIO_REFINED_SECRET_KEY"),
    "endpoint_url_raw": Variable.get("MINIO_RAW_ENDPOINT"),
    "aws_access_key_id_raw": Variable.get("MINIO_RAW_ACCESS_KEY"),
    "aws_secret_access_key_raw": Variable.get("MINIO_RAW_SECRET_KEY"),
    "trino_endpoint": Variable.get("TRINO_ENDPOINT"),
    "trino_port": Variable.get("TRINO_PORT"),
    "trino_user": Variable.get("TRINO_USER"),
    "trino_password": Variable.get("TRINO_PASSWORD"),
    "opdb_bucket": Variable.get("OPDB_BUCKET"),
    "stage": Variable.get('STAGE'),
    "kafka_url": Variable.get('KAFKA_DATALAKE_ENDPOINT'),
    "exrep_url": Variable.get('EXREP_BASE_URL'),
    "exrep_client_id": Variable.get('EXREP_CLIENT_ID'),
    "exrep_client_secret": Variable.get('EXREP_CLIENT_SECRET'),
    "keycloack_token_url": Variable.get('KEYCLOAK_TOKEN_URL')
    }


# Definindo defaults
default_args = {
    "owner": "Vinicius Moraes",
    "retries": 0,
}


# Definindo a DAG
with DAG(
    dag_id='criacao_propostas_automaticas',
    schedule_interval='50 8 * * 1',  # Rodar todas as segundas-feiras às 06:50
    default_args=default_args,
    tags=['propostas', 'automaticas'] # DAG só será acionada manualmente pela API
) as dag:

    # Filtra casos que serão vencidos e exporta para o devido bucket
    task1 = PythonOperator(
        task_id = 'propostas_v3',
        python_callable = exporta_csv_politica_v3,
        provide_context = True  # Habilita o envio do contexto (incluindo conf)
    )

    # Captura proposta no jira
    task2 = PythonOperator(
        task_id = "propostas_v4",
        python_callable = exporta_csv_politica_v4,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Definindo a ordem de execução das tasks
    task1 >> task2