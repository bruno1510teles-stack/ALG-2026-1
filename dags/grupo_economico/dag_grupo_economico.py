### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep
import requests


### Importando scripts necessários
#from grupo_economico.cria_grupo_economico_autom_historico import cria_grupo_economico_automatico_historico
from grupo_economico.cria_grupo_economico_autom_rotina import cria_grupo_economico_automatico_rotina


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


def notificar_falha_teams(context):
    url = "https://yandehbr.webhook.office.com/webhookb2/3efc9ab8-aba8-4150-8e68-864d086592a3@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/2bb511bca72643d58ea858c433be3aec/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2AAjaUAPO15qUofSpSzGh6PW4gkg2FJypyvorUwW89eU1"
    mensagem = {
        "title": f"Falha na Execução DAG - {context['task_instance'].dag_id}",
        "text": f"Falha na DAG: {context['task_instance'].dag_id} na task: {context['task_instance'].task_id} VERIFICAR URGENTE!!"
    }
    requests.post(url, json=mensagem)


### Definindo defaults
default_args = {
    "owner": "Vinicius Moraes",
    "retries": 0,
    "on_failure_callback": notificar_falha_teams,
}


# Definindo a DAG
with DAG(
    dag_id='grupo_economico_automatico',
    start_date=days_ago(1),
    schedule_interval='30 * * * *',
    default_args=default_args,
    catchup=False, 
    tags=['grupo', 'economico', 'automatico'],
    max_active_runs=1
) as dag:

    '''
    # Definindo o task que processa a proposta
    cria_grupo_autom_hist = PythonOperator(
        task_id = 'cria_grupo_econ_hist',
        python_callable = cria_grupo_economico_automatico_historico,
        provide_context = True
    )
    '''

    # Definindo o task que processa a proposta
    cria_grupo_autom_rotina = PythonOperator(
        task_id = 'cria_grupo_econ_rotina',
        python_callable = cria_grupo_economico_automatico_rotina,
        provide_context = True
    )   

    # Definindo a ordem de execução das tasks
    cria_grupo_autom_rotina
    #cria_grupo_autom_hist >> cria_grupo_autom_rotina
    