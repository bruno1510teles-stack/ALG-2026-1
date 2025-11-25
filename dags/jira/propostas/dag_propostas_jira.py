### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import requests
import pendulum
import sys
from datetime import datetime, timezone, timedelta
from time import sleep


sys.path.append('/opt/airflow/dags/repo/dags/jira/propostas')
from processa_historico_propostas import processa_historico
from captura_proposta import captura_proposta
from raw_to_trusted import raw_to_trusted
from trusted_to_refined import trusted_to_refined
from ydh_raw_to_trusted import ydh_raw_to_trusted
from ydh_trusted_to_refined import ydh_trusted_to_refined
from tabela_decisor import decisor_trusted


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

    

# Definindo defaults
default_args = {
    "owner": "Vinicius Moraes",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notificar_falha_teams
}


# Definindo a DAG
with DAG(
    dag_id='processo_jira_propostas',
    start_date=days_ago(1),
    schedule_interval = '30 14,20 * * *', #Roda as 11:30 e 17:30 todos os dias
    default_args=default_args,
    catchup=False, 
    tags=['etl', 'jira', 'raw', 'trusted','refined'],
    max_active_runs=1
) as dag:
    
    '''
    processa_historico = PythonOperator(
         task_id='processa_historico_propostas',
         python_callable=processa_historico,
         op_kwargs={'access_params': access_params},
         provide_context=True
    )
    '''
    
    
    task1 = PythonOperator(
        task_id='captura_proposta',
        python_callable = captura_proposta,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    task2 = PythonOperator(
        task_id='raw_to_trusted',
        python_callable = raw_to_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    task3 = PythonOperator(
        task_id='extraindo_categoria_decisor',
        python_callable=decisor_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    task4 = PythonOperator(
        task_id='ydh_raw_to_trusted',
        python_callable = ydh_raw_to_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    task5 = PythonOperator(
        task_id='trusted_to_refined',
        python_callable = trusted_to_refined,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    task6 = PythonOperator(
        task_id='ydh_trusted_to_refined',
        python_callable = ydh_trusted_to_refined,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    # Definindo a ordem de execução das tasks
    #processa_historico
    task1 >> task2 >> task3 >> task4 >> task5 >> task6
