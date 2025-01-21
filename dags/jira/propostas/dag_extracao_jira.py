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
from extracao_jira_raw import base_details_raw
from extracao_jira_trusted import base_details_trusted
from extracao_jira_refined import base_details_refined
from propostas_boletos_vop import merge_propostas_boletos
from jira_notif import  enviar_notif
from captura_propostas_jira_v2 import captura_propostas_jira



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
    
    



def check_time_to_run(**kwargs):
    """
    Verifica se o horário atual é igual ou posterior ao último horário programado no dia (21:00 UTC).
    """
    # Obtém o horário atual em UTC
    current_time_utc = datetime.now(timezone.utc)

    # Logs para depuração
    print(f"Horário atual UTC: {current_time_utc}")

    # Verifica se o horário atual é posterior ou igual a 21:00 UTC
    if current_time_utc.hour >= 21:
        print("Horário válido para executar a notificação.")
        return 'enviar_notif_daily'  # Executa a task
    else:
        print("Horário inválido. Pulando a notificação.")
        return 'skip_task'  # Pula a execução
    
    

# Definindo defaults
default_args = {
    "owner": "Kevin Cardoso",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notificar_falha_teams
}



# Definindo a DAG
with DAG(
    dag_id='processo_jira_propostas',
    start_date=days_ago(1),
    schedule_interval='0 11,21 * * *',
    default_args=default_args,
    tags=['etl', 'jira', 'raw', 'trusted','refined'],
    max_active_runs=1
) as dag:

    '''
    # Definindo as tasks
    extracao_jira_to_raw = PythonOperator(
        task_id='extracao_jira_raw',
        python_callable=base_details_raw,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    extracao_jira_raw_to_trusted = PythonOperator(
        task_id='extracao_jira_trusted',
        python_callable=base_details_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    extracao_jira_to_refined = PythonOperator(
        task_id='extracao_jira_refined',
        python_callable=base_details_refined,
        provide_context=True
    )

    propostas_boletos_vop_aux = PythonOperator(
        task_id='merge_proposta_boletos_vop',
        python_callable=merge_propostas_boletos,
        provide_context=True
    )
    '''
    captura_proposta_jira_v2 = PythonOperator(
        task_id='captura_proposta_jira_v2',
        python_callable=captura_propostas_jira,
        provide_context=True
    )
    '''
    # BranchPythonOperator para verificar o horário e decidir qual task executar
    check_time_task = BranchPythonOperator(
        task_id='check_time',
        python_callable=check_time_to_run,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ts }}'}
    )

    # Task para pular caso não seja 21:00 UTC
    skip_task = DummyOperator(task_id='skip_task')

    # Task para enviar notificações (executa somente às 21:00 UTC)
    jira_notif_teams = PythonOperator(
        task_id='enviar_notif_daily',
        python_callable=enviar_notif,
        provide_context=True
    )
    '''

    # Definindo a ordem de execução das tasks
    captura_proposta_jira_v2
    #extracao_jira_to_raw >> extracao_jira_raw_to_trusted >> extracao_jira_to_refined >> propostas_boletos_vop_aux >> check_time_task
    #check_time_task >> [jira_notif_teams, skip_task]