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
from datetime import datetime
from datetime import timedelta
from time import sleep


### Importando scripts necessários
from jira.propostas import extracao_jira_raw
from jira.propostas import extracao_jira_trusted
from jira.propostas import extracao_jira_refined
from jira.propostas import propostas_boletos_vop
from jira.propostas import jira_notif



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

# def notificar_falha_teams(context):
#     url = "https://yandehbr.webhook.office.com/webhookb2/3efc9ab8-aba8-4150-8e68-864d086592a3@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/2bb511bca72643d58ea858c433be3aec/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2AAjaUAPO15qUofSpSzGh6PW4gkg2FJypyvorUwW89eU1"
#     mensagem = {
#         "title": f"Falha na Execução DAG - {context['task_instance'].dag_id}",
#         "text": f"Falha na DAG: {context['task_instance'].dag_id} na task: {context['task_instance'].task_id} VERIFICAR URGENTE!!"
#     }
#     requests.post(url, json=mensagem)
 
### Definindo defaults
default_args = {
    "owner": "Kevin Cardoso",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # "on_failure_callback": notificar_falha_teams
}

# Definindo o fuso horário de São Paulo
local_tz = pendulum.timezone("America/Sao_Paulo")

def check_time_to_run(execution_date, **kwargs):
    """
    Verifica se o horário de execução é igual ou posterior a 21:00 UTC.
    """
    # Certifica-se de que execution_date é um objeto datetime
    if isinstance(execution_date, str):
        execution_time = datetime.fromisoformat(execution_date)  # Converte a string ISO-8601 para datetime
    else:
        execution_time = execution_date  # Já é datetime, usa diretamente

    # Log para depuração
    print(f"execution_date processado: {execution_time}")

    # Verifica se o horário é igual ou posterior a 21:00 UTC
    if execution_time.hour >= 15:
        return 'enviar_notif_daily'  # Executa a task se for 21:00 UTC ou mais
    return 'skip_task'  # Caso contrário, pula a execução

# Definindo a DAG
with DAG(
    dag_id='processo_jira_propostas',
    start_date=days_ago(1),
    schedule_interval='0 11,21 * * *',  # 08:00 e 18:00 São Paulo (11:00 e 21:00 UTC)
    default_args=default_args,
    tags=['etl', 'jira','raw','trusted'],
    max_active_runs=1
) as dag:

    # Definindo as tasks
    extracao_jira_to_raw = PythonOperator(
        task_id='extracao_jira_raw',
        python_callable=extracao_jira_raw.base_details_raw,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    extracao_jira_raw_to_trusted = PythonOperator(
        task_id='extracao_jira_trusted',
        python_callable=extracao_jira_trusted.base_details_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    extracao_jira_to_refined = PythonOperator(
        task_id='extracao_jira_refined',
        python_callable=extracao_jira_refined.base_details_refined,
        provide_context=True
    )

    propostas_boletos_vop_aux = PythonOperator(
        task_id='merge_proposta_boletos_vop',
        python_callable=propostas_boletos_vop.merge_propostas_boletos,
        provide_context=True
    )

    # BranchPythonOperator para verificar o horário e decidir qual task executar
    check_time_task = BranchPythonOperator(
        task_id='check_time',
        python_callable=check_time_to_run,
        provide_context=True,
        op_kwargs={'execution_date': '{{ ts }}'}
    )

    # Task para pular caso não seja 18:00
    skip_task = DummyOperator(task_id='skip_task')

    # Task para enviar notificações (executa somente às 18:00 São Paulo)
    jira_notif_teams = PythonOperator(
        task_id='enviar_notif_daily',
        python_callable=jira_notif.enviar_notif,
        provide_context=True
    )

    # Definindo a ordem de execução das tasks
    extracao_jira_to_raw >> extracao_jira_raw_to_trusted >> extracao_jira_to_refined >> propostas_boletos_vop_aux >> check_time_task
    check_time_task >> [jira_notif_teams, skip_task] 