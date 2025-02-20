import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import requests
from datetime import timedelta

from politica_e_modelagem.hub_politicas.scripts.hub_politicas import aplicar_politica 
from politica_e_modelagem.hub_politicas.scripts.pre_filtro_comum import analise_pre_filtro 

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
    "keycloack_token_url": Variable.get('KEYCLOAK_TOKEN_URL'),
    "jira_url": Variable.get('JIRA_API_URL'),
    "jira_api_token": Variable.get('JIRA_API_TOKEN'),
    "jira_api_user": Variable.get('JIRA_API_USER'),
    "jira_project": Variable.get('JIRA_PROJECT'),
    "jira_max_result": 50
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
    "owner": "Felipe Ferraz",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notificar_falha_teams
}

# Definindo a DAG
with DAG(
    dag_id='hub_politica',
    start_date=days_ago(1),
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    catchup=False,
    tags=['proposta-negocio', 'definir política', 'pre-filtro'],
    description="Pre filtro das propostas de negócio independente da política",
    max_active_tasks=1,
    max_active_runs=1
) as dag:
    
    politicas_task = PythonOperator(
            task_id="selecionar_politica",
            python_callable=aplicar_politica,
            op_kwargs={'access_params': access_params},
            execution_timeout=timedelta(minutes=15),
            provide_context=True
        )

    prefiltro_task =  PythonOperator(
            task_id="pre_filtro",
            python_callable=analise_pre_filtro,
            op_kwargs={'access_params': access_params},
            execution_timeout=timedelta(minutes=15),
            provide_context=True
        )
    
    # Definindo a ordem de execução das tasks
    politicas_task >> prefiltro_task
    