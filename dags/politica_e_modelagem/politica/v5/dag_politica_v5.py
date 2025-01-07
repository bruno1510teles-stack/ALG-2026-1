### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep
from datetime import timedelta
import requests


### Importando scripts necessários
from politica_e_modelagem.politica.v5 import pre_filtro_task
from politica_e_modelagem.auxiliares.serasa import execucao_chamada_serasa


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

'''
def notificar_falha_teams(context):
    task_id = context['task_instance'].task_id

    url = "https://yandehbr.webhook.office.com/webhookb2/3efc9ab8-aba8-4150-8e68-864d086592a3@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/2bb511bca72643d58ea858c433be3aec/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2AAjaUAPO15qUofSpSzGh6PW4gkg2FJypyvorUwW89eU1"

    # Verifica se a task_id é 'captura_proposta' e, se for, não envia notificação
    if task_id == "captura_proposta":
        print("Notificação não enviada para 'captura_proposta'")
    else:
          

        mensagem = {
            "title": "Falha na DAG - politica_v_2",
            "text": f"Falha na DAG: {context['task_instance'].dag_id} na task: {context['task_instance'].task_id} VERIFICAR URGENTE!!"
        }
        requests.post(url, json=mensagem)
'''

### Definindo defaults
default_args = {
    "owner": "Vinicius Moraes",
    #"retries": 1,
    "retry_delay": timedelta(minutes=1),
    #"on_failure_callback": notificar_falha_teams
}


# Definindo a DAG
with DAG(
    dag_id='politica_v5',
    start_date=days_ago(1),
    default_args=default_args,
    tags=['politica_v5', 'lote', 'pre-aprovado'],
    max_active_runs=1  # Apenas uma execução ativa
) as dag:
     
    # Definindo o task de pre filtro
    pre_filtro = PythonOperator(
        task_id="pre_filtro_task",
        python_callable=pre_filtro_task.analise_pre_filtro_v5,
        op_kwargs={'access_params': access_params},
        provide_context=True,
    )

    # Definindo o task que faz a chamada do serasa
    serasa = PythonOperator(
        task_id="serasa_task",
        python_callable=execucao_chamada_serasa.chamando_serasa,
        op_kwargs={'access_params': access_params},
        provide_context=True,
    )

    # Definindo a ordem de execução das tasks
    pre_filtro >> serasa