from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import requests
from datetime import timedelta
from airflow.utils.dates import datetime

def notificar_falha_teams(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']

    url = "https://yandehbr.webhook.office.com/webhookb2/3efc9ab8-aba8-4150-8e68-864d086592a3@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/2bb511bca72643d58ea858c433be3aec/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2AAjaUAPO15qUofSpSzGh6PW4gkg2FJypyvorUwW89eU1"

    mensagem = {
        "title": "Falha na DAG - Processamento Receita Federal",
        "text": f"Falha na DAG: {dag_id} na task: {task_id} \nData de execução: {execution_date}\nVERIFICAR URGENTE!!"
    }

    response = requests.post(url, json=mensagem)

    if response.status_code != 200:
        raise Exception(f"Falha ao enviar notificação para o Teams. Status code: {response.status_code}")


default_args = {
    'owner': 'Felipe Ferraz',
    'start_date': days_ago(1),
    #"on_failure_callback": notificar_falha_teams
} 


with DAG(
    dag_id='auxiliar_parecer',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['auxiliar', 'parecer', 'motor', 'refined'],
    max_active_runs=1
) as dag:

    parecer_motor = SparkKubernetesOperator(
        task_id='auxiliar_parecer_motor',
        application_file='auxiliar-parecer-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )

    parecer_motor