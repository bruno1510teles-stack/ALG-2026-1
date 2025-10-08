from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import requests
from datetime import timedelta
from airflow.utils.dates import datetime

class NoTemplateSparkKubernetesOperator(SparkKubernetesOperator):
    template_fields = ()

def notificar_falha_teams(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']

    url = "https://yandehbr.webhook.office.com/webhookb2/3efc9ab8-aba8-4150-8e68-864d086592a3@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/2bb511bca72643d58ea858c433be3aec/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2AAjaUAPO15qUofSpSzGh6PW4gkg2FJypyvorUwW89eU1"

    mensagem = {
        "title": "Falha na DAG - Cálculo Faturamento Estimado",
        "text": f"Falha na DAG: {dag_id} na task: {task_id} \nData de execução: {execution_date}\nVERIFICAR URGENTE!!"
    }

    response = requests.post(url, json=mensagem)

    if response.status_code != 200:
        raise Exception(f"Falha ao enviar notificação para o Teams. Status code: {response.status_code}")


default_args = {
    'owner': 'Vinicius Moraes Teixeira',
    #'start_date': days_ago(1),
    "on_failure_callback": notificar_falha_teams
} 


with DAG(
    dag_id='faturamento_estimado',
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['faturamento', 'motor', 'refined'],
    max_active_runs=1,
    schedule_interval = '0 10 * * 0-1',
) as dag:

    app_fat_estimado = "/opt/airflow/dags/repo/dags/faturamento_estimado/faturamento-estimado-spark-app.yaml"

    calculo_fat_estimado = NoTemplateSparkKubernetesOperator(
        task_id="calcula_faturamento_estimado",
        application_file = app_fat_estimado,
        namespace="spark",
        kubernetes_conn_id="kubernetes_default",
        startup_timeout_seconds=600,
        retries=5,
        reattach_on_restart=True,
        log_events_on_failure=True,
        get_logs=True
    )

    calculo_fat_estimado