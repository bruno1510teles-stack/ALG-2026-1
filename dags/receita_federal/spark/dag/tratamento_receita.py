from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import requests
from datetime import timedelta


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
    "retries": 3,
    "retry_delay": timedelta(minutes=5)#,
    #"on_failure_callback": notificar_falha_teams
} 


with DAG(
    dag_id='tratamento_dados_receita_federal',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args = default_args,
    tags=['etl', 'receita_federal', 'raw', 'trusted', 'refined'],
    max_active_runs=1
) as dag:

    '''
    cnae = SparkKubernetesOperator(
        task_id='cnae',
        application_file='cnae-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )

    empresas = SparkKubernetesOperator(
        task_id='empresas',
        application_file='empresas-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )

    estabelecimentos = SparkKubernetesOperator(
        task_id='estabelecimentos',
        application_file='estabelecimentos-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )

    motivos = SparkKubernetesOperator(
        task_id='motivos',
        application_file='motivos-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )

    municipios = SparkKubernetesOperator(
        task_id='municipios',
        application_file='municipios-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )
    naturezas = SparkKubernetesOperator(
        task_id='naturezas',
        application_file='naturezas-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )
    paises = SparkKubernetesOperator(
        task_id='paises',
        application_file='paises-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )
    qualificacoes = SparkKubernetesOperator(
        task_id='qualificacoes',
        application_file='qualificacoes-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )
    simples = SparkKubernetesOperator(
        task_id='simples',
        application_file='simples-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )
    socios = SparkKubernetesOperator(
        task_id='socios',
        application_file='socios-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=120)
    )
    '''
    
    pre_filtro = SparkKubernetesOperator(
        task_id='pre_filtro',
        application_file='pre-filtro-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        deferrable=True,
        execution_timeout=timedelta(minutes=120)
    )

    '''
    dados_cadastrais = SparkKubernetesOperator(
        task_id='dados-cadastrais',
        application_file='dados-cadastrais-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
        retries = 3, # Ajustando retries
        retry_delay=timedelta(minutes=120) # Ajuste
    )
    '''

    pre_filtro

    #cnae >> empresas >> estabelecimentos >> motivos >> municipios >> naturezas >> paises >> qualificacoes >> simples >> socios >> pre_filtro >> dados_cadastrais