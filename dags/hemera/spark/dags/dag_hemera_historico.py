### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep
import requests
from datetime import timedelta
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

### Importando scripts necessários

from hemera.spark.hemera_historico import aquisicao_excel_to_csv_historico
from hemera.spark.hemera_historico import estoque_excel_to_csv_historico
from hemera.spark.hemera_historico import recompra_excel_to_csv_historico
from hemera.spark.hemera_historico import retorno_excel_to_csv_historico


## Scripts que tratam o consolidado - Trusted
from hemera.spark.hemera_historico import aquisicao_consolidado_trusted
from hemera.spark.hemera_historico import estoque_consolidado_trusted
from hemera.spark.hemera_historico import recompra_consolidado_trusted
from hemera.spark.hemera_historico import retorno_consolidado_trusted


## Scripts Trusted to Refined
from hemera.spark.hemera_historico import aquisicao_refined
from hemera.spark.hemera_historico import estoque_refined
from hemera.spark.hemera_historico import recompra_refined
from hemera.spark.hemera_historico import retorno_refined


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
    url = "https://yandehbr.webhook.office.com/webhookb2/3efc9ab8-aba8-4150-8e68-864d086592a3@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/2bb511bca72643d58ea858c433be3aec/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2AAjaUAPO15qUofSpSzGh6PW4gkg2FJypyvorUwW89eU1"
    mensagem = {
        "title": f"Falha na Execução DAG - {context['task_instance'].dag_id}",
        "text": f"Falha na DAG: {context['task_instance'].dag_id} na task: {context['task_instance'].task_id} VERIFICAR URGENTE!!"
    }
    requests.post(url, json=mensagem)

    '''

### Definindo defaults
default_args = {
    "owner": "Vinicius Moraes",
    #"on_failure_callback": notificar_falha_teams
}

# Definindo a DAG
with DAG(
    dag_id='hemera_historico',
    start_date=days_ago(1),
    schedule_interval=None,
    default_args=default_args,
    tags=['hemera', 'historico'] 
) as dag:

    aquisicao_excel_to_csv_hist = PythonOperator(
        task_id="aquisicao_excel_to_csv_hist",
        python_callable=aquisicao_excel_to_csv_historico.aquisicao_excel_to_csv_historico,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )


    estoque_excel_to_csv_hist = PythonOperator(
        task_id="estoque_excel_to_csv_hist",
        python_callable=estoque_excel_to_csv_historico.estoque_excel_to_csv_historico,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )


    recompra_excel_to_csv_hist = PythonOperator(
        task_id="recompra_excel_to_csv_hist",
        python_callable=recompra_excel_to_csv_historico.recompra_excel_to_csv_historico,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )


    retorno_excel_to_csv_hist = PythonOperator(
        task_id="retorno_excel_to_csv_hist",
        python_callable=retorno_excel_to_csv_historico.retorno_excel_to_csv_historico,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )


    ## TRUSTED

    aquisicao_consolid_trusted = PythonOperator(
        task_id="aquisicao_consolidado_trusted",
        python_callable=aquisicao_consolidado_trusted.aquisicao_consolidado_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    estoque_consolid_trusted = SparkKubernetesOperator(
        task_id='estoque_consolidado_trusted',
        application_file='estoque-consolidado-trusted-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    recompra_consolid_trusted = PythonOperator(
        task_id="recompra_consolidado_trusted",
        python_callable=recompra_consolidado_trusted.recompra_consolidado_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    retorno_consolid_trusted = PythonOperator(
        task_id="retorno_consolidado_trusted",
        python_callable=retorno_consolidado_trusted.retorno_consolidado_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    
    ## REFINED

    aquisicao_refined_task = PythonOperator(
        task_id="aquisicao_refined",
        python_callable=aquisicao_refined.aquisicao_trusted_to_refined,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    estoque_refined_task = SparkKubernetesOperator(
        task_id='estoque_refined',
        application_file='estoque-refined-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    recompra_refined_task = PythonOperator(
        task_id="recompra_refined",
        python_callable=recompra_refined.recompra_trusted_to_refined,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    retorno_refined_task = PythonOperator(
        task_id="retorno_refined",
        python_callable=retorno_refined.retorno_trusted_to_refined,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )
    
    # Definindo a ordem de execução das tasks
    aquisicao_excel_to_csv_hist >> estoque_excel_to_csv_hist >> recompra_excel_to_csv_hist >> retorno_excel_to_csv_hist >> aquisicao_consolid_trusted >> estoque_consolid_trusted >> recompra_consolid_trusted >> retorno_consolid_trusted >> aquisicao_refined_task >> estoque_refined_task >> recompra_refined_task >> retorno_refined_task