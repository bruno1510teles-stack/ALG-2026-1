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

### Importando scripts necessários (Excel to Csv) - Rotina

from hemera.spark.hemera_rotina import aquisicao_excel_to_csv_diario
from hemera.spark.hemera_rotina import estoque_excel_to_csv_diario
from hemera.spark.hemera_rotina import recompra_excel_to_csv_diario
from hemera.spark.hemera_rotina import retorno_excel_to_csv_diario

## Scripts que tratam o consolidado - Trusted
from hemera.spark.hemera_rotina import aquisicao_diario_trusted
from hemera.spark.hemera_rotina import recompra_diario_trusted
from hemera.spark.hemera_rotina import retorno_diario_trusted

## Scripts Trusted to Refined
from hemera.spark.hemera_historico import aquisicao_refined
from hemera.spark.hemera_historico import recompra_refined
from hemera.spark.hemera_historico import retorno_refined

## Fechamento
from hemera.spark.hemera_historico import fechamento_hemera


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
    "on_failure_callback": notificar_falha_teams,
}


# Definindo a DAG
with DAG(
    dag_id='hemera_rotina',
    start_date=days_ago(1),
    schedule_interval='30 15 * * 1-5',
    schedule_interval=None,
    default_args=default_args,
    tags=['hemera', 'rotina'] 
) as dag:

    aquisicao_excel_to_csv_rotina = PythonOperator(
        task_id="aquisicao_excel_to_csv_diario",
        python_callable=aquisicao_excel_to_csv_diario.aquisicao_excel_to_csv_diario,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )


    estoque_excel_to_csv_rotina = PythonOperator(
        task_id="estoque_excel_to_csv_diario",
        python_callable=estoque_excel_to_csv_diario.estoque_excel_to_csv_diario,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )


    recompra_excel_to_csv_rotina = PythonOperator(
        task_id="recompra_excel_to_csv_diario",
        python_callable=recompra_excel_to_csv_diario.recompra_excel_to_csv_diario,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )


    retorno_excel_to_csv_rotina = PythonOperator(
        task_id="retorno_excel_to_csv_diario",
        python_callable=retorno_excel_to_csv_diario.retorno_excel_to_csv_diario,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )


    ## TRUSTED

    aquisicao_rotina_trusted = PythonOperator(
        task_id="aquisicao_diario_trusted",
        python_callable=aquisicao_diario_trusted.aquisicao_diario_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    estoque_rotina_trusted = SparkKubernetesOperator(
        task_id='estoque_diario_trusted',
        application_file='estoque-diario-trusted-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    recompra_rotina_trusted = PythonOperator(
        task_id="recompra_diario_trusted",
        python_callable=recompra_diario_trusted.recompra_diario_trusted,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    retorno_rotina_trusted = PythonOperator(
        task_id="retorno_diario_trusted",
        python_callable=retorno_diario_trusted.retorno_diario_trusted,
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

    ## Processo Fe
    consolidado_hemera_produto = SparkKubernetesOperator(
        task_id='consolidado_hemera_produto',
        application_file='estoque-consolidado-hemera-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )


    ## FECHAMENTO
    fechamento = PythonOperator(
        task_id="fechamento",
        python_callable=fechamento_hemera.fechamento_hemera_refined,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    
    # Definindo a ordem de execução das tasks
    aquisicao_excel_to_csv_rotina >> estoque_excel_to_csv_rotina >> recompra_excel_to_csv_rotina >> retorno_excel_to_csv_rotina >> aquisicao_rotina_trusted >> estoque_rotina_trusted >> recompra_rotina_trusted >> retorno_rotina_trusted >> aquisicao_refined_task >> estoque_refined_task >> recompra_refined_task >> retorno_refined_task >> consolidado_hemera_produto >> fechamento