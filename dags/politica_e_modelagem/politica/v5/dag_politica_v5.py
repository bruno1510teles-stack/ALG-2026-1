# Importando Libs necessárias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep

### Importando scripts necessários
from politica_e_modelagem.politica.v5.importa_base_lote import importa_base_pre_aprovado_lote
#from dags.politica_e_modelagem.politica.v5.pre_filtro_task_old import pre_filtro_task
from dags.politica_e_modelagem.politica.v5.pre_filtro_task_new import pre_filtro_task_v2
from politica_e_modelagem.politica.v5.serasa import compra_info_serasa
from politica_e_modelagem.politica.v5.motor_v5 import executa_politica_v5
from politica_e_modelagem.politica.v5.exporta_csv import exporta_csv_cria_proposta_jira
from politica_e_modelagem.politica.v5.captura_proposta import captura_proposta
from politica_e_modelagem.politica.v5.executa_politica import execucao_politica
from politica_e_modelagem.politica.v5.envio_kafka import envio_kafka
 
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


# Definindo defaults
default_args = {
    "owner": "Vinicius Moraes",
    "retries": 0,
}


# Definindo a DAG
with DAG(
    dag_id='politica_v5',
    start_date=days_ago(1),
    schedule_interval=None,
    default_args=default_args,
    tags=['politica_v5', 'pre_aprovado', 'lote']  # DAG só será acionada manualmente pela API
) as dag:

    # Filtra casos que serão vencidos e exporta para o devido bucket
    task1 = PythonOperator(
        task_id = 'importa_base_lote',
        python_callable = importa_base_pre_aprovado_lote,
        provide_context = True  # Habilita o envio do contexto (incluindo conf)
    )

    # Captura proposta no jira
    task2 = PythonOperator(
        task_id = "pre_filtro",
        python_callable = pre_filtro_task_v2,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Captura proposta no jira
    task3 = PythonOperator(
        task_id = "compra_info_serasa",
        python_callable = compra_info_serasa,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Captura proposta no jira
    task4 = PythonOperator(
        task_id = "motor_v5",
        python_callable = executa_politica_v5,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Captura proposta no jira
    task5 = PythonOperator(
        task_id = "exporta_csv",
        python_callable = exporta_csv_cria_proposta_jira,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Captura proposta no jira
    task6 = PythonOperator(
        task_id = "captura_proposta",
        python_callable = captura_proposta,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Captura proposta no jira
    task7 = PythonOperator(
        task_id = "executa_politica",
        python_callable = execucao_politica,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Captura proposta no jira
    task8 = PythonOperator(
        task_id = "envio_kafka",
        python_callable = envio_kafka,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Definindo a ordem de execução das tasks
    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8