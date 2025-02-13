### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep


### Importando scripts necessários
from politica_e_modelagem.politica.v3.zerar_limite import exporta_csv
from politica_e_modelagem.politica.v3.zerar_limite import captura_proposta
from politica_e_modelagem.politica.v3.zerar_limite import executa_politica
from politica_e_modelagem.politica.v3.zerar_limite import envio_kafka_task    


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
    dag_id='politica_v_3',
    start_date=days_ago(1),
    schedule_interval=None,
    default_args=default_args,
    tags=['politica_v3', 'zerar_limite']  # DAG só será acionada manualmente pela API
) as dag:

    # Filtra casos que serão vencidos e exporta para o devido bucket
    exporta_csv_zerar_limites = PythonOperator(
        task_id = 'exporta_csv_zerar_limites',
        python_callable = exporta_csv.execucao_politica_zerar_limites,
        provide_context = True  # Habilita o envio do contexto (incluindo conf)
    )

    # Captura proposta no jira
    captura_proposta = PythonOperator(
        task_id = "captura_proposta",
        python_callable = captura_proposta.captura_proposta,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Execução da política
    executa_politica = PythonOperator(
        task_id = "executa_politica",
        python_callable = executa_politica.execucao_politica,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )


    # Enviando dados para o Kafka
    enviar_kafka = PythonOperator(
        task_id = "envio_kafka_task",
        python_callable = envio_kafka_task.envio_kafka,
        op_kwargs = {'access_params': access_params},
        provide_context = True,
    )


    # Definindo a ordem de execução das tasks
    exporta_csv_zerar_limites >> captura_proposta >> executa_politica >> enviar_kafka