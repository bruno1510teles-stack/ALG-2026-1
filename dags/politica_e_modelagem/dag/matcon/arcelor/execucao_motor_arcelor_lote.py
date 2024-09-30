### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep


### Importando scripts necessários
from politica_e_modelagem.auxiliares.jira import import_base_jira_lote
from politica_e_modelagem.pre_filtro.matcon.arcelor import pre_filtro_arcelor_lote
from politica_e_modelagem.auxiliares.serasa import execucao_chamada_serasa
from politica_e_modelagem.modelo.matcon.arcelor import execucao_modelo_arcelor_v_1_0
from politica_e_modelagem.politica.matcon.arcelor import polotica_arcelor_lote
from politica_e_modelagem.auxiliares.kafka import execucao_envio_kafka


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



### Definindo defaults
default_args = {
    "owner": "Felipe Ferraz",
    "retries": 0,
}


# Definindo a DAG
with DAG(
    dag_id='execucao_politica_em_lote',
    start_date=days_ago(1),
    schedule_interval= None,
    default_args=default_args,
    tags=['execucao_politica_em_lote'],
    max_active_runs=1  # Apenas uma execução ativa
) as dag:

    # Definindo o task que processa a proposta
    captura_proposta = PythonOperator(
        task_id='captura_proposta',
        python_callable=import_base_jira_lote.base_analisar,
        provide_context=True  # Habilita o envio do contexto (incluindo conf)
    )    
    # Definindo o task de pre filtro
    pre_filtro = PythonOperator(
        task_id="pre_filtro_task",
        python_callable=pre_filtro_arcelor_lote.analise_pre_filtro,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    # Definindo o task que faz a chamada do serasa
    serasa = PythonOperator(
        task_id="serasa_task",
        python_callable=execucao_chamada_serasa.chamando_serasa,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    # Execução do modelo
    modelo = PythonOperator(
        task_id="modelo_task",
        python_callable=execucao_modelo_arcelor_v_1_0.execucao_modelo,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    # Execução da política
    politica = PythonOperator(
        task_id="politica_task",
        python_callable=polotica_arcelor_lote.execucao_politica,
        op_kwargs={'access_params': access_params},
        provide_context=True
    )

    # Enviando dados para o Kafka
    enviar_kafka = PythonOperator(
        task_id="envio_kafka_task",
        python_callable=execucao_envio_kafka.envio_kafka,
        op_kwargs={'access_params': access_params},
        provide_context=True,
    )

    # Definindo o sleep para processo de dados no data lake
    aguarde = PythonOperator(
        task_id="wait_1_minute",
        python_callable=lambda: sleep(1020),  # Espera por 1 minuto (ajustado de 17 minutos)
    )

    # Definindo a ordem de execução das tasks
    captura_proposta >> pre_filtro >> serasa >> aguarde >> modelo >> politica >> enviar_kafka