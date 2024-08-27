# IMPORT LIBS
from datetime import datetime, timedelta
from time import sleep

# AIRFLOW LIBS
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# SCRIPTS
from scripts.analises_credito.motor.execucao_import_base_analisar import base_analisar
from scripts.analises_credito.motor.execucao_analise_pre_filtro import analise_pre_filtro
from scripts.analises_credito.motor.execucao_chamada_serasa import chamando_serasa
from scripts.analises_credito.motor.execucao_modelo import execucao_modelo
from scripts.analises_credito.motor.execucao_politica import execucao_politica
from scripts.analises_credito.motor.execucao_envio_kafka import envio_kafka


# PARAMETROS DE ACESSO
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

# DEFINE DEFAULT ARGS
default_args = {
    "owner": "João Leite",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=4),
}

# DEFINE DAG
with DAG(
    dag_id="execucao_motor",
    start_date=datetime(2024, 3, 1),
    max_active_runs=1,
    schedule_interval='0 18 * * *',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'minio', 'mesa', 'variaveis', 'motor']
) as dag:
    
    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    # Python tasks
    base = PythonOperator(
        task_id="base_analisar_task",
        python_callable=base_analisar,
        op_kwargs={'access_params': access_params},
        executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}},
    )

    pre_filtro = PythonOperator(
        task_id="pre_filtro_task",
        python_callable=analise_pre_filtro,
        op_kwargs={'access_params': access_params},
        executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}},
    )

    serasa = PythonOperator(
        task_id="serasa_task",
        python_callable=chamando_serasa,
        op_kwargs={'access_params': access_params},
        executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}},
    )
    
    modelo = PythonOperator(
        task_id="modelo_task",
        python_callable=execucao_modelo,
        op_kwargs={'access_params': access_params},
        executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}},
    )
        
    politica = PythonOperator(
        task_id="politica_task",
        python_callable=execucao_politica,
        op_kwargs={'access_params': access_params},
        executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}},
    )

    enviar_kafka = PythonOperator(
        task_id="envio_kafka_task",
        python_callable=envio_kafka,
        op_kwargs={'access_params': access_params},
        executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}},
    )
    
    wait_1_minute = PythonOperator(
        task_id="wait_1_minute",
        python_callable=lambda: sleep(2400),  # Espera por 60 segundos
    )

    # Ordem
    init_data_load >> base >> pre_filtro >> serasa >> wait_1_minute >> modelo >> politica >> enviar_kafka >> finish_data_load