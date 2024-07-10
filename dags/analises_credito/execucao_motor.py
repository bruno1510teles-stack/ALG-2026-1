    # IMPORT LIBS
from datetime import datetime, timedelta, timezone

# AIRFLOW LIBS
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.models import Variable

# SCRIPTS
from scripts.analises_credito.motor.execucao_analise_pre_filtro import analise_pre_filtro
from scripts.analises_credito.motor.execucao_motor import execucao_motor

#PARAMETROS DE ACESSO
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
    "stage": Variable.get('STAGE')
}
    
# DEFINE VARIABLES
MINIO_CONN_RAW = "minio_raw"
MINIO_RAW_BUCKET = "receita-federal"
RECEITA_FEDERAL_SIMPLES_FOLDER = "simples/"

# DEFINE DEFAULT ARGS
default_args = {
    "owner": "João Leite",
    "retries": 0,
    "retry_delay": 0,
    "execution_timeout": timedelta(seconds=60 * 60 * 4),
}

# DEFINE DAG
@dag(
    start_date=datetime(2024, 3, 1), # definir quando for rodar automatico
    max_active_runs=1,
    schedule_interval='0 18 28 * *',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'minio', 'mesa', 'variaveis', 'motor']
)

def execucao_motor():
    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")
    
    @task(executor_config={"KubernetesExecutor": {"request_memory": "12000Mi"}})
    def pre_filtro(access_params):
        
        return analise_pre_filtro(access_params)

    @task(executor_config={"KubernetesExecutor": {"request_memory": "12000Mi"}})
    def motor(access_params):
         
         return execucao_motor(access_params)

    # run order
    init_data_load >> pre_filtro >> motor >> finish_data_load

execucao_motor()