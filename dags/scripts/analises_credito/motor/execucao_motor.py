# IMPORT LIBS
from datetime import datetime, timedelta, timezone
from time import sleep

# AIRFLOW LIBS
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.models import Variable

# SCRIPTS
from scripts.analises_credito.motor.execucao_analise_pre_filtro import analise_pre_filtro
from scripts.analises_credito.motor.execucao_modelo import execucao_modelo
from scripts.analises_credito.motor.execucao_politica import execucao_politica
from scripts.analises_credito.motor.execucao_envio_kafka import envio_kafka
from scripts.analises_credito.motor.execucao_chamada_serasa import chamando_serasa


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
    "stage": Variable.get('STAGE'),
    "kafka_url": Variable.get('KAFKA_DATALAKE_ENDPOINT')
}
    

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
    schedule_interval='0 18 * * *',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'minio', 'mesa', 'variaveis', 'motor']
)

def execucao_motor():
    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    @task(executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}})
    def pre_filtro_task():
        analise_pre_filtro(access_params)

    @task(executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}})
    def serasa_task():
        chamando_serasa(access_params)
    
    @task(executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}})
    def modelo_task():
        execucao_modelo(access_params)
        
    @task(executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}})
    def politica_task():
        execucao_politica(access_params)

    @task(executor_config={"KubernetesExecutor": {"request_memory": "4000Mi"}})
    def envio_kafka_task():
        envio_kafka(access_params)
    
    @task 
    def wait_1_minute():
        sleep(2400)  # Espera por 60 segundos

    # # Define a time delay of 1 minute between tasks
    # wait_1_minute = TimeDeltaSensor(
    #     task_id="wait_1_minute",
    #     delta=timedelta(minutes=1)
    # )

    pre_filtro = pre_filtro_task()   
    serasa = serasa_task()
    modelo = modelo_task()
    politica = politica_task()
    kafka = envio_kafka_task()

    # Set dependencies between tasks
    init_data_load >> pre_filtro >> serasa >> wait_1_minute >> modelo >> politica >> kafka >> finish_data_load
    #init_data_load >> pre_filtro >> serasa >> modelo >> politica >> finish_data_load

execucao_motor()