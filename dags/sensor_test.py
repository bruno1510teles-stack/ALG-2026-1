# IMPORT LIBS
from datetime import datetime, timedelta, timezone

# AIRFLOW LIBS
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from minio import Minio
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

# Create the Minio connection dynamically
def create_minio_connection():
    # Define your Minio connection parameters
    MINIO_CONN_ID = "minio_default"
    MINIO_HOST = "https://minio-datalake.alpe.tech/raw/browser"
    MINIO_ACCESS_KEY = "vGZStLPT4O0161DWwxBz"
    MINIO_SECRET_KEY = "ggXG2Ks71vu4nUUeyCfY2rjsfrqirFSSYBKHZVin"

    # Add the Minio connection
    conn = BaseHook.get_connection(conn_id=MINIO_CONN_ID)
    if conn is None:
        conn = Connection(
        conn_id=MINIO_CONN_ID,
        conn_type="S3",
        host=MINIO_HOST,
        login=MINIO_ACCESS_KEY,
        password=MINIO_SECRET_KEY,
        )
        conn.add_connection()
    else:
        raise AirflowException(f"Connection with ID {MINIO_CONN_ID} already exists.")

# Call the function to create the connection
create_minio_connection()
# DEFINE DEFAULT ARGS
default_args = {
    "owner": "João Leite",
    "retries": 0,
    "retry_delay": 0,
    "execution_timeout": timedelta(seconds=60 * 60 * 4),
}

@dag(
    start_date=datetime(2024, 3, 1), # definir quando for rodar automatico
    max_active_runs=1,
    schedule_interval='0 10 * * *',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'minio', 'mesa', 'variaveis', 'motor']
)
def sensor_test():
    # # Variaveis Conexão

    # Conectando na trusted
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='teste-vini',
        bucket_key='data.csv',
        aws_conn_id="minio_default"
    )
    
    task1 

sensor_test()