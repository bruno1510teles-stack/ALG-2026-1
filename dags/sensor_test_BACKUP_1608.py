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
<<<<<<< HEAD

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
=======
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
        conn.add_to_session(BaseHook.get_connection(session=None))
        print(f"Minio connection with ID {MINIO_CONN_ID} created successfully.")
    else:
        print(f"Minio connection with ID {MINIO_CONN_ID} already exists.")
        
# Call the function to create the connection
create_minio_connection()
>>>>>>> parent of 8b9c20d (recomeçando)
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
<<<<<<< HEAD
    # BUCKET_SOURCE_RAW = "receita-federal"
    # BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    # TRUSTED_FOLDER = "relacionamento/"
    print(access_params['endpoint_url_raw'])
    # Conectando na trusted
    storage_options = {
        "aws_access_key_id": access_params['aws_access_key_id_raw'],
        "aws_secret_access_key": access_params['aws_secret_access_key_raw'],
        "host":f"https://{access_params['endpoint_url_raw']}"
        # "AWS_REGION": "us-east-1",
        # "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
=======

    # Conectando na trusted
>>>>>>> parent of 8b9c20d (recomeçando)
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='teste-vini',
        bucket_key='data.csv',
<<<<<<< HEAD
        aws_conn_id=storage_options
    
=======
        aws_conn_id="minio_default"
>>>>>>> parent of 8b9c20d (recomeçando)
    )
    
    task1 

sensor_test()