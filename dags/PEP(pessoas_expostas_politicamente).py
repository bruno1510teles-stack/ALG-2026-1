# IMPORT LIBS
from datetime import datetime, timedelta, timezone

# AIRFLOW LIBS
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.models import Variable

# SCRIPTS
from scripts.pep.pessoas_expostas_politicamente import transform_pep_to_trusted

# DEFINE VARIABLES
MINIO_CONN_RAW = "minio_raw"
MINIO_RAW_BUCKET = "pessoas-expostas-politicamente"
BOLETOS_ALPE_TRUSTED_FOLDER = "pep/"

now = datetime.now(tz=timezone(timedelta(hours=-3)))
yesterday = now - timedelta(days=1)
day_to_process = f"{BOLETOS_ALPE_TRUSTED_FOLDER}year={yesterday.year}/month={yesterday.month}/day={yesterday.day}/"


# DEFINE FUNCTIONS
def check_files_to_processed(files_to_process):
    print(len(files_to_process))
    return len(files_to_process) > 0

# DEFINE DEFAULT ARGS
default_args = {
    "owner": "João Leite",
    "retries": 1,
    "retry_delay": 0,
    "execution_timeout": timedelta(seconds=60 * 50),
}

# DEFINE DAG
@dag(
    start_date=datetime(2024, 3, 1), # definir quando for rodar automatico
    max_active_runs=1,
    schedule_interval='30 11 * * *',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'minio', 'PEP']
)
def pep():
    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    list_today_files = S3ListOperator(
        task_id="list_today_files",
        aws_conn_id=MINIO_CONN_RAW,
        bucket=MINIO_RAW_BUCKET,
        prefix=day_to_process,
        apply_wildcard=True,
    )

    check_files = ShortCircuitOperator(
        task_id='check_files',
        python_callable=check_files_to_processed,
        provide_context=True,
        op_kwargs={'files_to_process': list_today_files.output}
    )
    
    @task()
    def pep_transform_raw_to_trusted(current_files):

        access_params = {          
            "endpoint_url_trusted": Variable.get("MINIO_TRUSTED_ENDPOINT"),
            "aws_access_key_id_trusted": Variable.get("MINIO_TRUSTED_ACCESS_KEY"),
            "aws_secret_access_key_trusted": Variable.get("MINIO_TRUSTED_SECRET_KEY"),
            "endpoint_url_refined": Variable.get("MINIO_REFINED_ENDPOINT"),
            "aws_access_key_id_refined": Variable.get("MINIO_REFINED_ACCESS_KEY"),
            "aws_secret_access_key_refined": Variable.get("MINIO_REFINED_SECRET_KEY"),
            "trino_endpoint": Variable.get("TRINO_ENDPOINT"),
            "trino_port": Variable.get("TRINO_PORT"),
            "trino_user": Variable.get("TRINO_USER"),
            "trino_password": Variable.get("TRINO_PASSWORD"),
            "opdb_bucket": Variable.get("OPDB_BUCKET"),
            "stage": Variable.get('STAGE')
            	
	

        }

        print(f"current_files as { type(current_files) } and size of { len(current_files) }")

        transform_pep_to_trusted(current_files, access_params)

    unique_clients = pep_transform_raw_to_trusted(list_today_files.output)

    # run order
    init_data_load >> list_today_files >> check_files >> unique_clients >> finish_data_load

pep()