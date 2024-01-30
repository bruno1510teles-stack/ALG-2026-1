# IMPORT LIBS
from datetime import datetime, timedelta, timezone

# AIRFLOW LIBS
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.models import Variable

# SCRIPTS
from scripts.hpex_raw_to_trusted import transform_data_to_trusted

# DEFINE VARIABLES
MINIO_CONN_RAW = "minio_raw"
MINIO_RAW_BUCKET = "hp-externa"
HPEX_RAW_FOLDER = "hp_externa/"

now = datetime.now(tz=timezone(timedelta(hours=-3)))

day_to_process = f"{HPEX_RAW_FOLDER}year={now.year}/month={now.month}/day={str(now.day).zfill(2)}/"

# DEFINE FUNCTIONS
def check_files_to_processed(files_to_process):
    return len(files_to_process) > 0

# DEFINE DEFAULT ARGS
default_args = {
    "owner": "Mayer",
    "retries": 2,
    "retry_delay": 0,
    "execution_timeout": timedelta(seconds=60 * 500),
}

# DEFINE DAG
@dag(
    start_date=datetime(2024, 1, 24), # definir quando for rodar automatico
    max_active_runs=1,
    schedule_interval=None, #'0 12 * * *',
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'minio', 'first_batch', 'HPEX']
)
def hpexterna_to_trusted():
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
    def transform_raw_to_trusted(current_files):

        access_params = {
            "endpoint_url_raw": Variable.get("MINIO_RAW_ENDPOINT"),
            "aws_access_key_id_raw": Variable.get("MINIO_RAW_ACCESS_KEY"),
            "aws_secret_access_key_raw": Variable.get("MINIO_RAW_SECRET_KEY"),
            "endpoint_url_trusted": Variable.get("MINIO_TRUSTED_ENDPOINT"),
            "aws_access_key_id_trusted": Variable.get("MINIO_TRUSTED_ACCESS_KEY"),
            "aws_secret_access_key_trusted": Variable.get("MINIO_TRUSTED_SECRET_KEY"),
            "trino_endpoint": Variable.get("TRINO_ENDPOINT"),
            "trino_port": Variable.get("TRINO_PORT"),
            "trino_user": Variable.get("TRINO_USER"),
            "trino_password": Variable.get("TRINO_PASSWORD"),
            "opdb_bucket": Variable.get("OPDB_BUCKET"),
            "stage": Variable.get('STAGE')
        }

        print(f"current_files as { type(current_files) } and size of { len(current_files) }")

        transform_data_to_trusted(current_files, access_params)

    unique_clients = transform_raw_to_trusted(list_today_files.output)

    # run order
    init_data_load >> list_today_files >> check_files >> unique_clients >> finish_data_load

hpexterna_to_trusted()