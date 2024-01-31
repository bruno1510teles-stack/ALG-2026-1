# IMPORT LIBS
from datetime import datetime, timedelta, timezone

# AIRFLOW LIBS
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.models import Variable

# SCRIPTS
from scripts.mesa_raw_to_trusted import transform_data_to_trusted

# DEFINE VARIABLES
MINIO_CONN_RAW = "minio_raw"
MINIO_RAW_BUCKET = Variable.get("OPDB_BUCKET")
RESPONSE_MESA_RAW_FOLDER = f"topics/opdb.inrp_{ Variable.get('STAGE') }_default.jira_issue/"

now_tz = datetime.now(tz=timezone(timedelta(hours=-3)))
yesterday = now_tz - timedelta(days=1)

day_to_process = f"{RESPONSE_MESA_RAW_FOLDER}year={yesterday.year}/month={str(yesterday.month).zfill(2)}/day={str(yesterday.day).zfill(2)}/"

# DEFINE FUNCTIONS
def check_files_to_processed(files_to_process):
    print(len(files_to_process))
    return len(files_to_process) > 0

# DEFINE DEFAULT ARGS
default_args = {
    "owner": "Mayer",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(seconds=60 * 50),
}

# DEFINE DAG
@dag(
    start_date=datetime(2024, 1, 31), # definir quando for rodar automatico
    max_active_runs=1,
    schedule_interval=None, #'0 9 * * *',
    default_args=default_args,
    catchup=True,
    tags=['development', 'elt', 'minio', 'mesa', 'motor v1']
)
def mesa_credito():
    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    mesa_files = S3ListOperator(
        task_id="mesa_files",
        aws_conn_id=MINIO_CONN_RAW,
        bucket=MINIO_RAW_BUCKET,
        prefix=day_to_process,
        apply_wildcard=True,
    )

    check_files = ShortCircuitOperator(
        task_id='check_files',
        python_callable=check_files_to_processed,
        provide_context=True,
        op_kwargs={'files_to_process': mesa_files.output}
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
    
        print(f"files_to_process: {current_files}") 
        transform_data_to_trusted(current_files, access_params)

        return current_files

    files_at_trusted = transform_raw_to_trusted(mesa_files.output)

    # run order
    init_data_load >> mesa_files >> check_files >> files_at_trusted >> finish_data_load

mesa_credito()