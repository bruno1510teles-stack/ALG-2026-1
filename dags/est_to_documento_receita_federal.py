# IMPORT LIBS
from datetime import datetime, timedelta, timezone

# AIRFLOW LIBS
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.models import Variable

# SCRIPTS
from scripts.receita_federal.estabelecimento_to_documento_pessoas_e_organizacoes import transform_estabelecimento_receita_to_documento

# DEFINE VARIABLES
MINIO_CONN_RAW = "minio_raw"
MINIO_RAW_BUCKET = "receita-federal"
RECEITA_FEDERAL_ESTABELECIMENTOS_FOLDER = "estabelecimento/"

now = datetime.now(tz=timezone(timedelta(hours=-3)))
#yesterday = now - timedelta(day=1)
day_to_process_estabelecimento = f"{RECEITA_FEDERAL_ESTABELECIMENTOS_FOLDER}year=2024/month=2/" #f"{RECEITA_FEDERAL_ESTABELECIMENTOS_FOLDER}year={yesterday.year}/month={str(yesterday.month)}/"

# DEFINE FUNCTIONS
def check_files_to_processed(files_to_process):
    print(len(files_to_process))
    return len(files_to_process) > 0

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
    schedule_interval='0 10 * * *',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'minio', 'mesa', 'variaveis', 'motor']
)
def estabelecimento_to_documento_receita_federal():
    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    list_today_files_estabelecimento = S3ListOperator(
        task_id="list_today_files_estabelecimento",
        aws_conn_id=MINIO_CONN_RAW,
        bucket=MINIO_RAW_BUCKET,
        prefix=day_to_process_estabelecimento,
        apply_wildcard=True,
    )

    check_files = ShortCircuitOperator(
        task_id='check_files',
        python_callable=check_files_to_processed,
        provide_context=True,
        op_kwargs={'files_to_process': list_today_files_estabelecimento.output}
    )
    
    @task(
        executor_config={
        "KubernetesExecutor": {
            "request_memory": "8000Mi"
        }
    }
    )
    def estabelecimento_to_documento_receita_federal(current_files_estabelecimento):

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

        print(f"current_files_estabelecimento as { type(current_files_estabelecimento) } and size of { len(current_files_estabelecimento) }")

        transform_estabelecimento_receita_to_documento(current_files_estabelecimento, access_params)

    unique_clients = estabelecimento_to_documento_receita_federal(list_today_files_estabelecimento.output)

    # run order
    init_data_load >> list_today_files_estabelecimento >> check_files >> unique_clients >> finish_data_load

estabelecimento_to_documento_receita_federal()