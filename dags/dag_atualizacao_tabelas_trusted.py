# IMPORT LIBS
from datetime import datetime, timedelta, timezone

# AIRFLOW LIBS
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.models import Variable
from scripts.query_trino_atualizar_tabelas import query_trino

# DEFINE VARIABLES
tabelas_atualizar = [
    "call miniotrusted.system.sync_partition_metadata('payments', 'boletos', 'ADD', false)",	
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'cnae', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'contato', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'documento', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'endereco', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'natureza_juridica', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'organizacoes', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'pep', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'relacionamento', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'situacao_cadastral', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('risco', 'mesa', 'ADD', false)",
    "call miniotrusted.system.sync_partition_metadata('risco', 'motor', 'ADD', false)"
]

access_params = {     
            "endpoint_url_raw": Variable.get("MINIO_RAW_ENDPOINT"),
            "aws_access_key_id_raw": Variable.get("MINIO_RAW_ACCESS_KEY"),
            "aws_secret_access_key_raw": Variable.get("MINIO_RAW_SECRET_KEY"),     
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
    schedule_interval='0 10 * * *',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'minio', 'call_trino']
)
def atualizacao_tabelas_trusted():
    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    
    @task()
    def atualizar_tabelas():
        
        tamanho = len(tabelas_atualizar)
        print(f'CALLS A FAZER: {tamanho}')
        
        i = 1
        for call in tabelas_atualizar:
            print(f"calls feitas: {i} de {tamanho}")   
            query = f"""{call}"""
                        
            print(f"query: {query}")
            
            query_trino(query, 
                        catalog = 'miniotrusted',                    
                        host = access_params['trino_endpoint'],
                        port = access_params['trino_port'],
                        user = access_params['trino_user'],
                        password = access_params['trino_password'])

    atualizar_tabelas_task = atualizar_tabelas()

    # run order
    init_data_load >> atualizar_tabelas_task >> finish_data_load

atualizacao_tabelas_trusted()