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
    "call miniotrusted.system.sync_partition_metadata('payments', 'boletos', 'ADD', false);",	
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'cnae', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'contato', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'documento', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'endereco', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'natureza_juridica', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'organizacoes', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'pep', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'relacionamento', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('pessoas_e_organizacoes', 'situacao_cadastral', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('risco', 'mesa', 'ADD', false);",
    "call miniotrusted.system.sync_partition_metadata('risco', 'motor', 'ADD', false);"
]


# DEFINE FUNCTIONS
def atualizar_tabelas():
    query = f""" WITH CTE AS (
        SELECT 
            substring(documento, 1, 8) documento_raiz,
            razao_social,
            numero_titulo,
            data_emissao,
            data_vencimento,
            data_pagamento,
            valor_titulo,
            data_hp,
            numero_parcela,
            fornecedor,
            fonte,
            atualizado_em,
            tipo_documento,
            year,
            month,
            day,
            ROW_NUMBER() OVER (PARTITION BY documento, numero_titulo, data_emissao, data_vencimento, fonte, fornecedor ORDER BY year DESC, month DESC, day DESC) AS rn
        FROM miniotrusted.payments.boletos 
        WHERE substring(documento, 1, 8) IN {ids_query} AND fonte = 'HP_EXTERNA' and tipo_documento = 'CNPJ'
            )
            SELECT 
                *
            FROM CTE
            WHERE rn = 1"""
        
        print(f'quantidade de CNPJs a serem atualziados: {len(ids_query)}')
        print(f"query: {query}")
        
        query_trino(query, 
                                    access_params['trino_endpoint'],
                                    access_params['trino_port'],
                                    access_params['trino_user'],
                                    access_params['trino_password'])

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

        print(f"current_files as { type(current_files) } and size of { len(current_files) }")

        transform_pep_to_trusted(current_files, access_params)

    unique_clients = pep_transform_raw_to_trusted(list_today_files.output)

    # run order
    init_data_load >> list_today_files >> check_files >> unique_clients >> finish_data_load

pep()