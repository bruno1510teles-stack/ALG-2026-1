import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from politica_e_modelagem.pre_filtro.pre_filtro_comun import analise_pre_filtro

### Parâmetros de acesso
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
    "kafka_url": Variable.get('KAFKA_DATALAKE_ENDPOINT'),
    "exrep_url": Variable.get('EXREP_BASE_URL'),
    "exrep_client_id": Variable.get('EXREP_CLIENT_ID'),
    "exrep_client_secret": Variable.get('EXREP_CLIENT_SECRET'),
    "keycloack_token_url": Variable.get('KEYCLOAK_TOKEN_URL'),
    "jira_url": Variable.get('JIRA_API_URL'),
    "jira_api_token": Variable.get('JIRA_API_TOKEN'),
    "jira_api_user": Variable.get('JIRA_API_USER'),
    "jira_project": Variable.get('JIRA_PROJECT')
    }

### Definindo defaults
default_args = {
    "owner": "Felipe Ferraz",
    "retries": 0,
}

# Definindo a DAG
with DAG(
    dag_id='pre_filtro',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    catchup=False,
    tags=['pre-filtro', 'proposta-negocio'],
    description="Pre filtro das propostas de negócio independente da política",
    max_active_tasks=1
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    prefiltro_task =  PythonOperator(
            task_id="pre_filtro_comun_task",
            python_callable=analise_pre_filtro,
            op_kwargs={'access_params': access_params},
        )
    
    # Definindo a ordem de execução das tasks
    start >> prefiltro_task >> end