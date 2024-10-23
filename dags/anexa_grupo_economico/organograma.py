import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import requests
import time
import json
from io import BytesIO
from scripts.utils import extract_path_from_url, get_trino_connection, execute_query
from minio.error import S3Error
from airflow_dags_core.lib.MinioWriteFile import MinioWriteFile
from airflow_dags_core.lib.MinioSaveIndex import MinioSaveIndex
from airflow_dags_core.lib.MinioReadFile import MinioReadFile

default_args = {
    'owner': 'Matheus Barbosa'
}

@dag(
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule= None,
    description='DAG consulta trino e faz upload do arquivo para minio',
    catchup=False,
)
def anexa_grupo_economico():

    @task
    def consulta_trino_e_anexa_arquivo(**kwargs):
        sacado = kwargs['dag_run'].conf.get('payer_identification', 'default_value')
        bucket = extract_path_from_url(kwargs['dag_run'].conf.get('minio_url', 'default_value'))
        issueKey = kwargs['dag_run'].conf.get('issue_key', 'default_value')
        MinioIndex = MinioSaveIndex()

        connection = get_trino_connection()


        query = f"select cnpj_raiz, arquivo from minioraw.credito_grupos.indice where cnpj_raiz = '{sacado[:8]}'"

        rows = execute_query(connection, query)

        if not rows:
            print(f"Nehum arquivo encontrado para o sacado: {sacado[:8]}")
            return

        catalogos = get_catalogos(rows)

        for catalogo in catalogos:
            arquivo = baixa_arquivo_minio(catalogo)

            pdf_content = BytesIO(arquivo.read())

            MinioWriteFile().write_file(file=pdf_content, 
                                    file_name=catalogo, 
                                    bucket=bucket)
            MinioIndex.save(key= issueKey, identification= sacado, path= bucket + f"{catalogo}", fileType= "ORGANOGRAMA")

    def get_catalogos(data):
        catalogos = []

        for dado in data:
            catalogos.append(dado[1])

        return catalogos

    def baixa_arquivo_minio(catalogo):
        try:
            return MinioReadFile().read_file(file_name="catalogo/" + catalogo,
                                    bucket="credito-grupos/")    
        except S3Error as e:
            print(f"Erro ao interagir com o MinIO: {e}")
            raise
        except Exception as e:
            print(f"Erro inesperado: {e}") 
            raise

    start = EmptyOperator(task_id="start")
    relato = consulta_trino_e_anexa_arquivo()
    end = EmptyOperator(task_id="end")

    start >> relato >> end

dag_instance = anexa_grupo_economico()