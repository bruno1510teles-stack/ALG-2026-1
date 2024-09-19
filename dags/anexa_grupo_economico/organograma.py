import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import requests
import time
import json
from io import BytesIO
from airflow.models import Variable
from airflow_dags_core.lib.auth_trino import get_acess_token
from scripts.utils import extract_path_from_url
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
        
        trino_endpoint = Variable.get("TRINO_ENDPOINT")
        trino_port = Variable.get("TRINO_PORT")
        url = f"https://{trino_endpoint}:{trino_port}/v1/statement"

        body = f"select cnpj_raiz, 'catalogo/'||arquivo arquivo from minioraw.credito_grupos.indice where cnpj_raiz = '{sacado[:8]}'"

        body_bytes = body.encode('utf-8')

        auth = get_acess_token()

        headers = {
            "Content-Type": "text/plain",
        }

        response = requests.post(url, auth=auth, data=body_bytes, headers=headers)

        status_requisicao = ''

        content_trino = json.loads(response.content)

        while status_requisicao != 'FINISHED' and status_requisicao != 'FAILED' and status_requisicao != 'RUNNING':
            time.sleep(1)
            response_next_uri = requests.get(content_trino['nextUri'], auth=auth)
            print(response_next_uri.text)
            content_trino = json.loads(response_next_uri.content)
            status_requisicao = content_trino["stats"]["state"]

        data = json.loads(response_next_uri.content)

        catalogos = monta_objeto_arquivo_para_salvar(data['data'])

        arquivo = baixa_arquivo_minio(catalogos=catalogos, bucket=bucket)

        pdf_content = BytesIO(arquivo.read())

        MinioWriteFile().write_file(file=pdf_content, 
                                  file_name=catalogos[1]['arquivo'], 
                                  bucket=bucket, 
                                  type = "application/pdf")
        MinioIndex.save(key= issueKey, identification= sacado, path= bucket + f"/{catalogos[1]['arquivo']}", fileType= "ORGANOGRAMA")

    def monta_objeto_arquivo_para_salvar(data):
        catalogos = []

        for dado in data[0]:
            catalogos.append({'arquivo': dado})

        return catalogos

    def baixa_arquivo_minio(catalogos, bucket):
        try:
            return MinioReadFile().read_file(file_name=catalogos[1]['arquivo'],
                                    bucket=bucket)    
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