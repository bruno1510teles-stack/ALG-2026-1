import pendulum
import trino
import os
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from io import BytesIO
from airflow.models import Variable
from minio.error import S3Error
from airflow_dags_core.lib.MinioWriteFile import MinioWriteFile
from airflow_dags_core.lib.MinioReadFile import MinioReadFile

default_args = {"owner": "Matheus Barbosa"}


@dag(
    default_args=default_args,
    start_date=pendulum.today("UTC").add(days=-1),
    schedule=None,
    description="DAG consulta trino e faz upload do arquivo para minio",
    catchup=False,
)
def anexa_grupo_economico():

    @task
    def consulta_trino_e_anexa_arquivo(**kwargs):
        cnpjRaiz = kwargs["dag_run"].conf.get("payer_identification", "default_value")
        bucket = kwargs["dag_run"].conf.get("minio_url", "default_value")

        trino_endpoint = Variable.get("TRINO_ENDPOINT")
        trino_port = Variable.get("TRINO_PORT")
        trino_username = Variable.get("TRINO_USER")
        trino_password = Variable.get("TRINO_PASSWORD")

        query = f"select cnpj_raiz, 'catalogo/'||arquivo arquivo from minioraw.credito_grupos.indice where cnpj_raiz = '{cnpjRaiz}'"

        retorno = consulta_trino(
            query=query,
            host=trino_endpoint,
            port=trino_port,
            user=trino_username,
            password=trino_password,
        )

        file_name = retorno[0][1]

        arquivo = baixa_arquivo_minio(file_name=file_name, bucket=bucket)

        if arquivo is not None:
            pdf_content = BytesIO(arquivo.read())
            file_name_without_path = os.path.basename(file_name)
            MinioWriteFile().write_file(
                file=pdf_content, file_name=file_name_without_path, bucket=bucket
            )
        else:
            print(f"Não foi encontrado o arquivo {file_name} para download")

    def baixa_arquivo_minio(file_name, bucket):
        try:
            return MinioReadFile().read_file(file_name=file_name, bucket=bucket)
        except S3Error as e:
            print(f"Erro ao interagir com o MinIO: {e}")
            raise
        except Exception as e:
            print(f"Erro inesperado: {e}")
            raise

    def consulta_trino(query, host, port, user, password):
        try:
            conn = trino.dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog="minioraw",
                http_scheme="https",
                auth=trino.auth.BasicAuthentication(user, password),
            )

            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()

            return result
        except trino.exceptions.TrinoQueryError as e:
            print(f"Erro ao executar a consulta: {e}")
            raise
        finally:
            conn.close()

    start = EmptyOperator(task_id="start")
    relato = consulta_trino_e_anexa_arquivo()
    end = EmptyOperator(task_id="end")

    start >> relato >> end


dag_instance = anexa_grupo_economico()
