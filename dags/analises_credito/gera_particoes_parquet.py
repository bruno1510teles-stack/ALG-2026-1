import csv
from datetime import datetime
import io
from airflow_dags_core.lib.MinioWriteFile import MinioWriteFile
from airflow_dags_core.lib.MinioReadFile import MinioReadFile
from processos_antigos.scripts_diversos.utils import get_trino_connection, execute_query
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'Rafael Leite',
    'retries': 1,
}

def createPartition(conn, cnpjSacado):
    prepareQuery = f"""prepare cria_particao_ata from CALL minioraw.system.create_empty_partition(
        schema_name => 'analise_credito_pcc',
        table_name => 'ata_particionada', 
        partition_columns => ARRAY['raiz_sacado'], 
        partition_values => ARRAY['{cnpjSacado}'])"""
    execute_query(conn=conn, query=prepareQuery)
    execute_query(conn=conn, query="execute cria_particao_ata")

@dag(
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    description='DAG consulta o limite das empresas coligadas e envia o arquivo retornado para o MinIO',
    catchup=False
)
def gera_particoes_parquet():

    @task()
    def cria_particoes(**kwargs):
        erro_list = []  # Lista para armazenar os erros
        try:
            conn = get_trino_connection()
            file = MinioReadFile().read_file(file_name="pcc/input/cnpjs_unicos.txt", bucket="analise-credito/")

            lista = file.read().decode('UTF-8').split(";")
            count = 0
            for cnpj in lista:
                count += 1
                try:
                    createPartition(conn, cnpj.replace('"', ''))
                    print(f"Partição criada com sucesso para CNPJ: {cnpj}")
                except Exception as e:
                    error_message = str(e)
                    # Ignora erros de partição já existente
                    if "partition already exists" not in error_message.lower():
                        erro_list.append({"cnpj": cnpj, "erro": error_message})
                    print(f"Erro ao criar partição para CNPJ {cnpj}: {error_message}")
        finally:
            conn.close()
        
        # Exibe a lista de erros ao final
        if erro_list:
            print("Erros encontrados durante a criação das partições:")
            for erro in erro_list:
                print(f"CNPJ: {erro['cnpj']} - Erro: {erro['erro']}")
        else:
            print("Nenhum erro encontrado durante a criação das partições.")

        print("Parquet gerado")

    cria_particoes()


parecer_dag = gera_particoes_parquet()
