import csv
from datetime import datetime
import io
from airflow_dags_core.lib.MinioWriteFile import MinioWriteFile
from processos_antigos.scripts_diversos.utils import get_trino_connection, execute_query
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Rafael Leite',
    'retries': 1,
}
def createPartition(conn, cnpjSacado):
                
    getPartition = f"select 1 from minioraw.analise_credito_pcc.ata where documento_sacado = '{cnpjSacado}' limit 1"

    partition = execute_query(conn=conn, query= getPartition)

    if not partition:
        
        print(f"Partição não encontrada, criando diretório para {cnpjSacado}")
        
        prepareQuery = f"""prepare cria_particao_ata from CALL minioraw.system.create_empty_partition(schema_name => 'analise_credito_pcc', table_name => 'ata', partition_columns => ARRAY['documento_sacado'], partition_values => ARRAY['{cnpjSacado}'])"""

        execute_query(conn=conn, query=prepareQuery)

        execute_query(conn=conn, query="execute cria_particao_ata")

        print(f"Partição criada para cnpj {cnpjSacado}")

    else:
        print(f"Partição para {cnpjSacado} já existe.")

def generate_csv(cnpj, key, pgid, nome, responsavel, limite_sugerido, limite_aprovado, status, tipo, parecer, data_parecer, responsavel_parecer, resolucao, data_issue):
    output = io.BytesIO()
    text_wrapper = io.TextIOWrapper(output, encoding='utf-8', newline='')

    writer = csv.writer(text_wrapper, delimiter=";")
    
    writer.writerow([key, pgid, nome, responsavel, limite_sugerido, limite_aprovado, status, resolucao, tipo, parecer, data_parecer, responsavel_parecer, data_issue, str(datetime.now()), cnpj])
    
    text_wrapper.flush()
    text_wrapper.detach()

    output.seek(0)
    MinioWriteFile().write_file(file=output, file_name= f"{tipo}-{key}.csv", bucket=f"analise-credito/pcc/ata/documento_sacado={cnpj}/", type = "text/csv")
    print("minio write file csv")
    output.close()

def update_parecer_comercial(conn, cnpj, key, pgid, nome, responsavel, limite_sugerido, limite_aprovado, status, resolucao, data_issue):
    print("Atualizando parecer do comercial")
    try:
        query = f"""SELECT 
                        parecer, 
                        data_parecer,
                        responsavel_parecer
                    FROM
                        minioraw.analise_credito_pcc.ata
                    WHERE 
                        tipo = 'comercial' 
                    AND 
                        documento_sacado = '{cnpj}' 
                    AND 
                        issue_key = '{key}' limit 1"""
        
        result = execute_query(conn, query)
        if not result:
            print("Não há registro de parecer do comercial")
            return
        
        generate_csv(cnpj, key, pgid, nome, responsavel, limite_sugerido, limite_aprovado, status, 'comercial', result[0][0], result[0][1], result[0][2], resolucao, data_issue)
        print("parecer comercial atualizado com sucesso")
    except Exception as e:
        print(f"ERRO: {e}")


@dag(
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    description='DAG consulta o limite das empresas coligadas e envia o arquivo retornado para o MinIO',
    catchup=False,
)
def salva_parecer():

    @task()
    def processa_parecer(**kwargs):
        conn = get_trino_connection()
        try:
            cnpj = kwargs['dag_run'].conf.get('cnpj', None)
            key = kwargs['dag_run'].conf.get('key', None)
            pgid = kwargs['dag_run'].conf.get('pgid', None)
            nome = kwargs['dag_run'].conf.get('nome', None)
            responsavel = kwargs['dag_run'].conf.get('responsavel', None)
            limite_sugerido = kwargs['dag_run'].conf.get('limite_sugerido', None)
            limite_aprovado = kwargs['dag_run'].conf.get('limite_aprovado', None)
            status = kwargs['dag_run'].conf.get('status', None)
            parecer = kwargs['dag_run'].conf.get('parecer', None)
            tipo = kwargs['dag_run'].conf.get('tipo', None).lower().replace('parecer ', '').capitalize()
            responsavel_parecer = kwargs['dag_run'].conf.get('responsavel_parecer', None)
            resolucao = kwargs['dag_run'].conf.get('resolucao', None)
            data_parecer = kwargs['dag_run'].conf.get('data_parecer', None)
            data_issue = kwargs['dag_run'].conf.get('data_issue', None)
            
            createPartition(conn, cnpj)
            generate_csv(cnpj, key, pgid, nome, responsavel, limite_sugerido, limite_aprovado, status, tipo, parecer, data_parecer, responsavel_parecer, resolucao, data_issue)
            if tipo != 'comercial':
                update_parecer_comercial(conn, cnpj, key, pgid, nome, responsavel, limite_sugerido, limite_aprovado, status, resolucao, data_issue)

        finally:
            conn.close()
        
        print("csv gerado")

    parecer = processa_parecer()

    parecer

parecer_dag = salva_parecer()