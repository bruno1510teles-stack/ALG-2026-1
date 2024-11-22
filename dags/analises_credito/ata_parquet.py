import csv
from datetime import datetime
import io
from airflow_dags_core.lib.MinioWriteFile import MinioWriteFile
from airflow_dags_core.lib.MinioReadFile import MinioReadFile
from scripts.utils import get_trino_connection, execute_query
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'Rafael Leite',
    'retries': 1,
}

def createPartition(conn, raizSacado):
    getPartition = f"select 1 from minioraw.analise_credito_pcc.ata_particionada where raiz_sacado = '{raizSacado}' limit 1"
    partition = execute_query(conn=conn, query=getPartition)

    if not partition:
        try:
            print(f"Partição não encontrada, criando diretório para {raizSacado}")
            prepareQuery = f"""prepare cria_particao_ata from CALL minioraw.system.create_empty_partition(
                schema_name => 'analise_credito_pcc', 
                table_name => 'ata_particionada', 
                partition_columns => ARRAY['raiz_sacado'], 
                partition_values => ARRAY['{raizSacado}'])"""
            execute_query(conn=conn, query=prepareQuery)
            execute_query(conn=conn, query="execute cria_particao_ata")
            print(f"Partição criada para cnpj {raizSacado}")

        except Exception as e:
            print("ERRO: ", e)
            
        return False
    else:
        print(f"Partição para {raizSacado} já existe.")
    return True

def generate_parquet(dados, append):
    dados = [dados]
    colunas = [
        "issue_key", 
        "pgid", 
        "razao_social_sacado",
        "fornecedor", 
        "limite_sugerido", 
        "limite_aprovado",     
        "resolucao", 
        "status", 
        "parecer", 
        "tipo",
        "responsavel", 
        "data_parecer", 
        "responsavel_resolution",
        "data_issue", 
        "data_atualizacao", 
        "documento_sacado", 
        "raiz_sacado"
    ]

    if append:
        file_name = f"pcc/ata-particionada/raiz_sacado={dados[0][16]}/{dados[0][16]}.parquet"
        bucket = f"analise-credito/"
        existing_file_content = MinioReadFile().read_file(file_name=file_name, bucket=bucket)
        existing_df = pd.read_parquet(io.BytesIO(existing_file_content.read()))
        new_df = pd.DataFrame(dados, columns=colunas)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        f = io.BytesIO()
        combined_df.to_parquet(f)
        f.seek(0)
        MinioWriteFile().write_file(file=f, file_name=file_name, bucket=bucket)
        f.close()
        
    else:
        df = pd.DataFrame(dados, columns=colunas)
        f = io.BytesIO()
        df.to_parquet(f)
        f.seek(0)
        MinioWriteFile().write_file(file=f, file_name=f"{dados[0][16]}.parquet", bucket=f"analise-credito/pcc/ata-particionada/raiz_sacado={dados[0][16]}/")
        f.close()

    # append consolidado
    try:
        file_name = f"pcc/ata-consolidada/{datetime.now().strftime('%Y-%m-%d')}.parquet"
        bucket = f"analise-credito/"
        existing_file_content = MinioReadFile().read_file(file_name=file_name, bucket=bucket)

        if existing_file_content:
            existing_df = pd.read_parquet(io.BytesIO(existing_file_content.read()))
            print("existing_df: ", existing_df)
            new_df = pd.DataFrame(dados, columns=colunas)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)

            f = io.BytesIO()
            combined_df.to_parquet(f)
            f.seek(0)
            MinioWriteFile().write_file(file=f, file_name=file_name, bucket=bucket)
            f.close()
        else:            
            print(f"WARNING: O arquivo especificado não existe no MinIO. \nWARNING: Gerando arquivo para o dia {datetime.now().strftime('%Y-%m-%d')}")
            df = pd.DataFrame(dados, columns=colunas)
            f = io.BytesIO()
            df.to_parquet(f)
            f.seek(0)
            MinioWriteFile().write_file(file=f, file_name=f"{datetime.now().strftime('%Y-%m-%d')}.parquet", bucket=f"analise-credito/pcc/ata-consolidada/")
            f.close()

    except Exception as e:
        print("Erro:", e)



@dag(
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    description='DAG consulta o limite das empresas coligadas e envia o arquivo retornado para o MinIO',
    catchup=False
)
def ata_parquet():

    @task()
    def processa_parecer(**kwargs):
        try:
            conn = get_trino_connection()
            
            # Extrair os parâmetros de trigger
            json = kwargs['dag_run'].conf.get('content', None)
            if not json:
                print("ERRO: Parâmetro de entrada vazio")
                exit()

            issue_key = json['issue']['key']
            pgid = json['issue']['fields']['customfield_13739']
            razao_social_sacado = json['issue']['fields']['customfield_13716']
            razao_social_fornecedor = json['issue']['fields']['customfield_13719']
            limite_sugerido = json['issue']['fields']['customfield_13730']

            if limite_sugerido:
                limite_sugerido = float(limite_sugerido.replace(' ', '').replace('R$', '').replace('.', '').replace(',', '.'))
            else:
                limite_sugerido = 0.0
                
            limite_aprovado = float(json['issue']['fields']['customfield_13709'])

            if limite_aprovado:
                limite_aprovado = float(limite_aprovado)
            else:
                limite_aprovado = 0.0
                
            resolucao = json['issue']['fields']['resolution']['name']
            status = json['issue']['fields']['status']['name']
            parecer_final = json['issue']['fields']['customfield_13753']
            parecer_comercial = json['issue']['fields']['customfield_13819']
            responsavel = json['user']['displayName']
            data_parecer = json['issue']['fields']['created']
            responsavel_resolution = json['user']['displayName']
            data_issue = json['issue']['fields']['created']
            data_atualizacao = datetime.now()
            documento_sacado = json['issue']['fields']['customfield_13729']
            raiz_sacado = json['issue']['fields']['customfield_13729'][:8]

            parecer_comercial = [
                issue_key,
                pgid,
                razao_social_sacado,
                razao_social_fornecedor,
                limite_sugerido,
                limite_aprovado,
                resolucao,
                status,
                parecer_comercial,                                  #parecer comercial
                'comercial',                                        #tipo do parecer
                responsavel,
                data_issue,                                         #data do parecer
                "",                                               #parecer resolution
                data_issue,
                data_atualizacao,
                documento_sacado,
                raiz_sacado]
            
            parecer_final = [
                issue_key,
                pgid,
                razao_social_sacado,
                razao_social_fornecedor,
                limite_sugerido,
                limite_aprovado,
                resolucao,
                status,
                parecer_final,                                      #parecer final
                'final',                                            #tipo do parecer
                responsavel,
                data_parecer,                                       #data do parecer
                responsavel_resolution,
                data_issue,
                data_atualizacao,
                documento_sacado,
                raiz_sacado]

            dados = [parecer_final, parecer_comercial]

            for parecer in dados:
                for i in range(len(parecer)):
                    if not parecer[i] and parecer[i] != 0.0:
                        print("valor null encontrado")
                        parecer[i] = ""

            
            for parecer in dados:
                for valor in parecer:
                    print(valor)
            
            for parecer in dados:
                append = createPartition(conn, raiz_sacado)
                generate_parquet(parecer, append)

        finally:
            conn.close()
        
        print("parquet gerado")

    processa_parecer()

parecer_dag = ata_parquet()
