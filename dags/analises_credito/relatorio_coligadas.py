import csv
from datetime import datetime
import io
from airflow_dags_core.lib.MinioWriteFile import MinioWriteFile
from airflow_dags_core.lib.MinioSaveIndex import MinioSaveIndex
from scripts.utils import get_trino_connection, execute_query
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'Rafael Leite',
    'retries': 1,
}
def createPartition(conn, cnpjSacado):
                
    getPartition = f"select 1 from minioraw.analise_credito_pcc.limite_csv where documento_sacado = '{cnpjSacado}' limit 1"

    partition = execute_query(conn=conn, query= getPartition)

    if not partition:
        
        print(f"Partição não encontrada, criando diretório para {cnpjSacado}")
        
        prepareQuery = f"""prepare cria_particao from CALL minioraw.system.create_empty_partition(schema_name => 'analise_credito_pcc', table_name => 'limite_csv', partition_columns => ARRAY['documento_sacado'], partition_values => ARRAY['{cnpjSacado}'])"""

        execute_query(conn=conn, query=prepareQuery)

        execute_query(conn=conn, query="execute cria_particao")

        print(f"Partição criada para cnpj {cnpjSacado}")

    else:
        print(f"Partição para {cnpjSacado} já existe.")

def generate_csv(rows, cnpj_sacado, issue_key):
    output = io.BytesIO()
    text_wrapper = io.TextIOWrapper(output, encoding='utf-8', newline='')

    writer = csv.writer(text_wrapper, delimiter=";")
    
    for row in rows:
        writer.writerow([
            row[0], row[1], row[2], row[3], row[4], row[5],
            row[6], row[7], row[8], row[9], row[10], issue_key, str(datetime.now()).replace(' ', 'T')[:-3], cnpj_sacado
        ])
    
    text_wrapper.flush()
    text_wrapper.detach()

    output.seek(0)
    MinioWriteFile().write_file(file=output, file_name= f"{issue_key}.csv", bucket=f"analise-credito/pcc/limite/csv/documento_sacado={cnpj_sacado}/", type = "text/csv")
    print("minio write file csv")
    output.close()

@dag(
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    description='DAG consulta o limite das empresas coligadas e envia o arquivo retornado para o MinIO',
    catchup=False,
)
def relatorio_coligadas():
    @task()
    def start_task():
        print("Operação iniciada")

    @task()
    def consulta_limites(**kwargs):
        conn = get_trino_connection()
        try:
            issueKey = kwargs['dag_run'].conf.get('issue_key', 'default_value')
            cnpjSacado = kwargs['dag_run'].conf.get('payer_identification', 'default_value')
            if not cnpjSacado:
                raise ValueError("CNPJ não fornecido na execução da DAG.")
            
            queryLimite = f"""
            with org_id as (
                select max(o.id) id, o.cnpj_raiz, trim(o.razao_social) razao_social, 
                case when rr.id is not null then 'S' else 'N' end indicador_restricao
                from deltalaketrusted.serasa.organizacoes o 
                    left join deltalaketrusted.serasa.resumo_restritivo rr on rr.id = o.id and rr.titular_pendencia= o.cnpj_raiz
                where o.cnpj_raiz = '{cnpjSacado[:8]}'
                group by o.cnpj_raiz, trim(o.razao_social), case when rr.id is not null then 'S' else 'N' end
            )
            ,coligadas as (
                select c.id, substring(c.documento,2 , 9) cnpj_raiz, c.nome, c.indicador_restricao
                from deltalaketrusted.serasa.coligada c 
                where c.identificacao_pessoa = 'J' and c.id = (select id from org_id)
            )
            ,socios as (
                select id, substring(s.documento_socio,2 , 9) cnpj_raiz, s.nome_socio, s.indicador_restricao
                from deltalaketrusted.serasa.socios s 
                where s.identificacao_pessoa = 'J' and s.id = (select id from org_id)
            )
            ,empresas as (
                select 1 ordem, 'Sacado' tipo, * from org_id
                union all
                select 3, 'Coligada', * from coligadas
                union all
                select 2, 'Sócio', * from socios
            )
            ,configs as ( 
                select 
                    e.tipo, e.cnpj_raiz, e.razao_social, e.indicador_restricao, e.ordem
                    ,pc.chave 
                    ,coalesce(lc_s.id, lc_a.id) id
                    ,coalesce(lc_s.categoria_limite, lc_a.categoria_limite) categoria_limite
                    ,lc_s.participante_chave_cedente_id
                from empresas e 
                    left join postgres.ccred_schema_{Variable.get('STAGE')}_default.participante_chave pc on pc.chave = e.cnpj_raiz 
                    left join postgres.ccred_schema_{Variable.get('STAGE')}_default.limite_config lc_s on lc_s.participante_chave_sacado_id = pc.id and lc_s.categoria_limite = 'SEGREGADO'
                    left join postgres.ccred_schema_{Variable.get('STAGE')}_default.limite_config lc_a on lc_a.participante_chave_sacado_id = pc.id 
                        and lc_a.categoria_limite = 'ATRIBUIDO' and lc_s.id is null
            )
            select 
                case 
                    when pl.limite_atribuido is not null then 'S'
                    else 'N'
                end as "has_limit",
                c.tipo, c.cnpj_raiz, c.razao_social,
                c.indicador_restricao,  
                cast(pl.limite_atribuido as double) as "limite_atribuido", cast(pl.limite_disponivel as double) as "limite_disponivel", cast(pl.total_vencido as double) as "total_vencido", pl.status,  c.categoria_limite, pc2.chave "fn"
            from configs c
                left join postgres.ccred_schema_{Variable.get('STAGE')}_default.participante_limite pl on pl.limite_config_id = c.id
                left join postgres.ccred_schema_{Variable.get('STAGE')}_default.participante_chave pc2 on pc2.id = c.participante_chave_cedente_id
            order by c.ordem, c.cnpj_raiz
            """
            
            rows = execute_query(conn, queryLimite)

            if not rows:
                return print(f"Não houve retorno para o sacado {cnpjSacado}")
            
            
            createPartition(conn, cnpjSacado)
            generate_csv(rows=rows, cnpj_sacado=cnpjSacado, issue_key=issueKey)

        finally:
            conn.close()
        

        print("csv gerado")
        MinioSaveIndex().save(key= issueKey, identification=cnpjSacado, path= f"analise-credito/pcc/limite/csv/documento={cnpjSacado}/{issueKey}.csv", file=None, fileType= "COLIGADAS")
        print("minio save index")

    @task()
    def end_task():
        print("Operação finalizada")

    start = start_task()
    limites = consulta_limites()
    end = end_task()

    start >> limites >> end 

coligadas_dag = relatorio_coligadas()