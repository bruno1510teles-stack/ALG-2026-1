import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import requests
import json
from io import BytesIO
from datetime import datetime, timedelta, timezone
from airflow_dags_core.lib.MinioWriteFile import MinioWriteFile
from airflow_dags_core.lib.MinioSaveIndex import MinioSaveIndex
from scripts.analises_credito.pcc.get_coordenadas import get_coordenadas
from gerar_pdf.auth import get_access_token
from dateutil import parser
from scripts.utils import extract_path_from_url, get_trino_connection, execute_query

default_args = {
    'owner': 'Matheus Barbosa',
    'retries': 1,
}

@dag(
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule= None,
    description='DAG consulta o relato serasa no exrep e envia o arquivo retornado para o minio',
    catchup=False,
)
def consulta_relato():

    @task
    def busca_relato(**kwargs):
        issueKey = kwargs['dag_run'].conf.get('issue_key', 'default_value')
        cnpj = kwargs['dag_run'].conf.get('payer_identification', 'default_value')
        bucket = extract_path_from_url(kwargs['dag_run'].conf.get('minio_url', 'default_value'))
        access_token = get_access_token()
        base_url = Variable.get('EXREP_BASE_URL')

        url_completo = f"{base_url}api/v1/report-executions?$sort=createdDate desc,lastModifiedDate desc&$expand=content&$filter=involved.party.identifications.value='{cnpj}' and definition.type=SERASA_RELATO and resolution=DONE&$audit=true"
        url_quadro_social = f"{base_url}api/v1/report-executions?$sort=createdDate desc,lastModifiedDate desc&$expand=content&$filter=involved.party.identifications.value='{cnpj}' and definition.type=SERASA_RELATO_QUADRO_SOCIAL and resolution=DONE&$audit=true"
        url_score = f"{base_url}api/v1/report-executions?$sort=createdDate desc,lastModifiedDate desc&$expand=content&$filter=involved.party.identifications.value='{cnpj}' and definition.type=SERASA_RELATO_SCORE and resolution=DONE&$audit=true"
        headers = {
            'Authorization': f"Bearer {access_token}"
        }

        response_completo = requests.get(url_completo, headers=headers)
        response_quadro_social = requests.get(url_quadro_social, headers=headers)
        response_score = requests.get(url_score, headers=headers)

        valida_response_e_envia_arquivo(response=response_completo, base_url=base_url, headers=headers, cnpj=cnpj, bucket=bucket, tipo_relato='SERASA_RELATO', key= issueKey)
        valida_response_e_envia_arquivo(response=response_quadro_social, base_url=base_url, headers=headers, cnpj=cnpj, bucket=bucket, tipo_relato='SERASA_RELATO_QUADRO_SOCIAL', key= issueKey)
        valida_response_e_envia_arquivo(response=response_score, base_url=base_url, headers=headers, cnpj=cnpj, bucket=bucket, tipo_relato='SERASA_RELATO_SCORE', key= issueKey)

    
    def envia_pdf(response, bucket, cnpj, tipo_relato,issueKey):
        if response.status_code == 200:
            pdf_content = BytesIO(response.content)
            connection = get_trino_connection()
            try:
                queryContentId = f'''
                        select 
                            rd.type ||'_'|| pi2.value || '_'|| rc.json_content || '.pdf', rc.json_content 
                        FROM 
                            postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                        inner join 
                            postgres.exrp_{Variable.get('STAGE')}_default.report_definition rd on rd.id = re.definition_id
                        left join 
                            postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
                        left join 
                            postgres.exrp_{Variable.get('STAGE')}_default.report_involvement ri on ri.report_execution_id = re.id
                        left join 
                            postgres.exrp_{Variable.get('STAGE')}_default.party p on p.id = ri.party_id
                        left join 
                            postgres.exrp_{Variable.get('STAGE')}_default.party_identification pi2 on pi2.party_id = p.id
                        where 
                            pi2.value like '{cnpj}%' and rd.type in ('SERASA_RELATO', 'SERASA_RELATO_COMPLETO') and re.resolution='DONE'
                        order by 
                            coalesce(re.last_modified_date, re.created_date) desc
                        limit 1'''
                
                result = execute_query(conn=connection, query=queryContentId)[0]
                print(result)
                print(Variable.get('TRINO_PASSWORD'))

                fileName = result[0]
                MinioWriteFile().write_file(file=pdf_content, file_name= fileName, bucket=bucket, type = "application/pdf")
                MinioSaveIndex().save(key= issueKey, identification=cnpj, path= bucket + f"/{fileName}", file=pdf_content, fileType= tipo_relato)
                MinioSaveIndex().save(key= issueKey, identification=cnpj, path= get_coordenadas(result[1],connection), file=None, fileType= "COORDENADA")
            except Exception as e:
                raise Exception(e)
            finally:
                connection.close()
        else:
            print(f'Erro na requisição: {response.status_code}')
    
    def is_relato_mais_antigo_que_60_dias(data):
        try:
            created_date_str = data['content'][0]['createdDate']

            created_date = parser.parse(created_date_str)

            created_date = created_date.replace(tzinfo=timezone.utc)

            now = datetime.now(tz=timezone.utc)
            return now - created_date > timedelta(days=60)
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON: {e}")
            raise
        except Exception as e:
            print(f"ERRO: {e}")
            raise

    def valida_response_e_envia_arquivo(response, base_url, headers, cnpj, bucket, tipo_relato, key):
        data = json.loads(response.content)
        if len(data['content']) > 0:
            if not is_relato_mais_antigo_que_60_dias(data):
                url_pdf = f"{base_url}api/v1/report-executions?$sort=createdDate desc,lastModifiedDate desc&$expand=content&$filter=involved.party.identifications.value='{cnpj}' and definition.type={tipo_relato} and resolution=DONE&$audit=true&$format=pdf"
                response_pdf = requests.get(url_pdf, headers=headers)
                print(response_pdf.status_code)

                if response_pdf:
                    envia_pdf(response=response_pdf, bucket=bucket, cnpj=cnpj, tipo_relato=tipo_relato, issueKey= key)
                else:
                    print('A consulta ao exrep não retornou o pdf')
        else:
            print(f"Não foi encontrado {tipo_relato} com menos de 60 dias")

    start = EmptyOperator(task_id="start")
    relato = busca_relato()
    end = EmptyOperator(task_id="end")

    start >> relato >> end

dag_instance = consulta_relato()