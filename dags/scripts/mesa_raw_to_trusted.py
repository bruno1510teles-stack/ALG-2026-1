import pandas as pd
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import json
import os
from deltalake import write_deltalake
from scripts.query_trino import query_trino
from flatten_json import flatten
import re

now = datetime.now(tz=timezone(timedelta(hours=-3)))

def transform_data_to_trusted(files_list, access_params):

    BUCKET_SOURCE_RAW = access_params['opdb_bucket']
    BUCKET_SOURCE_TRUSTED = "risk"
    TRUSTED_FOLDER = "mesa/"

    print(f"first file: {files_list[0]}")
    print(f"last file: {files_list[-1]}")

    '''
        CONEXÃO COM O MINIO RAW
    '''
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )

    df_all = pd.DataFrame()

    '''
        GET THE DATA FROM THE BUCKET AND TRANSFORM IT TO DATAFRAME
    '''
    dfs = []

    for file_name in files_list:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_raw_temp = pd.read_parquet(BytesIO(file.data))
        dfs.append(df_raw_temp)

    df_all = pd.concat(dfs, ignore_index=True)

    # Convertendo para o DataFrame final
    data = [v for v in df_all['after'].values]
    df_mesa_padrao = pd.DataFrame(data)

    ids_query = str(df_mesa_padrao['id'].tolist()).replace('[', '(').replace(']', ')')

    ''''
         QUERY TO GET THE DATA FROM THE TABLE "REPORT_EXECUTION" AND LARGE_OBJECT
    '''
    query_motor = f"""
            select * from postgres.inrp_prd_default.jira_issue j left join postgres.pg_catalog.pg_largeobject l on cast(j."input" as int) = l.loid where j."id" IN {ids_query}
        """    
    
    print(f"query: {query_motor}")

    df_response_mesa = query_trino(query_motor, 
                            access_params['trino_endpoint'],
                            access_params['trino_port'],
                            access_params['trino_user'],
                            access_params['trino_password'])
    
    df_response_mesa = df_response_mesa.sort_values(by=['pageno'])

    df_response_mesa_outuput = df_response_mesa.groupby(['loid']).agg({'id': 'max', 
                                                                       'report_execution_id': 'max',
                                                                       'data': 'sum',
                                                                        'pageno':'max'}).reset_index()

    df_response_mesa_outuput

    df_response_mesa_flat = pd.DataFrame()

    for i, row in df_response_mesa_outuput.iterrows():
        decoded_string = row["data"].decode('latin-1')
        data = decoded_string.replace('java.util.LinkedHashMap|', '')

        data_d = json.loads(data)

        data_issue = data_d['data']['issue']

        data_flat = flatten(data_issue)

        df_temp_flat = pd.DataFrame.from_dict(data_flat, orient='index').T

        df_temp_flat['report_execution_id'] = row['report_execution_id']
        df_temp_flat['id'] = row['id']

        df_response_mesa_flat = pd.concat([df_response_mesa_flat, df_temp_flat ], ignore_index=True)

    de_para_colunas = {'id': 'id_jira_issue',
                        'key': 'key_jira_issue',
                        'fields_statuscategorychangedate': 'data_atualizacao_status',
                        'fields_resolution_name': 'resolucao',
                        'fields_status_name': 'status',
                        'fields_customfield_13720_ongoingCycle_startTime_jira': 'data_inicio',
                        'fields_customfield_13720_ongoingCycle_startTime_jira': 'data_fim',
                        'fields_assignee_displayName': 'responsavel_analise',
                        'fields_customfield_13719': 'cedente_nome',
                        'fields_customfield_13716': 'sacado_nome',
                        'fields_priority_name': 'prioridade',
                        'fields_customfield_13709': 'valor_credito_aprovado',
                        'fields_customfield_13725_value': 'motivo_analise',
                        'fields_customfield_13728': 'cnpj_cedente',
                        'fields_customfield_13729': 'cnpj_sacado',
                        'fields_customfield_13714': 'parcelas',
                        'fields_customfield_13732': 'limite_atual',
                        'fields_customfield_13737': 'limite_solicitado',
                        'fields_customfield_13735': 'limite_disponivel',
                        'fields_customfield_13736': 'limite_utilizado',
                        'fields_customfield_13740': 'cedente_pgid',
                        'fields_customfield_13741': 'cedente_cnpj_raiz',
                        'fields_customfield_13739': 'sacado_pgid',
                        'fields_customfield_13738': 'sacado_cnpj_raiz',
                        'fields_description': 'descricao',}

    for k, v in de_para_colunas.items():
        if k in df_response_mesa_flat.columns:
            df_response_mesa_flat.rename(columns={k: v}, inplace=True)         
        else:
            print(f'coluna {k} não existe no dataframe, criando a {v}')
            df_response_mesa_flat[v] = None

    df_response_mesa_final = df_response_mesa_flat[de_para_colunas.values()]

    '''
        ADICIONAR COLUNAS DE METADADOS
    '''
    df_response_mesa_final['year'] = now.year
    df_response_mesa_final['month'] = now.month
    df_response_mesa_final['day'] = now.day


    '''
        CONVERTER TIPOS DE COLUNAS
    '''
    convert_dict = {'id_jira_issue': int,
            'key_jira_issue': str,
            'data_atualizacao_status': str,
            'resolucao': str,
            'status': str,
            'data_fim': str,
            'responsavel_analise': str,
            'cedente_nome': str,
            'sacado_nome': str,
            'prioridade': str,
            'valor_credito_aprovado': str,
            'motivo_analise': str,
            'cnpj_cedente': str,
            'cnpj_sacado': str,
            'parcelas': str,
            'limite_atual': str,
            'limite_solicitado': str,
            'limite_disponivel': str,
            'limite_utilizado': str,
            'cedente_pgid': str,
            'cedente_cnpj_raiz': str,
            'sacado_pgid': str,
            'sacado_cnpj_raiz': str,
            'descricao': str,
            'year': float,
            'month': float,
            'day': float}
    
    for k, v in convert_dict.items():
        df_response_mesa_final[k] = df_response_mesa_final[k].astype(v)

    df_response_mesa_final.replace({'nan': None}, inplace=True)
    
    '''
        ENVIAR OS DADOS PARA O MINIO TRUSTED NO FORMATO DE DELTA TABLE
    '''
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED }/{TRUSTED_FOLDER}", 
                    df_response_mesa_final, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )