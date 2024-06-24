''''
    ESSE SCRIPT É USADO PARA MANIPULAR OS DADOS DO RESULTADO DO MOTOR E TRANSFORMAR EM UM DELTA TABLE NA TRUSTED.
    passo a passo:
    1. Pega a lista de caminhos dos arquivos no minio raw, puxa os objetos do caminho (parquet) e transforma em dataframe.
    2. Extrai da coluna After o json, deixa eles flat e transforma em dataframe.
    3. Faz um query no trino para pegar os dados da tabela report_execution e large_object usando os IDs do dataframe do passo 2 como filtro.
    4. Ordena os dados e agrupa por loid para concatenar o json.
    5. Manipula o json para transformar em dataframe.
    6. Adiciona colunas de metadados.
    7. Define o schema dos dados.
    8. Envia os dados para o minio trusted no formato de delta table.
'''
import pandas as pd
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import json
import os
from deltalake import write_deltalake
from scripts.query_trino import query_trino

now = datetime.now(tz=timezone(timedelta(hours=-3)))

def transform_data_to_trusted(files_list, access_params):
    # files_list é uma lista de strings que são o caminho para o arquivo.

    BUCKET_SOURCE_RAW = access_params['opdb_bucket']
    BUCKET_SOURCE_TRUSTED = "risk"
    TRUSTED_FOLDER = "response_engine/"

    print(f"first file: {files_list[0]}")
    print(f"last file: {files_list[-1]}")

    df_all = pd.DataFrame()

    '''
        CONEXÃO COM O MINIO RAW
    '''
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )

    '''
        GET THE DATA FROM THE BUCKET AND TRANSFORM IT TO DATAFRAME
    '''
    dfs = []

    for file_name in files_list:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_boletos_raw_temp = pd.read_parquet(BytesIO(file.data))
        dfs.append(df_boletos_raw_temp)

    df_all = pd.concat(dfs, ignore_index=True)

    # Convertendo para o DataFrame final
    data = [v for v in df_all['after'].values]
    df_engine_respose_padrao = pd.DataFrame(data)

    ids_query = str(df_engine_respose_padrao['id'].tolist()).replace('[', '(').replace(']', ')')

    ''''
         QUERY TO GET THE DATA FROM THE TABLE "REPORT_EXECUTION" AND LARGE_OBJECT
    '''
    query_motor = f"""
            select * from postgres.inrp_prd_default.report_execution r left join postgres.pg_catalog.pg_largeobject l on cast(r."output" as int) = l.loid where r."id" IN {ids_query}
        """    
    
    print(f"query: {query_motor}")

    df_reponse_engine = query_trino(query_motor, 
                            access_params['trino_endpoint'],
                            access_params['trino_port'],
                            access_params['trino_user'],
                            access_params['trino_password'])

    ''''
        SORT VALUES AND GROUP BY LOID TO CONCATENATE THE JSON CONTENT
    '''
    df_reponse_engine = df_reponse_engine.sort_values(by=['loid', 'pageno'])

    df_response_motor_outuput = df_reponse_engine.groupby(['loid']).agg({'id': 'max',
                                                                        'created_date': 'max', 
                                                                        'external_reference': 'max',
                                                                        'resolution': 'last',
                                                                        'state': 'last',
                                                                        'data': 'sum', 
                                                                        'pageno':'max'}).reset_index()

    '''
        MANIPULATE THE JSON CONTENT TO TRANSFORM IT TO DATAFRAME
    '''
    df_response_motor_final = pd.DataFrame()

    for index, row in df_response_motor_outuput.iterrows():
        decoded_string = row["data"].decode('latin-1')
        data = decoded_string.replace('\n', '').replace('\\', '')\
                        .replace('\\', '').replace(',n  ', ', ').replace('"{n  ', '{')\
                        .replace('java.lang.String|', '').replace('n}n"', '}')
        try:
            data_d = json.loads(data) # transforma json em dict
            df_temp = pd.DataFrame.from_dict(data_d, orient='index').T
            df_temp['id'] = row['id']
            df_temp['created_date'] = row['created_date']
            df_temp['external_reference'] = row['external_reference']
            df_temp['resolution'] = row['resolution']
            df_temp['state'] = row['state']
            df_response_motor_final = pd.concat([df_response_motor_final, df_temp], ignore_index=True)
        except:
            pass

    '''
        ADICIONAR COLUNAS DE METADADOS
    '''
    df_response_motor_final['year'] = now.year
    df_response_motor_final['month'] = now.month
    df_response_motor_final['day'] = now.day

    ''''
        CONVERTER TIPOS DOS DADOS
    '''
    df_response_motor_final.replace({'nan': None}, inplace=True)
    df_response_motor_final.replace({'None': None}, inplace=True)
    df_response_motor_final.replace({'Sem Nome': None}, inplace=True)
    df_response_motor_final.replace({'Sem nome': None}, inplace=True)
    df_response_motor_final.replace({'Sem cpf': None}, inplace=True)
    df_response_motor_final.replace({'Sem CPF': None}, inplace=True)
    df_response_motor_final.replace({'Sem Razu00e3o Social': None}, inplace=True)

    '''
        DEFINE O SCHEMA DOS DADOS
    '''
    convert_dict = {'nome': str,
                'cnpj': str,
                'cpf': str,
                'classificacao': str,
                'responsavel': str,
                'pep': str,
                'grupo': str,
                'revalidar_micro_empresa': str,
                'idade': float,
                'score': str,
                'probabilidade_default': float,
                'restritivo': str,
                'razao_social': str,
                'limite_fornecedor': str,
                'limite_calculado': str,
                'limite_sugerido': str,
                'media_vop': str,
                'data_atualizacao': 'datetime64[ns]',
                'resultado': str,
                'year': int,
                'month': int,
                'id': int,
                'created_date': 'datetime64[ns]',
                'external_reference': str,
                'resolution': str,
                'state': str,
                'fornecedor': str,
                'cnpj_ativo': str,
                'cnae_aceito': str,
                'natureza_juridica_aceita': str,
                'fundacao_valida': str,
                'inadimplente_fornecedor': str,
                'resposta_pre_filtro': str,
                'vop': str,
                'hp': str,
                'atrasos': str,
                'titulos_vencidos': str,
                'tempo_de_fundacao': str,
                'restritivos_bvs': str,
                'restritivos_spc': str,
                'restritivos_bvs_ou_spc': str,
                'score_positivo_bvs': str,
                'socio_com_restritivo': str,
                'resposta_motor': str,
                'casa': str,
                'limite_concedidomedia_vop': str,
                'limite_concedido': str,
                'codigo_cnae': str,
                'day': int
                }

    for k, v in convert_dict.items():
        if k in df_response_motor_final.columns:
            df_response_motor_final[k] = df_response_motor_final[k].astype(v)
        else:
            print(f'coluna {k} não existe no dataframe, criando a {v}')
            df_response_motor_final[k] = None
            df_response_motor_final[k] = df_response_motor_final[k].astype(v)

    df_final = df_response_motor_final[convert_dict.keys()]

    ''''
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
                    df_final, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )