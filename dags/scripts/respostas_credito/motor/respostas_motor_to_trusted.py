# Carregando libs
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from deltalake import write_deltalake, DeltaTable
import psutil
import json
import numpy as np
from scripts.respostas_credito.query_trino_credito import query_trino

# Criando conexão
def transform_motor_to_trusted(files_list_motor, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "opdb-alpe"
    BUCKET_SOURCE_TRUSTED = "risco"
    REFINED_FOLDER = "analises_credito/motor"

    # Conectando na raw
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    print("::group::IMPORTANDO MOTOR")
    
    dfs_motor = []
    
    print(f"Importando .parquet Motor {psutil.virtual_memory()._asdict()}")
    # Juntando arquivos da Motor
    for file_name in files_list_motor:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_motor_temp = pd.read_parquet(BytesIO(file.data))
        dfs_motor.append(df_motor_temp)
        
    # Consolidando    
    print(f"Consolidando {psutil.virtual_memory()._asdict()}")
    df_motor = pd.concat(dfs_motor, ignore_index=True)
    
    print(f"Normalizando dados {psutil.virtual_memory()._asdict()}")
    # Pegando apenas ids de outputs do motor que foram fechados ontem
    df_motor = pd.json_normalize(df_motor['after'])
    mask_closed = df_motor['state'] == 'CLOSED'

    print(f"Pegando ids para puxar na query {psutil.virtual_memory()._asdict()}")
    ids_motor = str(df_motor[mask_closed]['id'].unique().tolist()).replace('[', '(').replace(']', ')') 
        
    query_motor = f"""select 
                    id,
                    created_date, last_modified_date,
                    external_reference,
                    output,
                    resolution,
                    state,
                    loid,
                    pageno,
                    from_utf8(pgl.data) as parse_file
                from postgres.inrp_prd_default.report_execution re
                join postgres.pg_catalog.pg_largeobject pgl on re."output" = cast(pgl.loid as varchar)
                WHERE id IN {ids_motor} AND resolution = 'DONE' and	state = 'CLOSED'"""
    
    print(f'quantidade de CNPJs a serem atualziados: {len(ids_motor)}')
    print(f"query: {query_motor}")
    
    print(f"Realizando Query {psutil.virtual_memory()._asdict()}")
    
    df_motor = query_trino(query_motor, 
                                access_params['trino_endpoint'],
                                access_params['trino_port'],
                                access_params['trino_user'],
                                access_params['trino_password'])
    
    print("::endgroup::")
    
    print("::group::Tratando DFs")
    
    print(f"Juntando parsefiles em uma linha só {psutil.virtual_memory()._asdict()}")
    # Concatena os valores de parse_file que correspondem a mesma análise de motor ou mesa
    df_motor = df_motor.sort_values(by=['loid', 'pageno', 'created_date']).groupby(['id', 'created_date', 'last_modified_date', 'external_reference', 'output', 'resolution', 'state', 'loid'])['parse_file'].apply(''.join).reset_index()
   
    print(f"removendo valores não referentes ao json {psutil.virtual_memory()._asdict()}")
    
    # Remove o começo o inicio da string do Jira Issue
    df_motor['json'] = df_motor['parse_file'].str.split('|', 1).str[1]
    
    print(f"Removendo erros de resolution {psutil.virtual_memory()._asdict()}")
    # REMOVENDO ANALISES QUE DERAM ERRO
    df_motor = df_motor[df_motor['resolution'] != 'ERROR'].reset_index(drop=True)
    
    print(f"Extraindo jsons {psutil.virtual_memory()._asdict()}")
    
    #Extraindo jsons
    df_motor.loc[df_motor['json'].notna(), 'json'] = df_motor.loc[df_motor['json'].notna(), 'json'].apply(json.loads).apply(json.loads)

    #'external_reference' == 'urn:reprocess' REPROCESSAMENTO DE ERROS
    #'external_reference' == 'urn:jira:issue:alpe:cmgt PRODUÇÃO DO JIRA
    #'external_reference' == 'urn:jira:issue:alpe:CMGH HOMOLOGAÇÃO DO JIRA
    #'external_reference' == 'urn:jira:issue:alpe:test' TESTE DO JIRA
    
    print(f"Retirando external_reference desnecessários {psutil.virtual_memory()._asdict()}")
    #DEIXANDO APENAS OS REPORCESSAMENTOS DE ERROS E OS PROCESSAMENTOS EM PRODUÇÃO
    mask1 = df_motor['external_reference'].str[0:14] == 'urn:reprocess:'
    mask2 = df_motor['external_reference'].str[0:24] == 'urn:jira:issue:alpe:cmgt'

    df_motor = df_motor[mask1 | mask2]    

    print(f"Retirando dados de modelo antigo do motor {psutil.virtual_memory()._asdict()}")
    # Retirando dados do motor anteriores ao modelo da política atual
    df_motor = df_motor[df_motor['created_date'] >= '2023-11-23'].reset_index()
    
    print(f"Convertendo Json do motor em colunas {psutil.virtual_memory()._asdict()}")
    # Convertendo Json de motor em colunas
    df_normalizado_motor = pd.json_normalize(df_motor['json'])

    #juntando tudo em um dataframe final
    df_final = pd.concat([df_normalizado_motor, df_motor], axis=1)
    
    print(f"Selecionando e ordenando colunas que permanecerão no DataFrame {psutil.virtual_memory()._asdict()}")
    #Selecionando colunas a serem utilizadas
    colunas_finais = ['cnpj','razao_social', 'id', 'fornecedor',
        'cnpj_ativo', 'cnae_aceito',
        'natureza_juridica_aceita', 'fundacao_valida',
        'inadimplente_fornecedor', 'resposta_pre_filtro',
        'vop', 'hp', 'atrasos', 'titulos_vencidos',
        'tempo_de_fundacao', 'restritivos_bvs',
        'restritivos_spc', 'restritivos_bvs_ou_spc',
        'score_positivo_bvs', 'socio_com_restritivo',
        'resposta_motor', 'casa', 'limite_concedido',
        'media_vop', 'codigo_cnae', 'cpf_do_principal_socio',
        'created_date', 'last_modified_date',
        'resolution', 'state']

    df_final = df_final[colunas_finais]

    #padronizando nome de colunas com o nome diferente
    df_final = df_final.rename(columns = {'cnpj':'cnpj_do_sacado', 'razao_social':'razao_social_sacado'})
    
    print(f"Retirando caracteres indevidos do Dataframe {psutil.virtual_memory()._asdict()}")
    #Criando função que retira caracteres despreziveis 
    def sustituindo_caracteres_despreziveis(df):
        return str(df).replace('R$', '').replace('.', '').replace(',', '.').replace('.00','0')

    colunas_com_possivel_caracter_desprezivel = ['cnpj_ativo', 'cnae_aceito',
        'natureza_juridica_aceita', 'fundacao_valida',
        'inadimplente_fornecedor', 'resposta_pre_filtro',
        'vop', 'hp', 'atrasos', 'titulos_vencidos',
        'tempo_de_fundacao', 'restritivos_bvs',
        'restritivos_spc', 'restritivos_bvs_ou_spc',
        'score_positivo_bvs', 'socio_com_restritivo',
        'resposta_motor', 'casa', 'limite_concedido',
        'media_vop', 'codigo_cnae',
        'last_modified_date']
    
    #aplicando Função e tratando strings vazias
    for coluna in colunas_com_possivel_caracter_desprezivel:
        df_final[coluna] = df_final[coluna].apply(sustituindo_caracteres_despreziveis)     
        
    def tratando_nulos(df):
        return str(df).replace('NAO ENCONTRADO', '').replace('Nao encontrado', '').replace('N/A', '').replace('nan', '').replace('None', '')
    
    for coluna in df_final.columns:
        df_final[coluna] = df_final[coluna].apply(tratando_nulos)
        
    df_final.replace('', np.nan)
    
    print(f"Definindo tipo de dados Dataframe {psutil.virtual_memory()._asdict()}")
    
     #Deixando YYYY-MM-DD HH:mm:SS
    df_final['created_date'] = df_final['created_date'].str[0:19]
    df_final['last_modified_date'] = df_final['last_modified_date'].str[0:19]
    
    #Definindo tipo dos dados
    
    data_types_dict = {'cnpj_do_sacado': 'str',
    'razao_social_sacado': 'str',
    'id': 'str',
    'fornecedor': 'str',
    'cnpj_ativo': 'bool',
    'cnae_aceito': 'bool',
    'natureza_juridica_aceita': 'bool',
    'fundacao_valida': 'bool',
    'inadimplente_fornecedor': 'bool',
    'resposta_pre_filtro': 'str',
    'vop': 'bool',
    'hp': 'bool',
    'atrasos': 'bool',
    'titulos_vencidos': 'bool',
    'tempo_de_fundacao': 'bool',
    'restritivos_bvs': 'bool',
    'restritivos_spc': 'bool',
    'restritivos_bvs_ou_spc': 'bool',
    'score_positivo_bvs': 'bool',
    'socio_com_restritivo': 'bool',
    'resposta_motor': 'str',
    'casa': 'str',
    'limite_concedido': 'float',
    'media_vop': 'float',
    'codigo_cnae': 'str',
    'cpf_do_principal_socio': 'str',
    'created_date': 'str',
    'last_modified_date': 'str',
    'resolution': 'str',
    'state': 'str'}

    #Ajustando tipo de dado
    df_final = df_final.astype(data_types_dict)
    
    print(f"Definindo colunas de fonte de partições {psutil.virtual_memory()._asdict()}")
    df_final['fonte'] = 'REPORT EXECUTION'
    
    #Definindo data de tratamento do arquivo
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    
    df_final['atualizado_em'] = now
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')  
    
    df_final['year'] = now.year
    df_final['month'] = now.month
    df_final['day'] = now.day
    
    print("::endgroup::")
    
    print(f"Salvando no Minio {psutil.virtual_memory()._asdict()}")
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    print(f"Definindo Schema {psutil.virtual_memory()._asdict()}")
    #Definindo schema para salvar no PyArrow
    schema = pa.schema([
        ('cnpj_do_sacado', pa.string()),
        ('razao_social_sacado', pa.string()),
        ('id', pa.string()),
        ('fornecedor', pa.string()),
        ('cnpj_ativo', pa.bool_()),
        ('cnae_aceito', pa.bool_()),
        ('natureza_juridica_aceita', pa.bool_()),
        ('fundacao_valida', pa.bool_()),
        ('inadimplente_fornecedor', pa.bool_()),
        ('resposta_pre_filtro', pa.string()),
        ('vop', pa.bool_()),
        ('hp', pa.bool_()),
        ('atrasos', pa.bool_()),
        ('titulos_vencidos', pa.bool_()),
        ('tempo_de_fundacao', pa.bool_()),
        ('restritivos_bvs', pa.bool_()),
        ('restritivos_spc', pa.bool_()),
        ('restritivos_bvs_ou_spc', pa.bool_()),
        ('score_positivo_bvs', pa.bool_()),
        ('socio_com_restritivo', pa.bool_()),
        ('resposta_motor', pa.string()),
        ('casa', pa.string()),
        ('limite_concedido', pa.float32()),
        ('media_vop', pa.float32()),
        ('codigo_cnae', pa.string()),
        ('cpf_do_principal_socio', pa.string()),
        ('created_date', pa.string()),
        ('last_modified_date', pa.string()),
        ('resolution', pa.string()),
        ('state', pa.string()),
        ('fonte', pa.string()),
        ('atualizado_em', pa.string()),
        ('year', pa.int32()),
        ('month', pa.int32()),
        ('day', pa.int32())
    ])
    
    print(f"Transformando em Pyarrow! {psutil.virtual_memory()._asdict()}")

    # # O pandas cria esse index, este codigo serve para remover caso ele crie
    df_final = pa.Table.from_pandas(df_final, preserve_index=False, schema=schema)

    print(f"Salvando no Minio {psutil.virtual_memory()._asdict()}")
    
    write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{REFINED_FOLDER}", 
                    df_final, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )