# Carregando libs
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from deltalake import write_deltalake, DeltaTable
import psutil
import json
from scripts.respostas_credito.query_trino_credito import query_trino

# Criando conexão
def transform_credito_to_trusted(files_list_motor, file_list_mesa, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "opdb-alpe"
    BUCKET_SOURCE_REFINED = "risco"
    REFINED_FOLDER = "motor/book_de_variaveis/"

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
    
    print("::group::IMPORTANDO MESA")
    
    # PEGANDO DF DA MESA
    dfs_mesa = []
    
    print(f"Importando .parquet Mesa {psutil.virtual_memory()._asdict()}")
    # Juntando arquivos da Motor
    for file_name in file_list_mesa:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        dfs_mesa_temp = pd.read_parquet(BytesIO(file.data))
        dfs_mesa.append(dfs_mesa_temp)
        
    # Consolidando    
    print(f"Consolidando {psutil.virtual_memory()._asdict()}")
    df_mesa = pd.concat(dfs_mesa, ignore_index=True)

    print(f"Normalizando dados {psutil.virtual_memory()._asdict()}")
    df_mesa = pd.json_normalize(df_mesa['after'])
    
    print(f"Pegando ids para puxar na query {psutil.virtual_memory()._asdict()}")
    ids_mesa = str(df_mesa['id'].unique().tolist()).replace('[', '(').replace(']', ')') 
    
    query_mesa = f"""select 
                        id,
                        created_date,
                        last_modified_date,
                        input,
                        report_execution_id,
                        loid,
                        pageno,
                        from_utf8(pgl.data) as parse_file
                    from postgres.inrp_prd_default.jira_issue ji
                    join postgres.pg_catalog.pg_largeobject pgl on ji."input" = cast(pgl.loid as varchar)
                 WHERE id IN {ids_mesa}"""
    
    print(f'quantidade de CNPJs a serem atualziados: {len(ids_mesa)}')
    print(f"query: {query_mesa}")
    
    print(f"Realizando Query {psutil.virtual_memory()._asdict()}")
    df_mesa = query_trino(query_mesa, 
                                access_params['trino_endpoint'],
                                access_params['trino_port'],
                                access_params['trino_user'],
                                access_params['trino_password'])
    
    print("::endgroup::")
    
    print("::group::Tratando DFs")
    
    print(f"Juntando parsefiles em uma linha só {psutil.virtual_memory()._asdict()}")
    # Concatena os valores de parse_file que correspondem a mesma análise de motor ou mesa
    df_motor = df_motor.sort_values(by=['loid', 'pageno', 'created_date']).groupby(['id', 'created_date', 'last_modified_date', 'external_reference', 'output', 'resolution', 'state', 'loid'])['parse_file'].apply(''.join).reset_index()
    df_mesa = df_mesa.sort_values(by=['loid', 'pageno', 'created_date']).groupby(['id', 'created_date', 'last_modified_date', 'input', 'report_execution_id', 'loid'])['parse_file'].apply(''.join).reset_index()
    
    print(f"removendo valores não referentes ao json {psutil.virtual_memory()._asdict()}")
    # Remove o começo o inicio da string do Jira Issue
    df_mesa['json'] = df_mesa['parse_file'].str.replace('java.util.LinkedHashMap|','')

    # Remove o começo o inicio da string do Jira Issue
    df_motor['json'] = df_motor['parse_file'].str.replace('java.lang.String|','')

    print(f"Juntando dfs {psutil.virtual_memory()._asdict()}")
    #Juntando dataframes
    df_merged = pd.merge(df_motor, df_mesa, left_on='id', right_on='report_execution_id', how='outer', suffixes=('_motor', '_mesa'))
    
    print(f"Removendo erros de resolution {psutil.virtual_memory()._asdict()}")
    # REMOVENDO ANALISES QUE DERAM ERRO
    df_merged = df_merged[df_merged['resolution'] != 'ERROR'].reset_index(drop=True)
    
    print(f"Extraindo jsons {psutil.virtual_memory()._asdict()}")
    
    #Extraindo jsons
    print(df_merged.loc[df_merged['json_mesa'].notna(), 'json_mesa'])
    df_merged.loc[df_merged['json_mesa'].notna(), 'json_mesa'] = df_merged.loc[df_merged['json_mesa'].notna(), 'json_mesa'].apply(json.loads)
    
    
    df_merged.loc[df_merged['json_motor'].notna(), 'json_motor'] = df_merged.loc[df_merged['json_motor'].notna(), 'json_motor'].apply(json.loads).apply(json.loads)
    
    #'external_reference' == 'urn:reprocess' REPROCESSAMENTO DE ERROS
    #'external_reference' == 'urn:jira:issue:alpe:cmgt PRODUÇÃO DO JIRA
    #'external_reference' == 'urn:jira:issue:alpe:CMGH HOMOLOGAÇÃO DO JIRA
    #'external_reference' == 'urn:jira:issue:alpe:test' TESTE DO JIRA
    
    print(f"Retirando external_reference desnecessários {psutil.virtual_memory()._asdict()}")
    #DEIXANDO APENAS OS REPORCESSAMENTOS DE ERROS E OS PROCESSAMENTOS EM PRODUÇÃO
    mask1 = df_merged['external_reference'].str[0:14] == 'urn:reprocess:'
    mask2 = df_merged['external_reference'].str[0:24] == 'urn:jira:issue:alpe:cmgt'

    df_merged = df_merged[mask1 | mask2]    
    
    print(f"Selecionando colunas a partir dos Jsons extraidos de mesa {psutil.virtual_memory()._asdict()}")
    #Criando Dicionário com o nome da coluna final do DF e o caminho necessário para extrair a coluna do JSON
    colunas = {
        'titulo_mesa': [['data'],['issue'],['fields'],['summary']],
        
        'responsavel_mesa': [['data'],['user'],['displayName']], 
        
        'customer_request_type_mesa': [['data'],['issue'],['fields'],['issuetype'],['name']], 
        
        'razao_social_do_cedente_mesa': [['data'],['issue'],['fields'],['customfield_13719']], 
        
        'razao_social_do_sacado_mesa': [['data'],['issue'],['fields'],['customfield_13716']],
        
        'limite_aprovado_mesa': [['data'],['issue'],['fields'],['customfield_13709']],
        
        'cnpj_do_cedente_mesa': [['data'],['issue'],['fields'],['customfield_13728']],
        
        'cnpj_do_sacado_mesa': [['data'],['issue'],['fields'],['customfield_13729']],
        
        'limite_atual_mesa': [['data'],['issue'],['fields'],['customfield_13732']],
        
        'limite_solicitado_mesa': [['data'],['issue'],['fields'],['customfield_13737']],
        
        'score_de_credito_mesa': [['data'],['issue'],['fields'],['customfield_13734']],
        
        'limite_disponível_mesa': [['data'],['issue'],['fields'],['customfield_13735']],
        
        'limite_utilizado_mesa': [['data'],['issue'],['fields'],['customfield_13736']],

        'parecer_final_mesa': [['data'],['issue'],['fields'],['customfield_13753']]
    }
    
    # Criando Função que extrai os valores do Json de acordo com a chave fornecida, desde que esteja contida no dicionario 'colunas'
    def extraindo_json(json, chave):    
        # Para cada valor, de acordo com a chave fornecida, o loop vai entrando no Json até buscar o valor final
        for caminho_json in colunas[chave]:
                json = json[caminho_json[0]]            
        # Retorna o valor final do Json        
        return json
    
    # Lista as chave que tem no dicionário de colunas
    chaves_colunas = list(colunas.keys())

    # Para cada chave no dicionário, aplica a função de extrair o JSON e atrbui conforme a respectiva coluna
    for chave_dict in chaves_colunas: 
        df_merged.loc[df_merged['json_mesa'].notna(), chave_dict] = df_merged.loc[df_merged['json_mesa'].notna(), 'json_mesa'].apply(lambda x: extraindo_json(x, chave_dict))
        
    #Dropando coluna já normalizada
    df_merged = df_merged.drop(columns = 'json_mesa')

    print(f"Retirando dados de modelo antigo do motor {psutil.virtual_memory()._asdict()}")
    # Retirando dados do motor anteriores ao modelo da política atual
    df_merged = df_merged[df_merged['created_date_motor'] >= '2023-11-23'].reset_index()
    
    print(f"Convertendo Json do motor em colunas {psutil.virtual_memory()._asdict()}")
    # Convertendo Json de motor em colunas
    df_normalizado_motor = pd.json_normalize(df_merged['json_motor'])

    # Acrescentando sufixo _motor em todas colunas de motor
    df_normalizado_motor = df_normalizado_motor.rename(columns=lambda x: x + '_motor')

    #juntando tudo em um dataframe final
    df_final = pd.concat([df_normalizado_motor, df_merged], axis=1)
    
    print(f"Selecionando e ordenando colunas que permanecerão no DataFrame {psutil.virtual_memory()._asdict()}")
    #Selecionando colunas a serem utilizadas
    colunas_finais = ['cnpj_motor','razao_social_motor', 'id_motor', 'fornecedor_motor',
        'cnpj_ativo_motor', 'cnae_aceito_motor',
        'natureza_juridica_aceita_motor', 'fundacao_valida_motor',
        'inadimplente_fornecedor_motor', 'resposta_pre_filtro_motor',
        'vop_motor', 'hp_motor', 'atrasos_motor', 'titulos_vencidos_motor',
        'tempo_de_fundacao_motor', 'restritivos_bvs_motor',
        'restritivos_spc_motor', 'restritivos_bvs_ou_spc_motor',
        'score_positivo_bvs_motor', 'socio_com_restritivo_motor',
        'resposta_motor_motor', 'casa_motor', 'limite_concedido_motor',
        'media_vop_motor', 'codigo_cnae_motor', 'cpf_do_principal_socio_motor',
        'created_date_motor', 'last_modified_date_motor',
        'resolution', 'state', 'id_mesa', 'created_date_mesa',
        'last_modified_date_mesa', 'titulo_mesa', 'responsavel_mesa',
        'razao_social_do_cedente_mesa',
        'razao_social_do_sacado_mesa', 'limite_aprovado_mesa',
        'cnpj_do_cedente_mesa', 'limite_atual_mesa',
        'limite_solicitado_mesa', 'score_de_credito_mesa',
        'limite_disponível_mesa', 'limite_utilizado_mesa',
        'parecer_final_mesa']

    df_final = df_final[colunas_finais]

    #padronizando nome de colunas com o nome diferente
    df_final = df_final.rename(columns = {'cnpj_motor':'cnpj_do_sacado_motor', 'razao_social_motor':'razao_social_sacado_motor'})
    
    print(f"Retirando caracteres indevidos do Dataframe {psutil.virtual_memory()._asdict()}")
    #Criando função que retira caracteres despreziveis 
    def sustituindo_caracteres_despreziveis(df):
        return str(df).replace('NAO ENCONTRADO', '').replace('N/A', '').replace('R$', '').replace('.', '').replace(',', '.').replace('.00','0').replace('nan', '').replace('None', '')

    #aplicando Função e tratando strings vazias
    for coluna in df_final.columns:
        df_final[coluna] = df_final[coluna].apply(sustituindo_caracteres_despreziveis).replace('', None)
        
    print(f"Definindo tipo de dados Dataframe {psutil.virtual_memory()._asdict()}")
    #Definindo tipo dos dados
    data_types_dict = {'cnpj_do_sacado_motor': 'str',
    'razao_social_sacado_motor': 'str',
    'id_motor': 'str',
    'fornecedor_motor': 'str',
    'cnpj_ativo_motor': 'bool',
    'cnae_aceito_motor': 'bool',
    'natureza_juridica_aceita_motor': 'bool',
    'fundacao_valida_motor': 'bool',
    'inadimplente_fornecedor_motor': 'bool',
    'resposta_pre_filtro_motor': 'str',
    'vop_motor': 'bool',
    'hp_motor': 'bool',
    'atrasos_motor': 'bool',
    'titulos_vencidos_motor': 'bool',
    'tempo_de_fundacao_motor': 'bool',
    'restritivos_bvs_motor': 'bool',
    'restritivos_spc_motor': 'bool',
    'restritivos_bvs_ou_spc_motor': 'bool',
    'score_positivo_bvs_motor': 'bool',
    'socio_com_restritivo_motor': 'bool',
    'resposta_motor_motor': 'str',
    'casa_motor': 'str',
    'limite_concedido_motor': 'float',
    'media_vop_motor': 'float',
    'codigo_cnae_motor': 'str',
    'cpf_do_principal_socio_motor': 'str',
    'created_date_motor': 'datetime64[us]',
    'last_modified_date_motor': 'datetime64[us]',
    'resolution': 'str',
    'state': 'str',
    'id_mesa': 'str',
    'created_date_mesa': 'datetime64[us]',
    'last_modified_date_mesa': 'datetime64[us]',
    'titulo_mesa': 'str',
    'responsavel_mesa': 'str',
    'razao_social_do_cedente_mesa': 'str',
    'razao_social_do_sacado_mesa': 'str',
    'limite_aprovado_mesa': 'float',
    'cnpj_do_cedente_mesa': 'str',
    'limite_atual_mesa': 'float',
    'limite_solicitado_mesa': 'float',
    'score_de_credito_mesa': 'float',
    'limite_disponível_mesa': 'float',
    'limite_utilizado_mesa': 'float',
    'parecer_final_mesa': 'str'}
    
    #Tratando colunas date antes de trocar o tipo de dado:
    colunas_datetime = []
    for chave, valor in data_types_dict.items():
        if valor == 'datetime64[us]':
            colunas_datetime.append(chave)

    for coluna in colunas_datetime:
        df_final[coluna] = df_final[coluna].str[0:19]

    #Ajustando tipo de dado
    df_final = df_final.astype(data_types_dict)
    
    print(f"Definindo colunas de fonte de partições {psutil.virtual_memory()._asdict()}")
    df_final['fonte'] = 'MOTOR/V1'
    
    #Definindo data de tratamento do arquivo
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    
    df_final['year'] = now.year
    df_final['month'] = now.month
    df_final['day'] = now.day
    
    print("::endgroup::")
    
    print(f"Salvando no Minio {psutil.virtual_memory()._asdict()}")
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    print(f"Definindo Schema {psutil.virtual_memory()._asdict()}")
    #Definindo schema para salvar no PyArrow
    schema = pa.schema([
        ('cnpj_do_sacado_motor', pa.string()),
        ('razao_social_sacado_motor', pa.string()),
        ('id_motor', pa.string()),
        ('fornecedor_motor', pa.string()),
        ('cnpj_ativo_motor', pa.bool_()),
        ('cnae_aceito_motor', pa.bool_()),
        ('natureza_juridica_aceita_motor', pa.bool_()),
        ('fundacao_valida_motor', pa.bool_()),
        ('inadimplente_fornecedor_motor', pa.bool_()),
        ('resposta_pre_filtro_motor', pa.string()),
        ('vop_motor', pa.bool_()),
        ('hp_motor', pa.bool_()),
        ('atrasos_motor', pa.bool_()),
        ('titulos_vencidos_motor', pa.bool_()),
        ('tempo_de_fundacao_motor', pa.bool_()),
        ('restritivos_bvs_motor', pa.bool_()),
        ('restritivos_spc_motor', pa.bool_()),
        ('restritivos_bvs_ou_spc_motor', pa.bool_()),
        ('score_positivo_bvs_motor', pa.bool_()),
        ('socio_com_restritivo_motor', pa.bool_()),
        ('resposta_motor_motor', pa.string()),
        ('casa_motor', pa.string()),
        ('limite_concedido_motor', pa.float32()),
        ('media_vop_motor', pa.float32()),
        ('codigo_cnae_motor', pa.string()),
        ('cpf_do_principal_socio_motor', pa.string()),
        ('created_date_motor', pa.date64()),
        ('last_modified_date_motor', pa.date64()),
        ('resolution', pa.string()),
        ('state', pa.string()),
        ('id_mesa', pa.string()),
        ('created_date_mesa', pa.date64()),
        ('last_modified_date_mesa', pa.date64()),
        ('titulo_mesa', pa.string()),
        ('responsavel_mesa', pa.string()),
        ('razao_social_do_cedente_mesa', pa.string()),
        ('razao_social_do_sacado_mesa', pa.string()),
        ('limite_aprovado_mesa', pa.float32()),
        ('cnpj_do_cedente_mesa', pa.string()),
        ('limite_atual_mesa', pa.float32()),
        ('limite_solicitado_mesa', pa.float32()),
        ('score_de_credito_mesa', pa.float32()),
        ('limite_disponível_mesa', pa.float32()),
        ('limite_utilizado_mesa', pa.float32()),
        ('parecer_final_mesa', pa.string()),
        ('fonte', pa.string()),
        ('year', pa.int32()),
        ('month', pa.int32()),
        ('day', pa.int32())
    ])
    
    print(f"Transformando em Pyarrow! {psutil.virtual_memory()._asdict()}")

    # # O pandas cria esse index, este codigo serve para remover caso ele crie
    df_final = pa.Table.from_pandas(df_final, preserve_index=False, schema=schema)

    print(f"Salvando no Minio {psutil.virtual_memory()._asdict()}")
    
    write_deltalake(f"s3a://{BUCKET_SOURCE_REFINED}/{REFINED_FOLDER}", 
                    df_final, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )