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
def transform_mesa_to_trusted(file_list_mesa, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "opdb-alpe"
    BUCKET_SOURCE_TRUSTED = "risco"
    REFINED_FOLDER = "analises_credito/mesa"

    # Conectando na raw
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    # PEGANDO DF DA MESA
    dfs_mesa = []
    
    print(f"Importando .parquet Mesa {psutil.virtual_memory()._asdict()}")
    # Juntando arquivos da Mesa
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
    
    #Ajustando o report_execution_id para str
    df_mesa.loc[df_mesa['report_execution_id'].notna(), 'report_execution_id'] = df_mesa.loc[df_mesa['report_execution_id'].notna(), 'report_execution_id'].astype(int).astype(str)
    
    
    # Concatena os valores de parse_file que correspondem a mesma análise
    df_mesa = df_mesa.sort_values(by=['loid', 'pageno', 'created_date']).groupby(['id', 'created_date', 'last_modified_date', 'input', 'report_execution_id', 'loid'])['parse_file'].apply(''.join).reset_index()
    
    print(f"removendo valores não referentes ao json {psutil.virtual_memory()._asdict()}")
    # Remove o começo o inicio da string do Jira Issue
    df_mesa['json'] = df_mesa['parse_file'].str.split('|', 1).str[1]

    
    print(f"Extraindo jsons {psutil.virtual_memory()._asdict()}")
    
    #Extraindo jsons
    df_mesa.loc[df_mesa['json'].notna(), 'json'] = df_mesa.loc[df_mesa['json'].notna(), 'json'].apply(json.loads)
    
    print(f"Selecionando colunas a partir dos Jsons extraidos de mesa {psutil.virtual_memory()._asdict()}")
    #Criando Dicionário com o nome da coluna final do DF e o caminho necessário para extrair a coluna do JSON
    colunas = {
        'titulo': [['data'],['issue'],['fields'],['summary']],
        
        'responsavel': [['data'],['user'],['displayName']], 
        
        'customer_request_type': [['data'],['issue'],['fields'],['issuetype'],['name']], 
        
        'razao_social_do_cedente': [['data'],['issue'],['fields'],['customfield_13719']], 
        
        'razao_social_do_sacado': [['data'],['issue'],['fields'],['customfield_13716']],
        
        'limite_aprovado': [['data'],['issue'],['fields'],['customfield_13709']],
        
        'cnpj_do_cedente': [['data'],['issue'],['fields'],['customfield_13728']],
        
        'cnpj_do_sacado': [['data'],['issue'],['fields'],['customfield_13729']],
        
        'limite_atual': [['data'],['issue'],['fields'],['customfield_13732']],
        
        'limite_solicitado': [['data'],['issue'],['fields'],['customfield_13737']],
        
        'score_de_credito': [['data'],['issue'],['fields'],['customfield_13734']],
        
        'limite_disponível': [['data'],['issue'],['fields'],['customfield_13735']],
        
        'limite_utilizado': [['data'],['issue'],['fields'],['customfield_13736']],

        'parecer_final': [['data'],['issue'],['fields'],['customfield_13753']]
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
        df_mesa.loc[df_mesa['json'].notna(), chave_dict] = df_mesa.loc[df_mesa['json'].notna(), 'json'].apply(lambda x: extraindo_json(x, chave_dict))
        
    #Dropando coluna já normalizada
    df_mesa = df_mesa.drop(columns = 'json')
    
    
    print(f"Selecionando e ordenando colunas que permanecerão no DataFrame {psutil.virtual_memory()._asdict()}")
    #Selecionando colunas a serem utilizadas
    colunas_finais = ['id', 'report_execution_id', 'created_date',
        'last_modified_date', 'titulo', 'responsavel',
        'razao_social_do_cedente',
        'razao_social_do_sacado', 'limite_aprovado',
        'cnpj_do_cedente', 'cnpj_do_sacado', 'limite_atual',
        'limite_solicitado', 'score_de_credito',
        'limite_disponível', 'limite_utilizado',
        'parecer_final']

    df_mesa = df_mesa[colunas_finais]
    
    print(f"Retirando caracteres indevidos do Dataframe {psutil.virtual_memory()._asdict()}")
    #Criando função que retira caracteres despreziveis 
    def sustituindo_caracteres_despreziveis(df):
        return str(df).replace('R$', '').replace('.', '').replace(',', '.').replace('.00','0')

    colunas_com_possivel_caracter_desprezivel = ['limite_aprovado', 'limite_atual', 'limite_solicitado',
        'limite_disponível', 'limite_utilizado', 'score_de_credito']
    
    #aplicando Função e tratando strings vazias
    for coluna in colunas_com_possivel_caracter_desprezivel:
        df_mesa[coluna] = df_mesa[coluna].apply(sustituindo_caracteres_despreziveis)
        
    def tratando_nulos(df):
        return str(df).replace('NAO ENCONTRADO', '').replace('N/A', '').replace('nan', '').replace('None', '')
    
    for coluna in df_mesa.columns:
        df_mesa[coluna] = df_mesa[coluna].apply(tratando_nulos).replace('', None)
    
        
    print(f"Definindo tipo de dados Dataframe {psutil.virtual_memory()._asdict()}")
    
    df_mesa['created_date'] = df_mesa['created_date'].dt.strftime('%Y-%m-%d %X') 
    df_mesa['last_modified_date'] = df_mesa['last_modified_date'].dt.strftime('%Y-%m-%d %X')
    
    #Definindo tipo dos dados
    data_types_dict = {
    'id': 'str',
    'report_execution_id': 'str',
    'created_date': 'str',
    'last_modified_date': 'str',
    'titulo': 'str',
    'responsavel': 'str',
    'razao_social_do_cedente': 'str',
    'razao_social_do_sacado': 'str',
    'limite_aprovado': 'float',
    'cnpj_do_cedente': 'str',
    'cnpj_do_sacado': 'str',
    'limite_atual': 'float',
    'limite_solicitado': 'float',
    'score_de_credito': 'float',
    'limite_disponível': 'float',
    'limite_utilizado': 'float',
    'parecer_final': 'str'}

    #Ajustando tipo de dado
    df_mesa = df_mesa.astype(data_types_dict)
    
    print(f"Definindo colunas de fonte de partições {psutil.virtual_memory()._asdict()}")
    df_mesa['fonte'] = 'JIRA ISSUE'
    
    #Definindo data de tratamento do arquivo
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    
    df_mesa['atualizado_em'] = now
    df_mesa['atualizado_em'] = now.strftime('%Y-%m-%d %X') 
    
    df_mesa['year'] = now.year
    df_mesa['month'] = now.month
    df_mesa['day'] = now.day
    
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
        ('id', pa.string()),
        ('report_execution_id', pa.string()),
        ('created_date', pa.string()),
        ('last_modified_date', pa.string()),
        ('titulo', pa.string()),
        ('responsavel', pa.string()),
        ('razao_social_do_cedente', pa.string()),
        ('razao_social_do_sacado', pa.string()),
        ('limite_aprovado', pa.float32()),
        ('cnpj_do_cedente', pa.string()),
        ('cnpj_do_sacado', pa.string()),
        ('limite_atual', pa.float32()),
        ('limite_solicitado', pa.float32()),
        ('score_de_credito', pa.float32()),
        ('limite_disponível', pa.float32()),
        ('limite_utilizado', pa.float32()),
        ('parecer_final', pa.string()),
        ('fonte', pa.string()),
        ('atualizado_em', pa.string()),
        ('year', pa.int32()),
        ('month', pa.int32()),
        ('day', pa.int32())
    ])
    
    print(f"Transformando em Pyarrow! {psutil.virtual_memory()._asdict()}")

    # # O pandas cria esse index, este codigo serve para remover caso ele crie
    df_mesa = pa.Table.from_pandas(df_mesa, preserve_index=False, schema=schema)

    print(f"Salvando no Minio {psutil.virtual_memory()._asdict()}")
    
    write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{REFINED_FOLDER}", 
                    df_mesa, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )