# Carregando libs
import pandas as pd
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable
from scripts.query_trino_payments import query_trino

# Calculando Variáveis
def calculo_qtde_titulos_abertos_vencidos (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['data_vencimento'] < data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['data_pagamento'])]
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'numero_titulo': 'count'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'qtde_titulos_abertos_vencidos']
    return repositorio_dado

def calculo_valor_titulos_abertos_vencidos (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['data_vencimento'] < data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['data_pagamento'])]
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'valor_titulo': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'valor_titulos_abertos_vencidos']
    return repositorio_dado

def calculo_qtde_titulos_abertos_a_vencer (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['data_vencimento'] >= data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['data_pagamento'])]
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'numero_titulo': 'count'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'qtde_titulos_abertos_a_vencer']
    return repositorio_dado

def calculo_valor_titulos_abertos_a_vencer (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['data_vencimento'] >= data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['data_pagamento'])]
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'valor_titulo': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'valor_titulos_abertos_a_vencer']
    return repositorio_dado

def calculo_prazo_medio_abertos_vencidos (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['data_vencimento'] < data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['data_pagamento'])]
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado['data_emissao'] = pd.to_datetime(repositorio_dado['data_emissao'])
    repositorio_dado['aux_prazo_medio'] = (repositorio_dado['data_vencimento'] - repositorio_dado['data_emissao']).dt.days
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'aux_prazo_medio': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'prazo_medio_abertos_vencidos']
    return repositorio_dado

def calculo_prazo_medio_abertos_a_vencer (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['data_vencimento'] >= data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['data_pagamento'])]
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado['data_emissao'] = pd.to_datetime(repositorio_dado['data_emissao'])
    repositorio_dado['aux_prazo_medio'] = (repositorio_dado['data_vencimento'] - repositorio_dado['data_emissao']).dt.days
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'aux_prazo_medio': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'prazo_medio_abertos_a_vencer']
    return repositorio_dado

def calculo_qtde_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'numero_titulo': 'count'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'qtde_titulos_liquidados']
    return repositorio_dado

def calculo_valor_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'valor_titulo': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'valor_titulos_liquidados']
    return repositorio_dado

def calculo_prazo_medio_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado['data_emissao'] = pd.to_datetime(repositorio_dado['data_emissao'])
    repositorio_dado['aux_prazo_medio'] = (repositorio_dado['data_vencimento'] - repositorio_dado['data_emissao']).dt.days
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'aux_prazo_medio': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'prazo_medio_titulos_liquidados']
    return repositorio_dado

def calculo_atraso_medio_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado['data_pagamento'] = pd.to_datetime(repositorio_dado['data_pagamento'])
    repositorio_dado['dif_dias_pagamentos'] = (repositorio_dado['data_pagamento'] - repositorio_dado['data_vencimento']).dt.days
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'dif_dias_pagamentos': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'atraso_medio_titulos_liquidados']
    return repositorio_dado

def calculo_atraso_max_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado['data_pagamento'] = pd.to_datetime(repositorio_dado['data_pagamento'])
    repositorio_dado['dif_dias_pagamentos'] = (repositorio_dado['data_pagamento'] - repositorio_dado['data_vencimento']).dt.days
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'dif_dias_pagamentos': 'max'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'atraso_max_titulos_liquidados']
    return repositorio_dado

def calculo_atraso_min_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado['data_pagamento'] = pd.to_datetime(repositorio_dado['data_pagamento'])
    repositorio_dado['dif_dias_pagamentos'] = (repositorio_dado['data_pagamento'] - repositorio_dado['data_vencimento']).dt.days
    repositorio_dado = repositorio_dado.groupby(['documento_raiz', 'fornecedor']).agg({
    'dif_dias_pagamentos': 'min'
    }).reset_index()
    repositorio_dado.columns = ['documento_raiz', 'fornecedor', 'atraso_min_titulos_liquidados']
    return repositorio_dado

# Criando conexão
def transform_data_to_refined(files_list, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_TRUSTED = "payments"
    TRUSTED_FOLDER =  "boletos/"
    BUCKET_SOURCE_REFINED = "payments"
    REFINED_FOLDER = "mesa/visao_resumida_hp_externa/"

    df_payments = pd.DataFrame()

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_trusted'],
        access_key = access_params['aws_access_key_id_trusted'],
        secret_key = access_params['aws_secret_access_key_trusted'],
    )
    
    dfs = []
    # Juntando arquivos da HP
    for file_name in files_list:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_TRUSTED, object_name=file_name)
        df_hpex_raw_temp = pd.read_parquet(BytesIO(file.data))
        dfs.append(df_hpex_raw_temp)
    # Consolidando    
    base = pd.concat(dfs, ignore_index=True)

    base = base[base['fonte'] == 'HP_EXTERNA']
    base = base[base['tipo_documento'] == 'CNPJ']
    
    #Selecionando apenas os cnpjs que precisam ser atualizados
    base['documento_raiz'] = base['documento'].str[:8]
    
    ids_query = str(base['documento_raiz'].unique().tolist()).replace('[', '(').replace(']', ')') 
    
    query = f""" WITH CTE AS (
    SELECT 
        substring(documento, 1, 8) documento_raiz,
        razao_social,
        numero_titulo,
        data_emissao,
        data_vencimento,
        data_pagamento,
        valor_titulo,
        data_hp,
        numero_parcela,
        fornecedor,
        fonte,
        atualizado_em,
        tipo_documento,
        year,
        month,
        day,
        ROW_NUMBER() OVER (PARTITION BY documento, numero_titulo, data_emissao, data_vencimento, fonte, fornecedor ORDER BY year DESC, month DESC, day DESC) AS rn
    FROM miniotrusted.payments.boletos 
    WHERE substring(documento, 1, 8) IN {ids_query} AND fonte = 'HP_EXTERNA'
)
SELECT 
    *
FROM CTE
WHERE rn = 1"""
    
    print(f'quantidade de CNPJs a serem atualziados: {len(ids_query)}')
    print(f"query: {query}")
    
    base = query_trino(query, 
                                access_params['trino_endpoint'],
                                access_params['trino_port'],
                                access_params['trino_user'],
                                access_params['trino_password'])
    base = base.drop('rn', axis = 1)
# Chamando as variáveis
       
    qtde_titulos_abertos_vencidos = copy.copy(base)
    qtde_titulos_abertos_vencidos = calculo_qtde_titulos_abertos_vencidos(qtde_titulos_abertos_vencidos['data_hp'], qtde_titulos_abertos_vencidos)

    valor_titulos_abertos_vencidos = copy.copy(base)
    valor_titulos_abertos_vencidos = calculo_valor_titulos_abertos_vencidos(valor_titulos_abertos_vencidos['data_hp'], valor_titulos_abertos_vencidos)   

    qtde_titulos_abertos_a_vencer = copy.copy(base)
    qtde_titulos_abertos_a_vencer = calculo_qtde_titulos_abertos_a_vencer(qtde_titulos_abertos_a_vencer['data_hp'], qtde_titulos_abertos_a_vencer)  

    valor_titulos_abertos_a_vencer = copy.copy(base)
    valor_titulos_abertos_a_vencer = calculo_valor_titulos_abertos_a_vencer(valor_titulos_abertos_a_vencer['data_hp'], valor_titulos_abertos_a_vencer) 

    prazo_medio_abertos_vencidos = copy.copy(base)
    prazo_medio_abertos_vencidos = calculo_prazo_medio_abertos_vencidos(prazo_medio_abertos_vencidos['data_hp'], prazo_medio_abertos_vencidos) 

    prazo_medio_abertos_a_vencer = copy.copy(base)
    prazo_medio_abertos_a_vencer = calculo_prazo_medio_abertos_a_vencer(prazo_medio_abertos_a_vencer['data_hp'], prazo_medio_abertos_a_vencer) 

    qtde_titulos_liquidados = copy.copy(base)
    qtde_titulos_liquidados = calculo_qtde_titulos_liquidados(qtde_titulos_liquidados['data_pagamento'], qtde_titulos_liquidados) 

    valor_titulos_liquidados = copy.copy(base)
    valor_titulos_liquidados = calculo_valor_titulos_liquidados(valor_titulos_liquidados['data_pagamento'], valor_titulos_liquidados) 

    prazo_medio_titulos_liquidados = copy.copy(base)
    prazo_medio_titulos_liquidados = calculo_prazo_medio_titulos_liquidados(prazo_medio_titulos_liquidados['data_pagamento'], prazo_medio_titulos_liquidados)  

    atraso_medio_titulos_liquidados = copy.copy(base)
    atraso_medio_titulos_liquidados = calculo_atraso_medio_titulos_liquidados(atraso_medio_titulos_liquidados['data_pagamento'], atraso_medio_titulos_liquidados)  

    atraso_max_titulos_liquidados = copy.copy(base)
    atraso_max_titulos_liquidados = calculo_atraso_max_titulos_liquidados(atraso_max_titulos_liquidados['data_pagamento'], atraso_max_titulos_liquidados)  

    atraso_min_titulos_liquidados = copy.copy(base)
    atraso_min_titulos_liquidados = calculo_atraso_min_titulos_liquidados(atraso_min_titulos_liquidados['data_pagamento'], atraso_min_titulos_liquidados) 

    
    #compilando as variaveis
    dfs_inter = [
        valor_titulos_abertos_vencidos,
        qtde_titulos_abertos_a_vencer,
        valor_titulos_abertos_a_vencer,
        prazo_medio_abertos_vencidos,
        prazo_medio_abertos_a_vencer,
        qtde_titulos_liquidados,
        valor_titulos_liquidados,
        prazo_medio_titulos_liquidados,
        atraso_medio_titulos_liquidados,
        atraso_max_titulos_liquidados,
        atraso_min_titulos_liquidados]
    
    for df_inter in dfs_inter:
        qtde_titulos_abertos_vencidos = pd.merge(qtde_titulos_abertos_vencidos, df_inter, on=['documento_raiz', 'fornecedor'], how='outer')
        
    df_final = qtde_titulos_abertos_vencidos
        
        
    colunas_float = [
        'qtde_titulos_abertos_vencidos',
        'valor_titulos_abertos_vencidos',
        'qtde_titulos_abertos_a_vencer',
        'valor_titulos_abertos_a_vencer',
        'prazo_medio_abertos_vencidos',
        'prazo_medio_abertos_a_vencer',
        'qtde_titulos_liquidados',
        'valor_titulos_liquidados',
        'prazo_medio_titulos_liquidados',
        'atraso_medio_titulos_liquidados',
        'atraso_max_titulos_liquidados',
        'atraso_min_titulos_liquidados'
    ]

    df_final[colunas_float] = df_final[colunas_float].astype('float')


        
    #Definindo data de tratamento do arquivo
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    
    df_final['year'] = now.year
    df_final['month'] = now.month
    df_final['day'] = now.day
        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    
    # O pandas cria esse index, este codigo serve para remover caso ele crie
    if "__index_level_0__" in df_final.columns:
        df_final = df_final.drop(["__index_level_0__"])
    
    write_deltalake(f"s3a://{BUCKET_SOURCE_REFINED}/{REFINED_FOLDER}", 
                    df_final, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )