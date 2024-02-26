# Carregando libs
import pandas as pd
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable

# Calculando Variáveis
def calculo_qtde_titulos_abertos_vencidos (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['DATA_VENCIMENTO'] < data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['DATA_PAGAMENTO'])]
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'NUMERO_TITULO': 'count'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'QTDE_TITULOS_ABERTOS_VENCIDOS']
    return repositorio_dado

def calculo_valor_titulos_abertos_vencidos (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['DATA_VENCIMENTO'] < data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['DATA_PAGAMENTO'])]
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'VALOR_TITULOS_ABERTOS_VENCIDOS']
    return repositorio_dado

def calculo_qtde_titulos_abertos_a_vencer (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['DATA_VENCIMENTO'] >= data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['DATA_PAGAMENTO'])]
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'NUMERO_TITULO': 'count'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'QTDE_TITULOS_ABERTOS_A_VENCER']
    return repositorio_dado

def calculo_valor_titulos_abertos_a_vencer (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['DATA_VENCIMENTO'] >= data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['DATA_PAGAMENTO'])]
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'VALOR_TITULOS_ABERTOS_A_VENCER']
    return repositorio_dado

def calculo_prazo_medio_abertos_vencidos (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['DATA_VENCIMENTO'] < data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['DATA_PAGAMENTO'])]
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado['DATA_EMISSAO'] = pd.to_datetime(repositorio_dado['DATA_EMISSAO'])
    repositorio_dado['AUX_PRAZO_MEDIO'] = (repositorio_dado['DATA_VENCIMENTO'] - repositorio_dado['DATA_EMISSAO']).dt.days
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'AUX_PRAZO_MEDIO': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'PRAZO_MEDIO_ABERTOS_VENCIDOS']
    return repositorio_dado

def calculo_prazo_medio_abertos_a_vencer (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['DATA_VENCIMENTO'] >= data_referencia]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['DATA_PAGAMENTO'])]
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado['DATA_EMISSAO'] = pd.to_datetime(repositorio_dado['DATA_EMISSAO'])
    repositorio_dado['AUX_PRAZO_MEDIO'] = (repositorio_dado['DATA_VENCIMENTO'] - repositorio_dado['DATA_EMISSAO']).dt.days
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'AUX_PRAZO_MEDIO': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'PRAZO_MEDIO_ABERTOS_A_VENCER']
    return repositorio_dado

def calculo_qtde_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'NUMERO_TITULO': 'count'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'QTDE_TITULOS_LIQUIDADOS']
    return repositorio_dado

def calculo_valor_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'VALOR_TITULOS_LIQUIDADOS']
    return repositorio_dado

def calculo_prazo_medio_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado['DATA_EMISSAO'] = pd.to_datetime(repositorio_dado['DATA_EMISSAO'])
    repositorio_dado['AUX_PRAZO_MEDIO'] = (repositorio_dado['DATA_VENCIMENTO'] - repositorio_dado['DATA_EMISSAO']).dt.days
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'AUX_PRAZO_MEDIO': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'PRAZO_MEDIO_TITULOS_LIQUIDADOS']
    return repositorio_dado

def calculo_atraso_medio_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado['DATA_PAGAMENTO'] = pd.to_datetime(repositorio_dado['DATA_PAGAMENTO'])
    repositorio_dado['DIF_DIAS_PAGAMENTO'] = (repositorio_dado['DATA_PAGAMENTO'] - repositorio_dado['DATA_VENCIMENTO']).dt.days
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'DIF_DIAS_PAGAMENTO': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'ATRASO_MEDIO_TITULOS_LIQUIDADOS']
    return repositorio_dado

def calculo_atraso_max_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado['DATA_PAGAMENTO'] = pd.to_datetime(repositorio_dado['DATA_PAGAMENTO'])
    repositorio_dado['DIF_DIAS_PAGAMENTO'] = (repositorio_dado['DATA_PAGAMENTO'] - repositorio_dado['DATA_VENCIMENTO']).dt.days
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'DIF_DIAS_PAGAMENTO': 'max'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'ATRASO_MAX_TITULOS_LIQUIDADOS']
    return repositorio_dado

def calculo_atraso_min_titulos_liquidados (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(data_referencia)]
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado['DATA_PAGAMENTO'] = pd.to_datetime(repositorio_dado['DATA_PAGAMENTO'])
    repositorio_dado['DIF_DIAS_PAGAMENTO'] = (repositorio_dado['DATA_PAGAMENTO'] - repositorio_dado['DATA_VENCIMENTO']).dt.days
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO_RAIZ', 'FORNECEDOR']).agg({
    'DIF_DIAS_PAGAMENTO': 'min'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO_RAIZ', 'FORNECEDOR', 'ATRASO_MIN_TITULOS_LIQUIDADOS']
    return repositorio_dado

# Criando conexão
def transform_data_to_refined(files_list, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_TRUSTED = "payments"
    TRUSTED_FOLDER =  "boletos/"
    BUCKET_SOURCE_REFINED = "payments"
    REFINED_FOLDER = "mesa/"

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

    base['DOCUMENTO_RAIZ'] = base['DOCUMENTO'].str[:8]

# Chamando as variáveis
       
    qtde_titulos_abertos_vencidos = copy.copy(base)
    qtde_titulos_abertos_vencidos = calculo_qtde_titulos_abertos_vencidos(qtde_titulos_abertos_vencidos['DATA_HP'], qtde_titulos_abertos_vencidos)

    valor_titulos_abertos_vencidos = copy.copy(base)
    valor_titulos_abertos_vencidos = calculo_valor_titulos_abertos_vencidos(valor_titulos_abertos_vencidos['DATA_HP'], valor_titulos_abertos_vencidos)   

    qtde_titulos_abertos_a_vencer = copy.copy(base)
    qtde_titulos_abertos_a_vencer = calculo_qtde_titulos_abertos_a_vencer(qtde_titulos_abertos_a_vencer['DATA_HP'], qtde_titulos_abertos_a_vencer)  

    valor_titulos_abertos_a_vencer = copy.copy(base)
    valor_titulos_abertos_a_vencer = calculo_valor_titulos_abertos_a_vencer(valor_titulos_abertos_a_vencer['DATA_HP'], valor_titulos_abertos_a_vencer) 

    prazo_medio_abertos_vencidos = copy.copy(base)
    prazo_medio_abertos_vencidos = calculo_prazo_medio_abertos_vencidos(prazo_medio_abertos_vencidos['DATA_HP'], prazo_medio_abertos_vencidos) 

    prazo_medio_abertos_a_vencer = copy.copy(base)
    prazo_medio_abertos_a_vencer = calculo_prazo_medio_abertos_a_vencer(prazo_medio_abertos_a_vencer['DATA_HP'], prazo_medio_abertos_a_vencer) 

    qtde_titulos_liquidados = copy.copy(base)
    qtde_titulos_liquidados = calculo_qtde_titulos_liquidados(qtde_titulos_liquidados['DATA_PAGAMENTO'], qtde_titulos_liquidados) 

    valor_titulos_liquidados = copy.copy(base)
    valor_titulos_liquidados = calculo_valor_titulos_liquidados(valor_titulos_liquidados['DATA_PAGAMENTO'], valor_titulos_liquidados) 

    prazo_medio_titulos_liquidados = copy.copy(base)
    prazo_medio_titulos_liquidados = calculo_prazo_medio_titulos_liquidados(prazo_medio_titulos_liquidados['DATA_PAGAMENTO'], prazo_medio_titulos_liquidados)  

    atraso_medio_titulos_liquidados = copy.copy(base)
    atraso_medio_titulos_liquidados = calculo_atraso_medio_titulos_liquidados(atraso_medio_titulos_liquidados['DATA_PAGAMENTO'], atraso_medio_titulos_liquidados)  

    atraso_max_titulos_liquidados = copy.copy(base)
    atraso_max_titulos_liquidados = calculo_atraso_max_titulos_liquidados(atraso_max_titulos_liquidados['DATA_PAGAMENTO'], atraso_max_titulos_liquidados)  

    atraso_min_titulos_liquidados = copy.copy(base)
    atraso_min_titulos_liquidados = calculo_atraso_min_titulos_liquidados(atraso_min_titulos_liquidados['DATA_PAGAMENTO'], atraso_min_titulos_liquidados) 

    
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
        qtde_titulos_abertos_vencidos = pd.merge(qtde_titulos_abertos_vencidos, df_inter, on=['DOCUMENTO_RAIZ', 'FORNECEDOR'], how='outer')
        
    df_final = qtde_titulos_abertos_vencidos
        
        
    colunas_float = [
        'QTDE_TITULOS_ABERTOS_VENCIDOS',
        'VALOR_TITULOS_ABERTOS_VENCIDOS',
        'QTDE_TITULOS_ABERTOS_A_VENCER',
        'VALOR_TITULOS_ABERTOS_A_VENCER',
        'PRAZO_MEDIO_ABERTOS_VENCIDOS',
        'PRAZO_MEDIO_ABERTOS_A_VENCER',
        'QTDE_TITULOS_LIQUIDADOS',
        'VALOR_TITULOS_LIQUIDADOS',
        'PRAZO_MEDIO_TITULOS_LIQUIDADOS',
        'ATRASO_MEDIO_TITULOS_LIQUIDADOS',
        'ATRASO_MAX_TITULOS_LIQUIDADOS',
        'ATRASO_MIN_TITULOS_LIQUIDADOS'
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

    write_deltalake(f"s3a://{BUCKET_SOURCE_REFINED}/{REFINED_FOLDER}", 
                    df_final, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )