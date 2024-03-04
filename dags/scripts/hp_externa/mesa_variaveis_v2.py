# Carregando libs
import pandas as pd
import numpy as np
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable

# Calculando Variáveis
def intervalo_hp_fornecedor(data_referencia, repositorio_dado):
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['SAFRA'] = repositorio_dado[data_referencia].dt.strftime('%Y-%m-01')
    repositorio_dado = repositorio_dado.groupby(['FORNECEDOR']).agg({
        'SAFRA': ['min', 'max']
    }).reset_index()
    repositorio_dado.columns = ['FORNECEDOR', 'INICIO', 'FIM']
    repositorio_dado['INICIO'] = pd.to_datetime(repositorio_dado['INICIO'])
    repositorio_dado['FIM'] = pd.to_datetime(repositorio_dado['FIM'])
    
    lista_safra = []

    for index, row in repositorio_dado.iterrows():
        datas_safra = pd.date_range(start=row['INICIO'], end=row['FIM'], freq='MS')
        for data in datas_safra:
            lista_safra.append({'FORNECEDOR': row['FORNECEDOR'], 'SAFRA': data})

    df_safra = pd.DataFrame(lista_safra)
    return df_safra

def calculo_vop_mensal (data_referencia, repositorio_dado):
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['SAFRA'] = repositorio_dado[data_referencia].dt.strftime('%Y-%m-01')
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'VOP']
    repositorio_dado['SAFRA'] = pd.to_datetime(repositorio_dado['SAFRA'])
    return repositorio_dado

def calculo_vop_mensal_a_vista (data_referencia, repositorio_dado):
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['SAFRA'] = repositorio_dado[data_referencia].dt.strftime('%Y-%m-01')
    repositorio_dado['VOP_A_VISTA'] = np.where(repositorio_dado['DATA_EMISSAO'] == repositorio_dado['DATA_VENCIMENTO'], repositorio_dado['VALOR_TITULO'], 0)
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA']).agg({
    'VOP_A_VISTA': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'VOP_A_VISTA']
    repositorio_dado['SAFRA'] = pd.to_datetime(repositorio_dado['SAFRA'])
    return repositorio_dado

def calculo_prazo_medio_mensal (data_referencia, repositorio_dado):
    repositorio_dado['DATA_EMISSAO'] = pd.to_datetime(repositorio_dado['DATA_EMISSAO'])
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado['SAFRA'] = repositorio_dado[data_referencia].dt.strftime('%Y-%m-01')
    repositorio_dado['DIF_DIAS'] = (repositorio_dado['DATA_VENCIMENTO'] - repositorio_dado['DATA_EMISSAO']).dt.days
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA']).agg({
    'DIF_DIAS': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'PRAZO_MEDIO']
    repositorio_dado['SAFRA'] = pd.to_datetime(repositorio_dado['SAFRA'])
    return repositorio_dado

def calculo_pagos_em_dia_mensal (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(repositorio_dado[data_referencia])]
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['DATA_EMISSAO'] = pd.to_datetime(repositorio_dado['DATA_EMISSAO'])
    repositorio_dado['SAFRA'] = repositorio_dado['DATA_EMISSAO'].dt.strftime('%Y-%m-01')
    repositorio_dado['VOP_PAGO_EM_DIA'] = np.where(repositorio_dado[data_referencia] <= repositorio_dado['DATA_VENCIMENTO'], repositorio_dado['VALOR_TITULO'], 0)
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA']).agg({
    'VOP_PAGO_EM_DIA': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'VOP_PAGO_EM_DIA']
    repositorio_dado['SAFRA'] = pd.to_datetime(repositorio_dado['SAFRA'])
    return repositorio_dado

def calculo_valor_vencido_mensal (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['DATA_VENCIMENTO'] < repositorio_dado[data_referencia]]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['DATA_PAGAMENTO'])]
    repositorio_dado['DATA_VENCIMENTO'] = pd.to_datetime(repositorio_dado['DATA_VENCIMENTO'])
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['DIAS_VENCIDOS'] = (repositorio_dado[data_referencia] - repositorio_dado['DATA_VENCIMENTO']).dt.days
    repositorio_dado['DATA_EMISSAO'] = pd.to_datetime(repositorio_dado['DATA_EMISSAO'])
    repositorio_dado['SAFRA'] = repositorio_dado['DATA_EMISSAO'].dt.strftime('%Y-%m-01')
    def categorizar_faixa(dias_vencidos):
        if dias_vencidos <= 5:
            return '01 - ATÉ 5 DIAS'
        elif dias_vencidos <= 10:
            return '02 - 6 - 10 DIAS'
        elif dias_vencidos <= 15:
            return '03 - 11 - 15 DIAS'
        elif dias_vencidos <= 30:
            return '04 - 16 - 30 DIAS'
        elif dias_vencidos <= 60:
            return '05 - 31 - 60 DIAS'
        elif dias_vencidos <= 90:
            return '06 - 61 - 90 DIAS'
        else:
            return '07 - ACIMA 90 DIAS'

    # Aplicar a função à coluna 'PRAZO_MEDIO_GERAL' para criar uma nova coluna 'FAIXA_PRAZO'
    repositorio_dado['FAIXA_VENCIDOS'] = repositorio_dado['DIAS_VENCIDOS'].apply(categorizar_faixa)
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA', 'FAIXA_VENCIDOS']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'FAIXA_VENCIDOS', 'VOP_VENCIDO']
    repositorio_dado['SAFRA'] = pd.to_datetime(repositorio_dado['SAFRA'])
    return repositorio_dado

def calculo_valor_a_vencer_mensal (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['DATA_VENCIMENTO'] > repositorio_dado[data_referencia]]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['DATA_PAGAMENTO'])]
    repositorio_dado['DATA_EMISSAO'] = pd.to_datetime(repositorio_dado['DATA_EMISSAO'])
    repositorio_dado['SAFRA'] = repositorio_dado['DATA_EMISSAO'].dt.strftime('%Y-%m-01')
    repositorio_dado = repositorio_dado.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'VOP_A_VENCER']
    return repositorio_dado

# Criando conexão
def transform_data_to_refined(files_list, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_TRUSTED = "payments"
    TRUSTED_FOLDER =  "boletos/"
    BUCKET_SOURCE_REFINED = "payments"
    REFINED_FOLDER = "mesa/visao_detalhada/"

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


# Chamando as variáveis
       
    periodo_intervalo_hp_fornecedor = copy.copy(base)
    periodo_intervalo_hp_fornecedor = intervalo_hp_fornecedor('DATA_EMISSAO', periodo_intervalo_hp_fornecedor)


    vop_mensal = copy.copy(base)
    vop_mensal = calculo_vop_mensal('DATA_EMISSAO', vop_mensal)
    vop_mensal_safra = periodo_intervalo_hp_fornecedor.merge(vop_mensal, on = ['FORNECEDOR'], how = 'left')
    vop_mensal_safra['VOP_REAL'] = np.where(vop_mensal_safra['SAFRA_x'] == vop_mensal_safra['SAFRA_y'], vop_mensal_safra['VOP'], np.nan)
    vop_mensal_safra = vop_mensal_safra.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA_x']).agg({
        'VOP_REAL': lambda x: np.nan if x.isnull().all() else x.sum()
        }).reset_index()
    vop_mensal_safra.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'VOP']


    vop_mensal_a_vista = copy.copy(base)
    vop_mensal_a_vista = calculo_vop_mensal_a_vista('DATA_EMISSAO', vop_mensal_a_vista)
    vop_mensal_a_vista = periodo_intervalo_hp_fornecedor.merge(vop_mensal_a_vista, on = ['FORNECEDOR'], how = 'left')
    vop_mensal_a_vista['VOP_A_VISTA_REAL'] = np.where(vop_mensal_a_vista['SAFRA_x'] == vop_mensal_a_vista['SAFRA_y'], vop_mensal_a_vista['VOP_A_VISTA'], np.nan)
    vop_mensal_a_vista = vop_mensal_a_vista.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA_x']).agg({
        'VOP_A_VISTA_REAL': lambda x: np.nan if x.isnull().all() else x.sum()
        }).reset_index()
    vop_mensal_a_vista.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'VOP_A_VISTA']


    prazo_medio_mensal = copy.copy(base)
    prazo_medio_mensal = calculo_prazo_medio_mensal('DATA_EMISSAO', prazo_medio_mensal)
    prazo_medio_mensal = periodo_intervalo_hp_fornecedor.merge(prazo_medio_mensal, on = ['FORNECEDOR'], how = 'left')
    prazo_medio_mensal['PRAZO_MEDIO_REAL'] = np.where(prazo_medio_mensal['SAFRA_x'] == prazo_medio_mensal['SAFRA_y'], prazo_medio_mensal['PRAZO_MEDIO'], np.nan)
    prazo_medio_mensal = prazo_medio_mensal.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA_x']).agg({
        'PRAZO_MEDIO_REAL': lambda x: np.nan if x.isnull().all() else x.sum()
        }).reset_index()
    prazo_medio_mensal.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'PRAZO_MEDIO']


    vop_pago_em_dia_mensal = copy.copy(base)
    vop_pago_em_dia_mensal = calculo_pagos_em_dia_mensal('DATA_PAGAMENTO', vop_pago_em_dia_mensal)
    vop_pago_em_dia_mensal = periodo_intervalo_hp_fornecedor.merge(vop_pago_em_dia_mensal, on = ['FORNECEDOR'], how = 'left')
    vop_pago_em_dia_mensal['VOP_PAGO_EM_DIA_REAL'] = np.where(vop_pago_em_dia_mensal['SAFRA_x'] == vop_pago_em_dia_mensal['SAFRA_y'], vop_pago_em_dia_mensal['VOP_PAGO_EM_DIA'], np.nan)
    vop_pago_em_dia_mensal = vop_pago_em_dia_mensal.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA_x']).agg({
        'VOP_PAGO_EM_DIA_REAL': lambda x: np.nan if x.isnull().all() else x.sum()
        }).reset_index()
    vop_pago_em_dia_mensal.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'VOP_PAGO_EM_DIA']


    valor_vencido_mensal = copy.copy(base)
    valor_vencido_mensal = calculo_valor_vencido_mensal('DATA_HP', valor_vencido_mensal)
    valor_vencido_mensal = periodo_intervalo_hp_fornecedor.merge(valor_vencido_mensal, on = ['FORNECEDOR'], how = 'left')
    valor_vencido_mensal['VOP_VENCIDO_REAL'] = np.where(valor_vencido_mensal['SAFRA_x'] == valor_vencido_mensal['SAFRA_y'], valor_vencido_mensal['VOP_VENCIDO'], np.nan)
    valor_vencido_mensal = valor_vencido_mensal.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA_x','FAIXA_VENCIDOS']).agg({
        'VOP_VENCIDO_REAL': lambda x: np.nan if x.isnull().all() else x.sum()
        }).reset_index()
    valor_vencido_mensal.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'FAIXA_VENCIDOS', 'VOP_VENCIDO']
    valor_vencido_mensal['CATEGORIA'] = valor_vencido_mensal['FAIXA_VENCIDOS'].str.split(' - ').str[0]
    valor_vencido_mensal = valor_vencido_mensal.pivot_table(index=['DOCUMENTO', 'FORNECEDOR', 'SAFRA'], columns='CATEGORIA', values='VOP_VENCIDO', aggfunc='sum', fill_value=None)
    valor_vencido_mensal = valor_vencido_mensal.reset_index()
    nomes = {'01':'01_ATE_5_DIAS',
            '02': '02_6_10_DIAS',
            '03': '03_11_15_DIAS',
            '04': '04_16_30_DIAS',
            '05':'05_31_60_DIAS',
            '06': '06_61_90_DIAS',
            '07': '07_ACIMA_90_DIAS'}
    valor_vencido_mensal = valor_vencido_mensal.rename(columns=nomes)
    valor_vencido_mensal['VENCIDO_TOTAL'] = valor_vencido_mensal[['01_ATE_5_DIAS', '02_6_10_DIAS', '03_11_15_DIAS', '04_16_30_DIAS', '05_31_60_DIAS', '06_61_90_DIAS','07_ACIMA_90_DIAS']].sum(axis=1)


    valor_a_vencer_mensal = copy.copy(base)
    valor_a_vencer_mensal = calculo_valor_a_vencer_mensal('DATA_HP', valor_a_vencer_mensal)
    valor_a_vencer_mensal = periodo_intervalo_hp_fornecedor.merge(valor_a_vencer_mensal, on = ['FORNECEDOR'], how = 'left')
    valor_a_vencer_mensal['VOP_A_VENCER_REAL'] = np.where(valor_a_vencer_mensal['SAFRA_x'] == valor_a_vencer_mensal['SAFRA_y'], valor_a_vencer_mensal['VOP_A_VENCER'], np.nan)
    valor_a_vencer_mensal = valor_a_vencer_mensal.groupby(['DOCUMENTO', 'FORNECEDOR','SAFRA_x']).agg({
        'VOP_A_VENCER_REAL': lambda x: np.nan if x.isnull().all() else x.sum()
        }).reset_index()
    valor_a_vencer_mensal.columns = ['DOCUMENTO', 'FORNECEDOR', 'SAFRA', 'VOP_A_VENCER']
    

    #compilando as variaveis
    dfs_inter = [
        vop_mensal_a_vista,
        prazo_medio_mensal,
        vop_pago_em_dia_mensal,
        valor_vencido_mensal,
        valor_a_vencer_mensal]
    

    df_final = vop_mensal_safra

    for df_inter in dfs_inter:
        df_final = pd.merge(df_final, df_inter, on=['DOCUMENTO', 'FORNECEDOR', 'SAFRA'], how='outer')
        
        
    colunas_float = [
        'VOP',
        'VOP_A_VISTA',
        'PRAZO_MEDIO',
        'VOP_PAGO_EM_DIA',
        '01_ATE_5_DIAS',
        '02_6_10_DIAS',
        '03_11_15_DIAS',
        '04_16_30_DIAS',
        '05_31_60_DIAS',
        '06_61_90_DIAS',
        '07_ACIMA_90_DIAS',
        'VENCIDOS_TOTAL',
        'VOP_A_VENCER'
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