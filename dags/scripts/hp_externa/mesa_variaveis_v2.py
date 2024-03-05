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
    repositorio_dado['safra'] = repositorio_dado[data_referencia].dt.strftime('%y-%m-01')
    repositorio_dado = repositorio_dado.groupby(['fornecedor',]).agg({
        'safra': ['min', 'max']
    }).reset_index()
    repositorio_dado.columns = ['fornecedor', 'inicio', 'fim']
    repositorio_dado['inicio'] = pd.to_datetime(repositorio_dado['inicio'])
    repositorio_dado['fim'] = pd.to_datetime(repositorio_dado['fim'])
    
    lista_safra = []

    for index, row in repositorio_dado.iterrows():
        datas_safra = pd.date_range(start=row['inicio'], end=row['fim'], freq='ms')
        for data in datas_safra:
            lista_safra.append({'fornecedor': row['fornecedor'], 'safra': data})

    df_safra = pd.dataframe(lista_safra)
    return df_safra

def calculo_vop_mensal (data_referencia, repositorio_dado):
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['safra'] = repositorio_dado[data_referencia].dt.strftime('%y-%m-01')
    repositorio_dado = repositorio_dado.groupby(['documento', 'fornecedor','safra']).agg({
    'valor_titulo': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['documento', 'fornecedor', 'safra', 'vop']
    repositorio_dado['safra'] = pd.to_datetime(repositorio_dado['safra'])
    return repositorio_dado

def calculo_vop_mensal_a_vista (data_referencia, repositorio_dado):
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['safra'] = repositorio_dado[data_referencia].dt.strftime('%y-%m-01')
    repositorio_dado['vop_a_vista'] = np.where(repositorio_dado['data_emissao'] == repositorio_dado['data_vencimento'], repositorio_dado['valor_titulo'], 0)
    repositorio_dado = repositorio_dado.groupby(['documento', 'fornecedor','safra']).agg({
    'vop_a_vista': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['documento', 'fornecedor', 'safra', 'vop_a_vista']
    repositorio_dado['safra'] = pd.to_datetime(repositorio_dado['safra'])
    return repositorio_dado

def calculo_prazo_medio_mensal (data_referencia, repositorio_dado):
    repositorio_dado['data_emissao'] = pd.to_datetime(repositorio_dado['data_emissao'])
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado['safra'] = repositorio_dado[data_referencia].dt.strftime('%y-%m-01')
    repositorio_dado['dif_dias'] = (repositorio_dado['data_vencimento'] - repositorio_dado['data_emissao']).dt.days
    repositorio_dado = repositorio_dado.groupby(['documento', 'fornecedor','safra']).agg({
    'dif_dias': 'mean'
    }).reset_index()
    repositorio_dado.columns = ['documento', 'fornecedor', 'safra', 'prazo_medio']
    repositorio_dado['safra'] = pd.to_datetime(repositorio_dado['safra'])
    return repositorio_dado

def calculo_pagos_em_dia_mensal (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[pd.notna(repositorio_dado[data_referencia])]
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['data_emissao'] = pd.to_datetime(repositorio_dado['data_emissao'])
    repositorio_dado['safra'] = repositorio_dado['data_emissao'].dt.strftime('%y-%m-01')
    repositorio_dado['vop_pago_em_dia'] = np.where(repositorio_dado[data_referencia] <= repositorio_dado['data_vencimento'], repositorio_dado['valor_titulo'], 0)
    repositorio_dado = repositorio_dado.groupby(['documento', 'fornecedor','safra']).agg({
    'vop_pago_em_dia': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['documento', 'fornecedor', 'safra', 'vop_pago_em_dia']
    repositorio_dado['safra'] = pd.to_datetime(repositorio_dado['safra'])
    return repositorio_dado

def calculo_valor_vencido_mensal (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['data_vencimento'] < repositorio_dado[data_referencia]]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['data_pagamento'])]
    repositorio_dado['data_vencimento'] = pd.to_datetime(repositorio_dado['data_vencimento'])
    repositorio_dado[data_referencia] = pd.to_datetime(repositorio_dado[data_referencia])
    repositorio_dado['dias_vencidos'] = (repositorio_dado[data_referencia] - repositorio_dado['data_vencimento']).dt.days
    repositorio_dado['data_emissao'] = pd.to_datetime(repositorio_dado['data_emissao'])
    repositorio_dado['safra'] = repositorio_dado['data_emissao'].dt.strftime('%y-%m-01')
    def categorizar_faixa(dias_vencidos):
        if dias_vencidos <= 5:
            return '01 - até 5 dias'
        elif dias_vencidos <= 10:
            return '02 - 6 - 10 dias'
        elif dias_vencidos <= 15:
            return '03 - 11 - 15 dias'
        elif dias_vencidos <= 30:
            return '04 - 16 - 30 dias'
        elif dias_vencidos <= 60:
            return '05 - 31 - 60 dias'
        elif dias_vencidos <= 90:
            return '06 - 61 - 90 dias'
        else:
            return '07 - acima 90 dias'
    repositorio_dado['faixa_vencidos'] = repositorio_dado['dias_vencidos'].apply(categorizar_faixa)
    repositorio_dado = repositorio_dado.groupby(['documento', 'fornecedor','safra', 'faixa_vencidos']).agg({
    'valor_titulo': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['documento', 'fornecedor', 'safra', 'faixa_vencidos', 'vop_vencido']
    repositorio_dado['safra'] = pd.to_datetime(repositorio_dado['safra'])
    return repositorio_dado

def calculo_valor_a_vencer_mensal (data_referencia, repositorio_dado):
    repositorio_dado = repositorio_dado[repositorio_dado['data_vencimento'] > repositorio_dado[data_referencia]]
    repositorio_dado = repositorio_dado[pd.isna(repositorio_dado['data_pagamento'])]
    repositorio_dado['data_emissao'] = pd.to_datetime(repositorio_dado['data_emissao'])
    repositorio_dado['safra'] = repositorio_dado['data_emissao'].dt.strftime('%y-%m-01')
    repositorio_dado = repositorio_dado.groupby(['documento', 'fornecedor','safra']).agg({
    'valor_titulo': 'sum'
    }).reset_index()
    repositorio_dado.columns = ['documento', 'fornecedor', 'safra', 'vop_a_vencer']
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
       
    aux_periodo = copy.copy(base)
    aux_periodo = aux_periodo[['documento','fornecedor']].drop_duplicates()
    periodo_intervalo_hp_fornecedor = copy.copy(base)
    periodo_intervalo_hp_fornecedor = intervalo_hp_fornecedor('data_emissao', periodo_intervalo_hp_fornecedor)
    periodo_intervalo_hp_fornecedor = periodo_intervalo_hp_fornecedor.merge(aux_periodo, on = ['fornecedor'], how = 'left')
    periodo_intervalo_hp_fornecedor
    print(f"Código para 'periodo_intervalo_hp_fornecedor' executado com sucesso!")

    vop_mensal = copy.copy(base)
    vop_mensal = calculo_vop_mensal('data_emissao', vop_mensal)
    vop_mensal_safra = periodo_intervalo_hp_fornecedor.merge(vop_mensal, on = ['fornecedor','safra', 'documento'], how = 'left')
    vop_mensal_safra = vop_mensal_safra.groupby(['documento', 'fornecedor', 'safra']).agg({
        'vop': 'sum'
    }).reset_index()
    vop_mensal_safra.columns = ['documento', 'fornecedor', 'safra', 'vop']
    vop_mensal_safra['vop'] = np.where(vop_mensal_safra['vop'] == 0, np.nan, vop_mensal_safra['vop'])
    vop_mensal_safra
    print(f"Código para 'vop_mensal_safra' executado com sucesso!")


    vop_mensal_a_vista = copy.copy(base)
    vop_mensal_a_vista = calculo_vop_mensal_a_vista('data_emissao', vop_mensal_a_vista)
    vop_mensal_a_vista = periodo_intervalo_hp_fornecedor.merge(vop_mensal_a_vista, on = ['fornecedor','safra', 'documento'], how = 'left')
    vop_mensal_a_vista = vop_mensal_a_vista.groupby(['documento', 'fornecedor','safra']).agg({
        'vop_a_vista': 'sum'
        }).reset_index()
    vop_mensal_a_vista.columns = ['documento', 'fornecedor', 'safra', 'vop_a_vista']
    vop_mensal_a_vista['vop_a_vista'] = np.where(vop_mensal_a_vista['vop_a_vista'] == 0, np.nan, vop_mensal_a_vista['vop_a_vista'])
    vop_mensal_a_vista


    prazo_medio_mensal = copy.copy(base)
    prazo_medio_mensal = calculo_prazo_medio_mensal('data_emissao', prazo_medio_mensal)
    prazo_medio_mensal = periodo_intervalo_hp_fornecedor.merge(prazo_medio_mensal, on = ['fornecedor','safra', 'documento'], how = 'left')
    prazo_medio_mensal = prazo_medio_mensal.groupby(['documento', 'fornecedor','safra']).agg({
        'prazo_medio': 'sum'
        }).reset_index()
    prazo_medio_mensal.columns = ['documento', 'fornecedor', 'safra', 'prazo_medio']
    prazo_medio_mensal['prazo_medio'] = np.where(prazo_medio_mensal['prazo_medio'] == 0, np.nan, prazo_medio_mensal['prazo_medio'])
    prazo_medio_mensal


    vop_pago_em_dia_mensal = copy.copy(base)
    vop_pago_em_dia_mensal = calculo_pagos_em_dia_mensal('data_pagamento', vop_pago_em_dia_mensal)
    vop_pago_em_dia_mensal = periodo_intervalo_hp_fornecedor.merge(vop_pago_em_dia_mensal, on = ['fornecedor','safra', 'documento'], how = 'left')
    vop_pago_em_dia_mensal = vop_pago_em_dia_mensal.groupby(['documento', 'fornecedor','safra']).agg({
        'vop_pago_em_dia': 'sum'
        }).reset_index()
    vop_pago_em_dia_mensal.columns = ['documento', 'fornecedor', 'safra', 'vop_pago_em_dia']
    vop_pago_em_dia_mensal['vop_pago_em_dia'] = np.where(vop_pago_em_dia_mensal['vop_pago_em_dia'] == 0, np.nan, vop_pago_em_dia_mensal['vop_pago_em_dia'])
    vop_pago_em_dia_mensal


    valor_vencido_mensal = copy.copy(base)
    valor_vencido_mensal = calculo_valor_vencido_mensal('data_hp', valor_vencido_mensal)
    valor_vencido_mensal = periodo_intervalo_hp_fornecedor.merge(valor_vencido_mensal, on = ['fornecedor','safra', 'documento'], how = 'left')
    valor_vencido_mensal = valor_vencido_mensal.groupby(['documento', 'fornecedor','safra','faixa_vencidos']).agg({
        'vop_vencido': 'sum'
        }).reset_index()
    valor_vencido_mensal.columns = ['documento', 'fornecedor', 'safra', 'faixa_vencidos', 'vop_vencido']
    valor_vencido_mensal['categoria'] = valor_vencido_mensal['faixa_vencidos'].str.split(' - ').str[0]
    valor_vencido_mensal['safra'] = valor_vencido_mensal['safra'].astype(str) 
    valor_vencido_mensal = valor_vencido_mensal.pivot_table(index=['documento', 'fornecedor', 'safra'], columns='categoria', values='vop_vencido', aggfunc='sum', fill_value=none)
    valor_vencido_mensal = valor_vencido_mensal.reset_index()
    nomes = {'01': '01_ate_5_dias',
            '02': '02_6_10_dias',
            '03': '03_11_15_dias',
            '04': '04_16_30_dias',
            '05': '05_31_60_dias',
            '06': '06_61_90_dias',
            '07': '07_acima_90_dias'}
    valor_vencido_mensal = valor_vencido_mensal.rename(columns=nomes)
    valor_vencido_mensal['vencido_total'] = valor_vencido_mensal[['01_ate_5_dias', '02_6_10_dias', '03_11_15_dias', '04_16_30_dias', '05_31_60_dias', '06_61_90_dias','07_acima_90_dias']].sum(axis=1)
    valor_vencido_mensal['safra'] = pd.to_datetime(valor_vencido_mensal['safra'])
    valor_vencido_mensal.replace(0, pd.na, inplace=true)
    valor_vencido_mensal


    valor_a_vencer_mensal = copy.copy(base)
    valor_a_vencer_mensal = calculo_valor_a_vencer_mensal('data_hp', valor_a_vencer_mensal)
    valor_a_vencer_mensal = periodo_intervalo_hp_fornecedor.merge(valor_a_vencer_mensal, on = ['fornecedor','safra', 'documento'], how = 'left')
    valor_a_vencer_mensal = valor_a_vencer_mensal.groupby(['documento', 'fornecedor','safra']).agg({
        'vop_a_vencer': 'sum'
        }).reset_index()
    valor_a_vencer_mensal.columns = ['documento', 'fornecedor', 'safra', 'vop_a_vencer']
    valor_a_vencer_mensal['vop_a_vencer'] = np.where(valor_a_vencer_mensal['vop_a_vencer'] == 0, np.nan, valor_a_vencer_mensal['vop_a_vencer'])
    valor_a_vencer_mensal
    

    #compilando as variaveis
    dfs_inter = [
        vop_mensal_a_vista,
        prazo_medio_mensal,
        vop_pago_em_dia_mensal,
        valor_vencido_mensal,
        valor_a_vencer_mensal]
    

    df_final = vop_mensal_safra

    for df_inter in dfs_inter:
        df_final = pd.merge(df_final, df_inter, on=['documento', 'fornecedor', 'safra'], how='outer')
        
        
    colunas_float = [
        'vop',
        'vop_a_vista',
        'prazo_medio',
        'vop_pago_em_dia',
        '01_ate_5_dias',
        '02_6_10_dias',
        '03_11_15_dias',
        '04_16_30_dias',
        '05_31_60_dias',
        '06_61_90_dias',
        '07_acima_90_dias',
        'vencidos_total',
        'vop_a_vencer'
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