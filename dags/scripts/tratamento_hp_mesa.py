# %% [markdown]
# Carregando Libs

# %%
import pandas as pd
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable


# %% [markdown]
# Carregando conexões

# %%
def transform_data_to_refined(files_list, access_params):

    # VARIAVEIS
    BUCKET_SOURCE_TRUSTED = "payments"
    TRUSTED_FOLDER =  "boletos/"
    BUCKET_SOURCE_REFINED = "payments"
    REFINED_FOLDER = "mesa/"

    df_payments = pd.DataFrame()

    # CONECTAR NO MINIO TRUSTED
    client = Minio(
        access_params['endpoint_url_trusted'],
        access_key = access_params['aws_access_key_id_trusted'],
        secret_key = access_params['aws_secret_access_key_trusted'],
    )
    
    dfs = []

    for file_name in files_list:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_TRUSTED, object_name=file_name)
        df_hpex_raw_temp = pd.read_parquet(BytesIO(file.data))
        dfs.append(df_hpex_raw_temp)
        
    base = pd.concat(dfs, ignore_index=True)

    #base = base[base['FONTE'] == 'hpex']
    # %% [markdown]
    # ## Começando Tratamento dos dados

    # %% [markdown]
    # # Quantidade de títulos em aberto (vencido);

    # %%
    qtde_titulos_abertos_vencidos = copy.copy(base)

    #Pegando somente casos válidos para análise
    qtde_titulos_abertos_vencidos = qtde_titulos_abertos_vencidos[qtde_titulos_abertos_vencidos['DATA_VENCIMENTO'] < qtde_titulos_abertos_vencidos['DATA_HP']]
    qtde_titulos_abertos_vencidos = qtde_titulos_abertos_vencidos[pd.isna(qtde_titulos_abertos_vencidos['DATA_PAGAMENTO'])]
    qtde_titulos_abertos_vencidos = qtde_titulos_abertos_vencidos.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'NUMERO_TITULO': 'count'
    }).reset_index()
    qtde_titulos_abertos_vencidos.columns = ['DOCUMENTO', 'FORNECEDOR', 'QTDE_TITULOS_ABERTOS_VENCIDOS']

    # %% [markdown]
    # # Valor total títulos em aberto(vencido);

    # %%
    valor_titulos_abertos_vencidos = copy.copy(base)
    #Pegando somente casos válidos para análise
    valor_titulos_abertos_vencidos = valor_titulos_abertos_vencidos[valor_titulos_abertos_vencidos['DATA_VENCIMENTO'] < valor_titulos_abertos_vencidos['DATA_HP']]
    valor_titulos_abertos_vencidos = valor_titulos_abertos_vencidos[pd.isna(valor_titulos_abertos_vencidos['DATA_PAGAMENTO'])]
    valor_titulos_abertos_vencidos = valor_titulos_abertos_vencidos.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    valor_titulos_abertos_vencidos.columns = ['DOCUMENTO', 'FORNECEDOR', 'VALOR_TITULOS_ABERTOS_VENCIDOS']


    # %% [markdown]
    # # Quantidade de títulos em aberto (vincendo);

    # %%
    qtde_titulos_abertos_a_vencer = copy.copy(base)
    #Pegando somente casos válidos para análise
    qtde_titulos_abertos_a_vencer = qtde_titulos_abertos_a_vencer[qtde_titulos_abertos_a_vencer['DATA_VENCIMENTO'] > qtde_titulos_abertos_a_vencer['DATA_HP']]
    qtde_titulos_abertos_a_vencer = qtde_titulos_abertos_a_vencer[pd.isna(qtde_titulos_abertos_a_vencer['DATA_PAGAMENTO'])]
    qtde_titulos_abertos_a_vencer = qtde_titulos_abertos_a_vencer.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'NUMERO_TITULO': 'count'
    }).reset_index()
    qtde_titulos_abertos_a_vencer.columns = ['DOCUMENTO', 'FORNECEDOR', 'QTDE_TITULOS_ABERTOS_A_VENCER']


    # %% [markdown]
    # # Valor total de títulos em aberto (vincendo);

    # %%
    valor_titulos_abertos_a_vencer = copy.copy(base)
    #Pegando somente casos válidos para análise
    valor_titulos_abertos_a_vencer = valor_titulos_abertos_a_vencer[valor_titulos_abertos_a_vencer['DATA_VENCIMENTO'] > valor_titulos_abertos_a_vencer['DATA_HP']]
    valor_titulos_abertos_a_vencer = valor_titulos_abertos_a_vencer[pd.isna(valor_titulos_abertos_a_vencer['DATA_PAGAMENTO'])]
    valor_titulos_abertos_a_vencer = valor_titulos_abertos_a_vencer.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    valor_titulos_abertos_a_vencer.columns = ['DOCUMENTO', 'FORNECEDOR', 'VALOR_TITULOS_ABERTOS_A_VENCER']


    # %% [markdown]
    # # Prazo Médio das operações em aberto(vencido);

    # %%
    prazo_medio_abertos_vencidos = copy.copy(base)
    #Pegando somente casos válidos para análise
    prazo_medio_abertos_vencidos = prazo_medio_abertos_vencidos[prazo_medio_abertos_vencidos['DATA_VENCIMENTO'] < prazo_medio_abertos_vencidos['DATA_HP']]
    prazo_medio_abertos_vencidos = prazo_medio_abertos_vencidos[pd.isna(prazo_medio_abertos_vencidos['DATA_PAGAMENTO'])]
    # Criando aux para cálculo prazo médio
    prazo_medio_abertos_vencidos['DATA_VENCIMENTO'] = pd.to_datetime(prazo_medio_abertos_vencidos['DATA_VENCIMENTO'])
    prazo_medio_abertos_vencidos['DATA_EMISSAO'] = pd.to_datetime(prazo_medio_abertos_vencidos['DATA_EMISSAO'])

    prazo_medio_abertos_vencidos['AUX_PRAZO_MEDIO'] = (prazo_medio_abertos_vencidos['DATA_VENCIMENTO'] - prazo_medio_abertos_vencidos['DATA_EMISSAO']).dt.days
    prazo_medio_abertos_vencidos = prazo_medio_abertos_vencidos.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'AUX_PRAZO_MEDIO': 'mean'
    }).reset_index()
    prazo_medio_abertos_vencidos.columns = ['DOCUMENTO', 'FORNECEDOR', 'PRAZO_MEDIO_ABERTOS_VENCIDOS']


    # %% [markdown]
    # # Prazo Médio das operações em aberto(vincendo);

    # %%
    prazo_medio_abertos_a_vencer = copy.copy(base)
    #Pegando somente casos válidos para análise
    prazo_medio_abertos_a_vencer = prazo_medio_abertos_a_vencer[prazo_medio_abertos_a_vencer['DATA_VENCIMENTO'] > prazo_medio_abertos_a_vencer['DATA_HP']]
    prazo_medio_abertos_a_vencer = prazo_medio_abertos_a_vencer[pd.isna(prazo_medio_abertos_a_vencer['DATA_PAGAMENTO'])]
    # Criando aux para cálculo prazo médio
    prazo_medio_abertos_a_vencer['DATA_VENCIMENTO'] = pd.to_datetime(prazo_medio_abertos_a_vencer['DATA_VENCIMENTO'])
    prazo_medio_abertos_a_vencer['DATA_EMISSAO'] = pd.to_datetime(prazo_medio_abertos_a_vencer['DATA_EMISSAO'])

    prazo_medio_abertos_a_vencer['AUX_PRAZO_MEDIO'] = (prazo_medio_abertos_a_vencer['DATA_VENCIMENTO'] - prazo_medio_abertos_a_vencer['DATA_EMISSAO']).dt.days
    prazo_medio_abertos_a_vencer = prazo_medio_abertos_a_vencer.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'AUX_PRAZO_MEDIO': 'mean'
    }).reset_index()
    prazo_medio_abertos_a_vencer.columns = ['DOCUMENTO', 'FORNECEDOR', 'PRAZO_MEDIO_ABERTOS_A_VENCER']


    # %% [markdown]
    # # Quantidade de títulos liquidados;

    # %%
    qtde_titulos_liquidados = copy.copy(base)
    #Pegando somente casos válidos para análise
    qtde_titulos_liquidados = qtde_titulos_liquidados[pd.notna(qtde_titulos_liquidados['DATA_PAGAMENTO'])]
    qtde_titulos_liquidados = qtde_titulos_liquidados.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'NUMERO_TITULO': 'count'
    }).reset_index()
    qtde_titulos_liquidados.columns = ['DOCUMENTO', 'FORNECEDOR', 'QTDE_TITULOS_LIQUIDADOS']

    qtde_titulos_liquidados = qtde_titulos_liquidados.astype('float')

    # %% [markdown]
    # # Valor de títulos liquidados;

    # %%
    valor_titulos_liquidados = copy.copy(base)
    #Pegando somente casos válidos para análise
    valor_titulos_liquidados = valor_titulos_liquidados[pd.notna(valor_titulos_liquidados['DATA_PAGAMENTO'])]
    valor_titulos_liquidados = valor_titulos_liquidados.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'VALOR_TITULO': 'sum'
    }).reset_index()
    valor_titulos_liquidados.columns = ['DOCUMENTO', 'FORNECEDOR', 'VALOR_TITULOS_LIQUIDADOS']


    # %% [markdown]
    # # Prazo Médio dos títulos liquidados;

    # %%
    prazo_medio_titulos_liquidados = copy.copy(base)
    #Pegando somente casos válidos para análise
    prazo_medio_titulos_liquidados = prazo_medio_titulos_liquidados[pd.notna(prazo_medio_titulos_liquidados['DATA_PAGAMENTO'])]
    # Criando aux para cálculo prazo médio
    prazo_medio_titulos_liquidados['DATA_VENCIMENTO'] = pd.to_datetime(prazo_medio_titulos_liquidados['DATA_VENCIMENTO'])
    prazo_medio_titulos_liquidados['DATA_EMISSAO'] = pd.to_datetime(prazo_medio_titulos_liquidados['DATA_EMISSAO'])

    prazo_medio_titulos_liquidados['AUX_PRAZO_MEDIO'] = (prazo_medio_titulos_liquidados['DATA_VENCIMENTO'] - prazo_medio_titulos_liquidados['DATA_EMISSAO']).dt.days
    prazo_medio_titulos_liquidados = prazo_medio_titulos_liquidados.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'AUX_PRAZO_MEDIO': 'mean'
    }).reset_index()
    prazo_medio_titulos_liquidados.columns = ['DOCUMENTO', 'FORNECEDOR', 'PRAZO_MEDIO_TITULOS_LIQUIDADOS']


    # %% [markdown]
    # # Atraso Médio liquidados;

    # %%
    atraso_medio_titulos_liquidados = copy.copy(base)
    #Pegando somente casos válidos para análise
    atraso_medio_titulos_liquidados = atraso_medio_titulos_liquidados[pd.notna(atraso_medio_titulos_liquidados['DATA_PAGAMENTO'])]
    # Criando aux para cálculo prazo médio
    atraso_medio_titulos_liquidados['DATA_VENCIMENTO'] = pd.to_datetime(atraso_medio_titulos_liquidados['DATA_VENCIMENTO'])
    atraso_medio_titulos_liquidados['DATA_PAGAMENTO'] = pd.to_datetime(atraso_medio_titulos_liquidados['DATA_PAGAMENTO'])

    # Calculo Diferenças Dias (Pagamento - Vencimento)
    atraso_medio_titulos_liquidados['DIF_DIAS_PAGAMENTO'] = (atraso_medio_titulos_liquidados['DATA_PAGAMENTO'] - atraso_medio_titulos_liquidados['DATA_VENCIMENTO']).dt.days

    atraso_medio_titulos_liquidados = atraso_medio_titulos_liquidados.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'DIF_DIAS_PAGAMENTO': 'mean'
    }).reset_index()
    atraso_medio_titulos_liquidados.columns = ['DOCUMENTO', 'FORNECEDOR', 'ATRASO_MEDIO_TITULOS_LIQUIDADOS']


    # %% [markdown]
    # # Atraso Max liquidados;

    # %%
    atraso_max_titulos_liquidados = copy.copy(base)
    #Pegando somente casos válidos para análise
    atraso_max_titulos_liquidados = atraso_max_titulos_liquidados[pd.notna(atraso_max_titulos_liquidados['DATA_PAGAMENTO'])]
    # Criando aux para cálculo prazo médio
    atraso_max_titulos_liquidados['DATA_VENCIMENTO'] = pd.to_datetime(atraso_max_titulos_liquidados['DATA_VENCIMENTO'])
    atraso_max_titulos_liquidados['DATA_PAGAMENTO'] = pd.to_datetime(atraso_max_titulos_liquidados['DATA_PAGAMENTO'])

    # Calculo Diferenças Dias (Pagamento - Vencimento)
    atraso_max_titulos_liquidados['DIF_DIAS_PAGAMENTO'] = (atraso_max_titulos_liquidados['DATA_PAGAMENTO'] - atraso_max_titulos_liquidados['DATA_VENCIMENTO']).dt.days

    atraso_max_titulos_liquidados = atraso_max_titulos_liquidados.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'DIF_DIAS_PAGAMENTO': 'max'
    }).reset_index()
    atraso_max_titulos_liquidados.columns = ['DOCUMENTO', 'FORNECEDOR', 'ATRASO_MAX_TITULOS_LIQUIDADOS']


    # %% [markdown]
    # # Atraso min liquidados;

    # %%
    atraso_min_titulos_liquidados = copy.copy(base)
    #Pegando somente casos válidos para análise
    atraso_min_titulos_liquidados = atraso_min_titulos_liquidados[pd.notna(atraso_min_titulos_liquidados['DATA_PAGAMENTO'])]
    # Criando aux para cálculo prazo médio
    atraso_min_titulos_liquidados['DATA_VENCIMENTO'] = pd.to_datetime(atraso_min_titulos_liquidados['DATA_VENCIMENTO'])
    atraso_min_titulos_liquidados['DATA_PAGAMENTO'] = pd.to_datetime(atraso_min_titulos_liquidados['DATA_PAGAMENTO'])

    # Calculo Diferenças Dias (Pagamento - Vencimento)
    atraso_min_titulos_liquidados['DIF_DIAS_PAGAMENTO'] = (atraso_min_titulos_liquidados['DATA_PAGAMENTO'] - atraso_min_titulos_liquidados['DATA_VENCIMENTO']).dt.days

    atraso_min_titulos_liquidados = atraso_min_titulos_liquidados.groupby(['DOCUMENTO', 'FORNECEDOR']).agg({
    'DIF_DIAS_PAGAMENTO': 'min'
    }).reset_index()
    atraso_min_titulos_liquidados.columns = ['DOCUMENTO', 'FORNECEDOR', 'ATRASO_MIN_TITULOS_LIQUIDADOS']

    
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
        qtde_titulos_abertos_vencidos = pd.merge(qtde_titulos_abertos_vencidos, df_inter, on=['DOCUMENTO', 'FORNECEDOR'], how='outer')
        
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

        df_final[colunas_float] = df_final[colunas_float].astype(float)


        
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