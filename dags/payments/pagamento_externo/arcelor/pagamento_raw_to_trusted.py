# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake


def extracao_pagamento(access_params=None, **kwargs):

    #---------------------------------------------------------------------------------------#
    # Conectando na Raw e carregando base

    # Vale lembrar que o pagamento e o faturamento externo estao no mesmo arquivo, porem em planilhas diferentes.

    minio_raw = Minio(
        "api-raw.alpe.com.br",
        access_key = 'B7q0avvSIpSdyGPXWnEC',
        secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
    )

        # Connection validation
    try:
        # Try to list the buckets
        
        buckets = minio_raw.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")


     # Gerando nome do arquivo para importacao
    BUCKET_SOURCE_RAW = "faturamento-externo"
    FOLDER_DESTINATION_RAW = 'arcelor/year=2024/month=12/day=12'
    file_name = 'Faturamento Base dez24 - Tratada.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Carregando Excel
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    df = pd.read_excel(file_data, sheet_name="Histórico Pagamento")


    print('Parte 1')

    #---------------------------------------------------------------------------------------#
    # Inicio Tratamento

    # Separando as linhas para tratamento dos nomes das colunas, dos dados principais da tabela
    df_colunas = df.iloc[:2]
    df_dados = df.iloc[2:]

    # Trabalhando em cima do df_colunas
    df_colunas_tratar = pd.DataFrame(df_colunas.astype(str).agg(' '.join).copy())

    lista_colunas = []

    def criando_lista(df):
        for x in df[0]:
            lista_colunas.append(x)

    criando_lista(df_colunas_tratar)

    lista_colunas[0] = 'pagador'
    lista_colunas[1] = 'razao_social'
    lista_colunas[2] = 'raiz_cnpj'
    lista_colunas[3] = 'unidade'


    print('Parte 2')

    def trata_colunas_numericas(lista):
        lista_verificada = []
        
        for x in lista:    
            if 'Vlr. Total Título Aber.' in x:
                lista_verificada.append('vlr_total_titulo_aberto_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])           
            elif 'Vl. Inad. Corrente' in x:
                lista_verificada.append('vlr_inad_corrente_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])        
            elif 'Vlr. Venc. até 5d' in x:
                lista_verificada.append('vlr_vencido_ate_5_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])       
            elif 'Vlr.Venc. 6-15d' in x:
                lista_verificada.append('vlr_vencido_6_a_15_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])     
            elif 'Vlr.Venc. 16-30d' in x:
                lista_verificada.append('vlr_vencido_16_a_30_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])     
            elif 'Vlr. Venc. > 30d' in x:
                lista_verificada.append('vlr_vencido_maior_30_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])        
            elif 'Valor Recebido' in x:
                lista_verificada.append('vlr_recebido_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])      
            elif 'Prz. Med. Atr (d)' in x:
                lista_verificada.append('prazo_medio_atrasado_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])          
            elif 'Vl.Rec.Atr. 1-5d' in x:
                lista_verificada.append('vlr_receb_atrasado_1_a_5_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])         
            elif 'Vl.Rec.Atr. 6-15d' in x:
                lista_verificada.append('vlr_receb_atrasado_6_a_15_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])     
            elif 'Vl.Rec.Atr.16-30d' in x:
                lista_verificada.append('vlr_receb_atrasado_16_a_30_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])          
            elif 'Vl.Rec.Atr. > 30d' in x:
                lista_verificada.append('vlr_receb_atrasado_maior_30_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])           
            elif 'Valor Receb. Atraso' in x:
                lista_verificada.append('vlr_receb_atrasado_' + (x.split(' ')[-1])[-4:] + (x.split(' ')[-1])[:2])
            else:
                lista_verificada.append(x)

        return lista_verificada

    lista_atualizada = trata_colunas_numericas(lista_colunas)

    df_colunas_final = pd.DataFrame([lista_atualizada])

    print('Parte 3')

    # Setando novos nomes das colunas na df_dados
    novos_nomes_colunas = df_colunas_final.iloc[0].tolist()  # Pega a primeira linha como novos nomes

    df_dados.columns = novos_nomes_colunas


    # Tratando valores Nan e criando uma copia do DataFrame (evitar warnings)
    colunas_para_converter = [col for col in df_dados.columns if col.startswith('vlr')]

    df_dados.loc[:, colunas_para_converter] = df_dados.loc[:, colunas_para_converter].fillna(0).infer_objects()

    df_final = df_dados.copy()

    print('Parte 4')

    # Tratamento upper() nas colunas razao_social e unidade
    df_final['razao_social'] = df_final['razao_social'].str.upper()
    df_final['unidade'] = df_final['unidade'].str.upper()

    print('Parte 5')

    # Transformando campos de valor em float
    df_final.iloc[:, 4:] = df_final.iloc[:, 4:].apply(pd.to_numeric, errors='coerce').fillna(0).astype(float)

    print('Parte 6')


    df_numeric = df_final.select_dtypes(include=['number'])  # Seleciona apenas as colunas numéricas
    df_final[df_numeric.columns] = df_numeric.abs()


    # Tratando CNPJ8
    df_final['raiz_cnpj'] = '00000000' + df_final['raiz_cnpj'].astype(str)
    df_final['raiz_cnpj'] = df_final['raiz_cnpj'].str[-8:]

    print('Parte 7')


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day


    # Agrupando para pegar Unidade e Prazo
    df_unidade = df_final.groupby('raiz_cnpj').agg({
                                'unidade': 'max'
                                }).reset_index()

    df_unidade = df_unidade.drop_duplicates()


    # Agrupando por Raiz_cnpj, year, month, day e somando valores

    df_raiz_cnpj_datas = df_final.copy()
    df_raiz_cnpj_datas = df_raiz_cnpj_datas.drop(columns=['pagador', 'unidade'])

    df_agrup_cnpj_datas = df_raiz_cnpj_datas.groupby(['raiz_cnpj', 'atualizado_em', 'year', 'month', 'day']).sum().reset_index()

    # Unindo os dois DFs
    df_dados_final = pd.merge(df_unidade, df_agrup_cnpj_datas, on='raiz_cnpj', how='left')
    df_dados_final = df_dados_final.reset_index(drop=True)


    # DEPARA UNIDADE CONSOLIDADA PARA CRUZAMENTO
    # Bucket and Folder_Destination
    BUCKET_SOURCE_RAW_2 = "arquivos-python"
    FOLDER_DESTINATION_RAW_2 = 'depara_unidade_fat_externo'
    file_name_2 = 'depara_unidade_fat_externo.xlsx'
    file_path_2 = f'{FOLDER_DESTINATION_RAW_2}/{file_name_2}'

    # Uploading Excel File
    response_2 = minio_raw.get_object(BUCKET_SOURCE_RAW_2, file_path_2)
    file_data_2 = BytesIO(response_2.read())
    df_unidade_consolidada = pd.read_excel(file_data_2, sheet_name="unidade_consolidada")

    # BASE 1
    df_dados_final = pd.merge(df_dados_final, df_unidade_consolidada, on = 'unidade', how='left')
    df_dados_final['unidade_consolidada'] = df_dados_final['unidade_consolidada'].fillna(df_dados_final['unidade'])


    # Dropar a colunas antigas
    df_dados_final = df_dados_final.drop(columns=['unidade', 'razao_social'], errors='ignore')
    df_dados_final = df_dados_final.drop_duplicates()

    df_dados_final = df_dados_final.reset_index(drop = True)


    #---------------------------------------------------------------------------------------#
    # Exportando saida para Trusted

    storage_options = {
        "AWS_ACCESS_KEY_ID": 'nr0qPLaAcdCtt7lAV4oa',
        "AWS_SECRET_ACCESS_KEY": 'GRA8FxnVMy7pGDvKP1wZK2nPOC3vP7F1AvH2u3Ch',
        "AWS_ENDPOINT_URL":"https://api-trusted.alpe.com.br",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = 'payments'
    FOLDER_DESTINATION_TRUSTED = 'pagamento_externo/arcelor'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_dados_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )