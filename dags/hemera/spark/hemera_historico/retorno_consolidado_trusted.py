# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
import os
import tempfile
import msoffcrypto
import re


def retorno_consolidado_trusted (access_params=None, **kwargs):

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

    

    BUCKET_NAME = "hemera-csv"
    PREFIX = "retorno/"
    ALL_DATA = []

    # Lista todos os arquivos CSV no bucket
    objects = minio_raw.list_objects(BUCKET_NAME, prefix=PREFIX, recursive=True)

    for obj in objects:
        file_key = obj.object_name
        if file_key.endswith(".csv"):
            print(f"📥 Lendo CSV: {file_key}")

            try:
                # Baixa o arquivo CSV como bytes
                response = minio_raw.get_object(BUCKET_NAME, file_key)
                csv_data = BytesIO(response.read())

                # Lê o CSV como DataFrame, forçando tudo como string
                df = pd.read_csv(csv_data, dtype=str, low_memory=False)
                df["source_file"] = file_key
                ALL_DATA.append(df)

            except Exception as e:
                print(f"⚠️ Erro ao ler {file_key}: {e}")

    # Empilha todos os DataFrames
    if ALL_DATA:
        final_df = pd.concat(ALL_DATA, ignore_index=True)
        print("✅ Dados empilhados com sucesso!")
    else:
        print("⚠️ Nenhum dado foi carregado.")



    # Tratando base

    print('Tratando base para inserção na Trusted...')

    def converter_data_excel(data):
        # Verifica se a entrada é um número (formato Excel)
        if isinstance(data, (int, float)):
            # Converte do formato Excel para a data correta
            return pd.to_datetime(data, origin='1899-12-30', unit='D')
        else:
            # Retorna a data original, caso já esteja em formato datetime
            return pd.to_datetime(data, errors='coerce')
        

    print('Tratando datas...')

    final_df['DataVencimento'] = final_df['DataVencimento'].apply(converter_data_excel)

    print('Tratando data_arquivo...')

    # Extrai o padrão de data do nome do arquivo e cria a coluna data_arquivo
    final_df["data_arquivo"] = final_df["source_file"].apply(
        lambda x: re.search(r"\d{2}\.\d{2}\.\d{2}", x).group() if re.search(r"\d{2}\.\d{2}\.\d{2}", x) else None
    )

    # Converte para datetime
    final_df["data_arquivo"] = pd.to_datetime(final_df["data_arquivo"], format="%d.%m.%y", errors="coerce")


    final_df['ID_Registro_VX'] = final_df['ID_Registro_VX'].astype(str).str.zfill(10)

    print('Tratando campos de valor...')

    # Formatar para ter 2 casas decimais
    final_df['ValorPagamento'] = pd.to_numeric(final_df['ValorPagamento'], errors='coerce').round(2)
    final_df['ValorNominal'] = pd.to_numeric(final_df['ValorNominal'], errors='coerce').round(2)
    final_df['Abatimentos'] = pd.to_numeric(final_df['Abatimentos'], errors='coerce').round(2)
    final_df['Juros'] = pd.to_numeric(final_df['Juros'], errors='coerce').round(2)

    print('Tratando data_fechamento...')

    # Renomear a coluna 'ultima_data' para 'data_fechamento'
    data_fechamento = final_df['data_arquivo'] + pd.offsets.MonthEnd(0)
    final_df['data_fechamento'] = data_fechamento

    print('Data do arquivo...')

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    final_df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    final_df['year'] = final_df['data_fechamento'].apply(lambda x: x.year if pd.notnull(x) else None)
    final_df['month'] = final_df['data_fechamento'].apply(lambda x: x.month if pd.notnull(x) else None)

    print('Renomeando colunas...')

    ## Filtrando apenas colunas necessárias e renomando-as
    final_df.rename(columns={
        "CodigoOcorrencia":"codigo_ocorrencia",     
        "Ocorrencia":"ocorrencia",      
        "ID_Registro_VX":"id_registro",     
        "Situacao":"situacao",     
        "CedenteCnpjCpf":"cnpj_cedente",     
        "CedenteNome":"nome_cedente",     
        "SacadoCnpjCpf":"cnpj_sacado",     
        "SacadoNome":"nome_sacado",     
        "TipoAtivo":"tipo_ativo",     
        "DataVencimento":"data_vencimento",     
        "NumeroTitulo":"numero_titulo",     
        "NumeroBoletoBanco":"numero_boleto_banco",
        "CampoChave":"campo_chave",      
        "ValorPagamento":"valor_pagamento",
        "ValorNominal":"valor_nominal",
        "Abatimentos":"abatimentos",            
        "Juros":"juros",
        "Banco":"banco",
        "Agencia":"agencia",
        "Conta":"conta"
    }, inplace=True)


    print('Filtrando colunas...')

    colunas_finais = ['data_fechamento', 'data_arquivo', 'codigo_ocorrencia', 'ocorrencia', 'id_registro', 'situacao', 'cnpj_cedente', 'nome_cedente', 'cnpj_sacado',
                'nome_sacado', 'tipo_ativo', 'data_vencimento', 'numero_titulo', 'numero_boleto_banco', 'campo_chave', 'valor_nominal', 'valor_pagamento', 'abatimentos',
                'juros', 'banco', 'agencia', 'conta','atualizado_em', 'year', 'month']

    final_df = final_df[colunas_finais]

    final_df = final_df.reset_index(drop=True)


    # Garantindo que não vou ter tidos de dados invalidos para o DeltaLake
    for col in final_df.columns:
        if pd.api.types.is_numeric_dtype(final_df[col]):
            final_df[col] = final_df[col].fillna(0)
        elif pd.api.types.is_datetime64_any_dtype(final_df[col]):
            final_df[col] = final_df[col].fillna(pd.NaT)
        else:
            final_df[col] = final_df[col].fillna('')

    def converter_para_datetime(df):
        # Seleciona todas as colunas do tipo datetime (independente de timezone)
        colunas_datetime = df.select_dtypes(include=['datetime', 'datetime64']).columns
        
        for coluna in colunas_datetime:
            # Converte a coluna para datetime.date, removendo o tempo e o timezone
            df[coluna] = pd.to_datetime(df[coluna]).dt.date
        
        return df

    # Chamando a função para converter dinamicamente todas as colunas de data
    final_df = converter_para_datetime(final_df).copy()

    

    print('Salvando dados na Trusted...')


    storage_options_trusted = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "hemera-trusted"
    FOLDER_DESTINATION_TRUSTED = "retorno/delta"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        final_df,
        storage_options=storage_options_trusted,
        mode="overwrite"
    )

    print('Arquivo salvo com sucesso!')
