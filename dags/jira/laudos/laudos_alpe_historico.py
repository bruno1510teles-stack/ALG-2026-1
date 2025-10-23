# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from deltalake import write_deltalake, DeltaTable
import os
import re
from io import BytesIO, StringIO
import csv
from decimal import Decimal, ROUND_DOWN

def raw_laudos(access_params=None, **kwargs):

    minio_raw = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
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

    #Configurações de leitura
    BUCKET_NAME = "laudo"
    PREFIX = "saida/esteira=alpe/"
    ALL_DATA = []  

    # 📥 Lendo arquivos CSV do MinIO
    objects = minio_raw.list_objects(BUCKET_NAME, prefix=PREFIX, recursive=True)

    for obj in objects:
        file_key = obj.object_name
        if file_key.endswith(".csv"):
            #print(f"📥 Lendo CSV: {file_key}")

            try:
                # Baixa o arquivo CSV como bytes
                response = minio_raw.get_object(BUCKET_NAME, file_key)
                
                # Lê o conteúdo como string
                csv_content = response.read().decode('utf-8')
                
                # Trata o conteúdo como arquivo em memória
                input_file = StringIO(csv_content)
                output_file = StringIO()
                
                # Cria leitor e escritor CSV
                reader = csv.reader(input_file)
                writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                
                # Processa cada linha
                for row in reader:
                    cleaned_row = [field.replace('\n', ' ').replace('\r', ' ') for field in row]
                    writer.writerow(cleaned_row)
                
                # Converte de volta para BytesIO para o pandas ler
                csv_data_buffer = BytesIO(output_file.getvalue().encode('utf-8'))
                
                # Lê o CSV como DataFrame
                df = pd.read_csv(csv_data_buffer, sep=',', dtype=str, low_memory=False, header=0)
                
                # Adiciona metadado de origem
                df["source_file"] = file_key
                
                ALL_DATA.append(df)
                
            except Exception as e:
                print(f"⚠️ Erro ao ler {file_key}: {e}")

    # Empilha todos os DataFrames
    if ALL_DATA:
        df_laudo = pd.concat(ALL_DATA, ignore_index=True)
        print(f"✅ Dados empilhados: {len(df_laudo)} linhas")
    else:
        raise ValueError("⚠️ Nenhum arquivo CSV foi encontrado.")    

    #Tratamentos
    print('Tratando base para inserção na Trusted...')   

    # Campo CNPJ
    if 'cnpj_ec' in df_laudo.columns:
        df_laudo.rename(columns={'cnpj_ec': 'cnpj'}, inplace=True)
    df_laudo['cnpj'] = df_laudo['cnpj'].astype(str).str.slice(0, 14).str.zfill(14)

    # Criação campo cnpj_raiz
    df_laudo['cnpj_raiz'] = df_laudo['cnpj'].str[:8].str.zfill(8)

    # Função para ajustar os valores ao formato decimal(8, 2)
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None
        valor_str = str(valor).strip().replace(',', '.')
        if valor_str == '':
            return None
        try:
            return Decimal(valor_str).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        except Exception:
            return None

    colunas_decimais = ['valor_aprovado', 'restritivo_pj', 'restritivo_pf', 'total_restritivo']
    for col in colunas_decimais:
        if col in df_laudo.columns:
            df_laudo[col] = df_laudo[col].apply(ajustar_decimal)
            df_laudo[col] = df_laudo[col].astype(float).round(2)


    # Tratamento colunas para inteiro com suporte a nulos
    colunas_int = ['score']

    for col in colunas_int:
        df_laudo[col] = (
            pd.to_numeric(df_laudo[col], errors='coerce')  # converte para numérico com NaNs
            .round(0)                                               # arredonda para zero casas decimais
            .astype('Int64')                                        # converte para inteiro com suporte a nulos
        )

    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_laudo['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_laudo['year'], df_laudo['month'], df_laudo['day'] = now.year, now.month, now.day

    # Configuração do Delta Lake
    storage_options = {
    "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
    "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
    "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    BUCKET_SOURCE_TRUSTED = "laudos-trusted"
    FOLDER_DESTINATION_TRUSTED = "laudos-full"

    # Escrevendo no Delta Lake com schema fixado
    write_deltalake(
    f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
    df_laudo,
    partition_by=["year", "month", "day"],
    storage_options=storage_options,
    mode="overwrite"
    )

