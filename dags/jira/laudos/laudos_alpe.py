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

    # --- INÍCIO DA LÓGICA INCREMENTAL (Carga Full ou Incremental) ---

    print('Iniciando merge incremental...')

    # 1️ Definir o caminho e as opções de storage
    storage_options_trusted = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    BUCKET_SOURCE_TRUSTED = "laudo-trusted"
    FOLDER_DESTINATION_TRUSTED = "laudo"
    delta_path = f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}"

    # 2 Ler dados existentes (Trusted)
    try:
        dt = DeltaTable(delta_path, storage_options=storage_options_trusted)
        df_trusted = dt.to_pandas()
        print(f"Tabela Delta existente carregada: {len(df_trusted)} linhas.")
    except Exception as e:
        print(f"Tabela Delta não encontrada (ou erro ao ler: {e}). Tratando como carga inicial.")
        df_trusted = pd.DataFrame() 

    PLACEHOLDER_NULO = 'NA_PLACEHOLDER'

    # 3 Garantir datetime UTC-3 no df_laudo
    df_laudo['data_hora'] = pd.to_datetime(df_laudo['data_hora'], errors='coerce')
    if df_laudo['data_hora'].dt.tz is None:
        df_laudo['data_hora'] = df_laudo['data_hora'].dt.tz_localize('America/Sao_Paulo')


    # 4 Criar chave_merge consistente (padronizada)
    df_laudo['chave_merge'] = (
        df_laudo['data_hora'].dt.strftime('%Y-%m-%d %H:%M:%S%z').fillna(PLACEHOLDER_NULO) + '|' +
        df_laudo['chave_unica'].fillna(PLACEHOLDER_NULO).astype(str)
    )

    # Garantir datetime UTC-3 no df_trusted, se existir
    if not df_trusted.empty:
        df_trusted['data_hora'] = pd.to_datetime(df_trusted['data_hora'], errors='coerce')
        if df_trusted['data_hora'].dt.tz is None:
            df_trusted['data_hora'] = df_trusted['data_hora'].dt.tz_localize('America/Sao_Paulo')

        # Criar chave_merge consistente no df_trusted
        df_trusted['chave_merge'] = (
            df_trusted['data_hora'].dt.strftime('%Y-%m-%d %H:%M:%S%z').fillna(PLACEHOLDER_NULO) + '|' +
            df_trusted['chave_unica'].fillna(PLACEHOLDER_NULO).astype(str)
        )
        
        print("Filtrando registros novos...")
        chaves_trusted = set(df_trusted['chave_merge'].unique())
        df_incremental = df_laudo[~df_laudo['chave_merge'].isin(chaves_trusted)].copy()
    else:
        print("Tabela de destino vazia. Inserindo todos os registros lidos.")
        df_incremental = df_laudo.copy()

    print(f"Registros novos para inserir: {df_incremental.shape[0]}")

    # 5 Checar se há dados novos
    if df_incremental.empty:
        print("Nenhum registro novo encontrado. Encerrando execução.")
        return

    # 6 Remover coluna auxiliar antes de salvar
    df_incremental.drop(columns=['chave_merge'], inplace=True)
    print("Coluna 'chave_merge' removida antes da escrita.")

    # 7 Garantir datetime UTC-3 antes de salvar no Delta ---
    # Converte a coluna 'data_hora' de string para datetime
    # - errors='coerce' faz com que valores inválidos se tornem NaT (missing)
    df_incremental['data_hora'] = pd.to_datetime(df_incremental['data_hora'], errors='coerce')

    # Verifica se a coluna 'data_hora' não possui timezone
    if df_incremental['data_hora'].dt.tz is None:
        # Se não houver timezone, localiza a hora no fuso de São Paulo (UTC-3)
        # Isso marca os datetime como timezone-aware, sem alterar a hora
        df_incremental['data_hora'] = df_incremental['data_hora'].dt.tz_localize('America/Sao_Paulo')


    # 8 Reordenar colunas
    ordem_colunas = [
        'data_hora', 'chave_unica', 'nome_filtro', 'ticket_jira', 'cnpj', 'cnpj_raiz', 'resolucao', 
        'valor_aprovado', 'politica', 'parecer', 'ramificacao', 'score', 'restritivo_pj', 
        'restritivo_pf', 'total_restritivo', 'year', 'month', 'day'
    ]
    df_incremental = df_incremental[ordem_colunas].reset_index(drop=True)

    # 9 Salvar no Delta
    print('Salvando dados incrementais na Trusted...')
    write_deltalake(
        delta_path,
        df_incremental,
        storage_options=storage_options_trusted,
        mode="append",
        overwrite_schema=False
    )

    print(f"Dados incrementais salvos com sucesso: {len(df_incremental)} linhas.")
    # --- FIM DA LÓGICA INCREMENTAL ---

