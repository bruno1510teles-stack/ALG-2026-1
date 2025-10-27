# Importing Libs
from minio import Minio
from io import BytesIO, StringIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake, DeltaTable
import os
import re
import csv
from decimal import Decimal, ROUND_DOWN

def raw_laudos(access_params=None, **kwargs):
    # Conexão com MinIO
    minio_raw = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
    )

    # Validação de conexão
    try:
        buckets = minio_raw.list_buckets()
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
    except Exception as e:
        print(f"Erro ao conectar ao MinIO: {e}")
        return

    # Configurações de leitura
    BUCKET_NAME = "laudo"
    PREFIX = "saida/esteira=alpe/"
    ALL_DATA = []

    # 📥 Lendo arquivos CSV do MinIO
    objects = minio_raw.list_objects(BUCKET_NAME, prefix=PREFIX, recursive=True)
    for obj in objects:
        file_key = obj.object_name
        if file_key.endswith(".csv"):
            try:
                response = minio_raw.get_object(BUCKET_NAME, file_key)
                csv_content = response.read().decode('utf-8')
                input_file = StringIO(csv_content)
                output_file = StringIO()

                reader = csv.reader(input_file)
                writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)

                for row in reader:
                    cleaned_row = [field.replace('\n', ' ').replace('\r', ' ') for field in row]
                    writer.writerow(cleaned_row)

                csv_data_buffer = BytesIO(output_file.getvalue().encode('utf-8'))
                df = pd.read_csv(csv_data_buffer, sep=',', dtype=str, low_memory=False, header=0)

                df["source_file"] = file_key
                ALL_DATA.append(df)
            except Exception as e:
                print(f"⚠️ Erro ao ler {file_key}: {e}")

    # Empilha todos os DataFrames
    if ALL_DATA:
        df_laudo_historico = pd.concat(ALL_DATA, ignore_index=True)
        print(f"✅ Dados empilhados: {len(df_laudo_historico)} linhas")
    else:
        raise ValueError("⚠️ Nenhum arquivo CSV foi encontrado.")

    # Tratamentos iniciais
    print('Tratando base para inserção na Trusted...')
    linhas_iniciais = len(df_laudo_historico)
    print(f"Linhas iniciais: {linhas_iniciais}")

    # Renomeia cnpj_ec para cnpj e garante 14 dígitos
    if 'cnpj_ec' in df_laudo_historico.columns:
        df_laudo_historico.rename(columns={'cnpj_ec': 'cnpj'}, inplace=True)
    df_laudo_historico['cnpj'] = df_laudo_historico['cnpj'].astype(str).str.slice(0, 14).str.zfill(14)

    # Criação do campo cnpj_raiz (8 primeiros dígitos)
    df_laudo_historico['cnpj_raiz'] = df_laudo_historico['cnpj'].str[:8].str.zfill(8)

    # Função para ajustar valores decimais
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
        if col in df_laudo_historico.columns:
            df_laudo_historico[col] = df_laudo_historico[col].apply(ajustar_decimal)
            df_laudo_historico[col] = df_laudo_historico[col].astype(float).round(2)

    # Colunas inteiras
    colunas_int = ['score']
    for col in colunas_int:
        df_laudo_historico[col] = (
            pd.to_numeric(df_laudo_historico[col], errors='coerce')
            .round(0)
            .astype('Int64')
        )

    # Garantir tipo string para algumas colunas
    colunas_string = [
        "data_hora", "chave_unica", "nome_filtro", "ticket_jira",
        "cnpj", "cnpj_raiz", "resolucao", "politica", "parecer", "ramificacao"
    ]
    df_laudo_historico[colunas_string] = (
        df_laudo_historico[colunas_string].astype(str)
        .replace(["nan", "NaT", "None"], "")
        .fillna("")
    )

    # 1 Remove linhas sem data_hora ou sem chave_unica
    linhas_antes = len(df_laudo_historico)
    df_laudo_historico = df_laudo_historico[df_laudo_historico['data_hora'].notna() & df_laudo_historico['chave_unica'].notna()]
    print(f"Removidas {linhas_antes - len(df_laudo_historico)} linhas sem data_hora ou chave_unica. Total atual: {len(df_laudo_historico)}")

    # 2 Mantém apenas CNPJs válidos (14 dígitos)
    linhas_antes = len(df_laudo_historico)
    df_laudo_historico = df_laudo_historico[df_laudo_historico['cnpj'].str.match(r'^\d{14}$', na=False)]
    print(f"Removidas {linhas_antes - len(df_laudo_historico)} linhas com CNPJ inválido. Total atual: {len(df_laudo_historico)}")

    # 3 Remove linhas totalmente nulas
    linhas_antes = len(df_laudo_historico)
    df_laudo_historico.dropna(how='all', inplace=True)
    print(f"Removidas {linhas_antes - len(df_laudo_historico)} linhas totalmente nulas. Total atual: {len(df_laudo_historico)}")

    # Reordenar colunas
    ordem_colunas = [
        'data_hora', 'chave_unica', 'nome_filtro', 'ticket_jira',
        'cnpj', 'cnpj_raiz', 'resolucao', 'valor_aprovado',
        'politica', 'parecer', 'ramificacao', 'score',
        'restritivo_pj', 'restritivo_pf', 'total_restritivo'
    ]
    df_laudo_historico = df_laudo_historico[ordem_colunas].reset_index(drop=True)

    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_laudo_historico['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_laudo_historico['year'], df_laudo_historico['month'], df_laudo_historico['day'] = now.year, now.month, now.day

    # Resumo final
    linhas_finais = len(df_laudo_historico)
    print(f"Linhas removidas no total: {linhas_iniciais - linhas_finais}")
    print(f"✅ Total de linhas após todos os tratamentos: {linhas_finais}")

    # Configuração do Delta Lake
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    BUCKET_SOURCE_TRUSTED = "laudo-trusted"
    FOLDER_DESTINATION_TRUSTED = "laudo"

    # Escrevendo no Delta Lake
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        df_laudo_historico,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )
