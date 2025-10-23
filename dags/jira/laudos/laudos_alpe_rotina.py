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

def trusted_incremental(access_params=None, **kwargs):
    """
    Script responsável apenas pela carga incremental:
    lê a base staging (df_laudo) e insere apenas novos registros na trusted.
    """

    minio_raw = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
    )

    print('Iniciando processo  incremental da base Trusted...')

    # 1️ Definir o caminho e as opções de storage
    storage_options_trusted = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Caminhos  
    staging_path = "s3a://laudos-trusted/laudos-full"       # origem: full
    BUCKET_SOURCE_TRUSTED = "laudo-trusted"
    FOLDER_DESTINATION_TRUSTED = "laudos-incremental" # destino: incremental
    delta_path = f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}"

    # 1 Lê df_laudo (base full)
    try:
        dt_staging = DeltaTable(staging_path, storage_options=storage_options_trusted)
        df_laudo = dt_staging.to_pandas()
        print(f"📥 df_laudo carregado da full: {len(df_laudo)} linhas")
    except Exception as e:
        raise ValueError(f"❌ Erro ao ler df_laudo (full): {e}")
    
    # 2 Lê df_trusted existente (incremental)
    try:
            dt_trusted = DeltaTable(delta_path, storage_options=storage_options_trusted)
            df_trusted = dt_trusted.to_pandas()
            print(f"📂 df_trusted incremental existente carregado: {len(df_trusted)} linhas")
    except Exception as e:
        raise ValueError(f"❌ Base incremental não encontrada. O incremental requer carga full existente. Erro: {e}")


    PLACEHOLDER_NULO = 'NA_PLACEHOLDER'

    # 3 Cria chave_merge em df_laudo e garante datetime UTC-3 no df_laudo
    df_laudo['data_hora'] = pd.to_datetime(df_laudo['data_hora'], errors='coerce')
    if df_laudo['data_hora'].dt.tz is None:
        df_laudo['data_hora'] = df_laudo['data_hora'].dt.tz_localize('America/Sao_Paulo')


    df_laudo['chave_merge'] = (
        df_laudo['data_hora'].dt.strftime('%Y-%m-%d %H:%M:%S%z').fillna(PLACEHOLDER_NULO) + '|' +
        df_laudo['chave_unica'].fillna(PLACEHOLDER_NULO).astype(str)
    )

    # 4 Cria chave_merge em df_trusted (se não vazio) e garante datetime UTC-3 no df_trusted (se não vazio)
    if not df_trusted.empty:
        df_trusted['data_hora'] = pd.to_datetime(df_trusted['data_hora'], errors='coerce')
        if df_trusted['data_hora'].dt.tz is None:
            df_trusted['data_hora'] = df_trusted['data_hora'].dt.tz_localize('America/Sao_Paulo')

        df_trusted['chave_merge'] = (
            df_trusted['data_hora'].dt.strftime('%Y-%m-%d %H:%M:%S%z').fillna(PLACEHOLDER_NULO) + '|' +
            df_trusted['chave_unica'].fillna(PLACEHOLDER_NULO).astype(str)
        )
        
        print("Filtrando registros novos...")
        chaves_existentes = set(df_trusted["chave_merge"].unique())
        df_incremental = df_laudo[~df_laudo["chave_merge"].isin(chaves_existentes)].copy()
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
        'restritivo_pf', 'total_restritivo'
    ]
    df_incremental = df_incremental[ordem_colunas].reset_index(drop=True)

    # 9 Salvar no Delta
    print(f"Gravando df_incremental em {delta_path}...")
    write_deltalake(
        delta_path,
        df_incremental,
        storage_options=storage_options_trusted,
        mode="append",
        overwrite_schema=False
    )

    print(f"✅ Incremento concluído com sucesso ({len(df_incremental)} novas linhas).")

