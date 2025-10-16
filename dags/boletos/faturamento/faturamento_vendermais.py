# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import os
from airflow.models import Variable
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from decimal import Decimal, ROUND_DOWN
import numpy as np


def faturamento_to_trusted(access_params=None,  **kwargs):

    colunas_schema = {
        "id": float,
        "numero_nfe": str,
        "numero_pedido": pd.Int64Dtype(),
        "cnpj_sacado": str,
        "nome_sacado": str,
        "cnpj_cedente": str,
        "nome_cedente": str,
        "data_fatura": "date32[day]",
        "valor_fatura": float,
        "status_fatura": str,
        "status_fatura_sefaz": str,
        "status_pago": str,
        "atualizado_em": str,
        "year": pd.Int64Dtype(),
        "month": pd.Int64Dtype(),
        "day": pd.Int64Dtype(),
    }

    def normalize_schema(df: pd.DataFrame, colunas_schema: dict) -> pd.DataFrame:
        """
        Garante que o DataFrame tenha todas as colunas e tipos compatíveis
        com o schema esperado do Delta Lake.
        """
        for col, dtype in colunas_schema.items():
            if col not in df.columns:
                # Cria coluna default coerente com o tipo
                if dtype in [str, "string"]:
                    df[col] = ""
                elif dtype in [float, np.float64]:
                    df[col] = np.nan
                else:
                    df[col] = pd.NA

            try:
                # Tratamento específico para data
                if dtype == "date32[day]":
                    df[col] = pd.to_datetime(df[col]).dt.date
                else:
                    df[col] = df[col].astype(dtype)

            except Exception:
                df[col] = df[col].astype(str)

        return df

    ### Coletando dados da camada Raw
    # Conectando com o banco
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    
    # Base Boletos CCRED
    query_fatura = f"""
        select
            faturamento_id AS id,
            chave_nfe AS numero_nfe,
            pedido_id AS numero_pedido,
            cnpj_sacado AS cnpj_sacado,
            razao_social_sacado AS nome_sacado,
            cnpj_cedente AS cnpj_cedente,
            razao_social_cedente AS nome_cedente,
            date(data_faturamento) AS data_fatura,
            valor_face AS valor_fatura,
            upper(status_pedido) AS status_fatura,
            upper(status_nfe) AS status_fatura_sefaz,
            upper(pago) as status_pago
        from postgres.ccred_schema_{Variable.get('STAGE')}_default.vw_pedido_faturamento
        where (
                pgid not in ('bariloche', 'ltcarol', 'hortmix', 'blow', 'ocean', 'philipmorris', 'caboclo',
                            'benassi', 'seugil', 'roge', 'ltxando', 'comprefacil', 'adoro', 'girotrade', 'embala', 'ultracheese', 'ltdeale', 'canelas')
                or pgid is null
        )
        and (excluido = false or excluido is null)
        """
    
    fatura = execute_query(conn, query_fatura)

    query_trusted = f"""
    SELECT *
    FROM deltalaketrusted.payments.faturamento ft
    """
    
    trusted = execute_query(conn, query_trusted)         
            
    print(f"Quantidade de linhas no DataFrame 'fatura': {fatura.shape[0]}")
    print(f"Quantidade de linhas no DataFrame 'trusted': {trusted.shape[0]}")

    # Ajuste do decimal
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None
        else:
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

    # Aplicar a função na coluna 'valor_titulo'
    fatura['valor_fatura'] = fatura['valor_fatura'].apply(ajustar_decimal).astype(float).round(2)

    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    fatura['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    fatura['year'], fatura['month'], fatura['day'] = now.year, now.month, now.day

    print("Tratamento dos dados concluído")

    ### Merge incremental

    # Cria chave temporária para comparação
    fatura['chave'] = (
        fatura['cnpj_sacado'].astype(str) + '|' +
        fatura['cnpj_cedente'].astype(str) + '|' +
        fatura['numero_nfe'].astype(str)
    )

    if not trusted.empty:
        trusted['chave'] = (
            trusted['cnpj_sacado'].astype(str) + '|' +
            trusted['cnpj_cedente'].astype(str) + '|' +
            trusted['numero_nfe'].astype(str)
        )
        chaves_trusted = set(trusted['chave'].unique())
        fatura_incremental = fatura[~fatura['chave'].isin(chaves_trusted)].copy() #~inverte a logica, e traz a chave que nao está na trusted
    else:
        fatura_incremental = fatura.copy()

    print(f"Registros novos para inserir: {fatura_incremental.shape[0]}")

    if fatura_incremental.empty:
        print("Nenhum registro novo encontrado. Encerrando execução.")
        return

    # Remove coluna 'chave' de todos os DataFrames
    for df in [fatura, trusted, fatura_incremental]:
        if 'chave' in df.columns:
            df.drop(columns=['chave'], inplace=True)

    print("Coluna 'chave' removida com sucesso antes da escrita no Delta Lake.")
    
    fatura_incremental.reset_index(drop=True, inplace=True)
    fatura_incremental = normalize_schema(fatura_incremental, colunas_schema)
        
    # Configuração do Delta Lake
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    BUCKET_SOURCE_TRUSTED = "payments"
    FOLDER_DESTINATION_TRUSTED = "faturamento"

    # Escrevendo no Delta Lake com schema fixado
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        fatura_incremental,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="append"
    )