import requests
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd
import numpy as np
import base64
from minio import Minio
from io import BytesIO
from requests.auth import HTTPBasicAuth
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
from tabulate import tabulate
from decimal import Decimal, ROUND_DOWN
import csv
import os


def base_prop_decididas(access_params=None):


    data_execucao = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(rows, columns=columns)

    query_bpdm = f"""
        SELECT 
            p.cnpj AS CNPJ,
            dc.razao_social AS "NOME CLIENTE", 
            p.pgid AS "CONVÊNIO", 
            DATE(p.data_resolvido) AS "DATA ANÁLISE", 
            CASE 
                WHEN p.decisao = 'REPROVADO' THEN 'Recusado' 
                WHEN p.decisao = 'CANCELADO' THEN 'Limite Cancelado'
                WHEN p.decisao = 'APROVADO' THEN 'Aprovado'
                WHEN p.decisao = 'MANTIDO' THEN 'Mantido'
                ELSE p.decisao 
            END AS STATUS, 
            p.limite_pedido AS "LIMITE SUGERIDO", 
            p.limite_aprovado AS LIMITE, 
            p.decisor AS "ANALISTA RESPONSÁVEL", 
            p.parecer AS PARECER
        FROM deltalaketrusted.jira.propostas p 
        LEFT JOIN deltalakerefined.receita_federal.dados_cadastrais dc 
            ON p.cnpj = dc.cnpj_sem_formatacao 
        WHERE p.decisor = 'MOTOR'
            AND try_cast(p.data_resolvido AS date) = current_date - interval '1' day
            AND p.pgid NOT IN (
                'adoro','aniolli','bariloche','bassar','benassi','caboclo','comprefacil','embala',
                'girotrade','ltcarol','ltdeale','ocean','philipmorris','roge','seugil',
                'ultracheese','yandeh','ADORO','Adoro'
            )
    """

    df_bpdm = execute_query(conn, query_bpdm)

    # Limpeza de texto
    if "PARECER" in df_bpdm.columns:
        df_bpdm["PARECER"] = df_bpdm["PARECER"].astype(str).str.replace(r'[\r\n]+', ' ', regex=True)

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

    colunas_decimais = ['LIMITE SUGERIDO', 'LIMITE']
    for col in colunas_decimais:
        if col in df_bpdm.columns:
            df_bpdm[col] = df_bpdm[col].apply(ajustar_decimal)
            df_bpdm[col] = df_bpdm[col].astype(float).round(2)

    colunas_monetarias = ["LIMITE SUGERIDO", "LIMITE"]
    for col in colunas_monetarias:
        if col in df_bpdm.columns:
            df_bpdm[col] = df_bpdm[col].astype(str)

            def limpa_e_converte(valor):
                valor = str(valor).upper().strip()
                if 'MOTOR' in valor or valor in ['', 'NAN']:
                    return 0
                valor_limpo = ''.join(filter(str.isdigit, valor))
                try:
                    return int(valor_limpo)
                except ValueError:
                    return 0

            df_bpdm[col] = df_bpdm[col].apply(limpa_e_converte)

    if not df_bpdm.empty:
        df_bpdm[colunas_monetarias] = df_bpdm[colunas_monetarias].astype(np.int64)

    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    df_bpdm['year'], df_bpdm['month'], df_bpdm['day'] = now.year, now.month, now.day

    print("Tratamento dos dados concluído")
    print(f"Quantidade de linhas no DataFrame final: {df_bpdm.shape[0]}")

    arquivo_excel = "/tmp/base_propostas_decididas_motor.xlsx"

    df_bpdm.to_excel(arquivo_excel, index=False, engine="openpyxl")

    from io import BytesIO

    minio_client = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
        secure=True
    )

    BUCKET_SOURCE_RAW = "propostas-motor"
    FOLDER_DESTINATION_RAW = "base_propostas_decididas_motor"

    excel_buffer = BytesIO()
    df_bpdm.to_excel(excel_buffer, index=False, engine="openpyxl")
    excel_buffer.seek(0)

    data_execucao_dt = datetime.strptime(data_execucao, "%d/%m/%Y")
    year = data_execucao_dt.year
    month = f"{data_execucao_dt.month:02d}"
    day = f"{data_execucao_dt.day:02d}"

    file_name = f"{FOLDER_DESTINATION_RAW}/year={year}/month={month}/day={day}/base_propostas_decididas_motor.xlsx"


    minio_client.put_object(
        bucket_name=BUCKET_SOURCE_RAW,
        object_name=file_name,
        data=excel_buffer,
        length=len(excel_buffer.getvalue()),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    print(f"✅ Arquivo Excel salvo com sucesso no MinIO: s3://{BUCKET_SOURCE_RAW}/{file_name}")
