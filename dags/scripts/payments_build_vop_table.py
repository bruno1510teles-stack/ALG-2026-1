import pandas as pd
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable

from scripts.payments_refined_engine_v2 import transform_payments_engine_v2

now = datetime.now(tz=timezone(timedelta(hours=-3)))
yesterday = now - timedelta(days=1)

def transform_data_to_refined(files_list, access_params):

    # VARIAVEIS
    BUCKET_SOURCE_TRUSTED = "payments"
    TRUSTED_FOLDER =  "boletos/"
    BUCKET_SOURCE_REFINED = "payments"
    REFINED_FOLDER = "engine/v2/"

    df_payments = pd.DataFrame()

    # CONECTAR NO MINIO RAW
    client = Minio(
        access_params['endpoint_url_trusted'],
        access_key = access_params['aws_access_key_id_trusted'],
        secret_key = access_params['aws_secret_access_key_trusted'],
    )

    dfs = []

    for file_name in files_list:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_TRUSTED, object_name=file_name)
        payments_temp = pd.read_parquet(BytesIO(file.data))
        dfs.append(payments_temp)

    df_payments = pd.concat(dfs, ignore_index=True)

    convert_dict = {'documento': str,
            'razao_social': str,
            'numero_titulo': str,
            'data_emissao': str,
            'data_vencimento': str,
            'data_pagamento': str,
            'valor_titulo': float,
            'data_hp': str,
            'numero_parcela': int,
            'valor_pago': str,
            'fornecedor': str,
            'fonte': str,
            'atualizado_em': str,
            'tipo_documento': str,}
    
    for k, v in convert_dict.items():
        if k not in df_payments.columns:
            df_payments[k] = None
        df_payments[k] = df_payments[k].astype(v)

    date_cols = ['data_emissao', 'data_vencimento', 'data_pagamento', 'data_hp']
    for col in date_cols:
        # change columns data type to datetime with format YYYY-MM-DD
        df_payments[col] = pd.to_datetime(df_payments[col], format='%Y-%m-%d', errors='coerce')

    print(df_payments.info())

    # TRATAMENTO DOS DADOS PARA MOTOR V2
    df_refined_motor = transform_payments_engine_v2(df_payments)

    print(df_refined_motor)

    df_refined_motor['year'] = now.year
    df_refined_motor['month'] = now.month
    df_refined_motor['day'] = now.day

    df_refined_motor.to_csv('include/df_refined_motor.csv')

    storage_options_refined = {
        "AWS_ACCESS_KEY_ID":access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    write_deltalake(f"s3a://{BUCKET_SOURCE_REFINED }/{REFINED_FOLDER}", 
            df_refined_motor, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options_refined, 
            mode="append",
                )