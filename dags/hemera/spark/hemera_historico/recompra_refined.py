# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np


def recompra_trusted_to_refined (access_params=None, **kwargs):


    # Coletando dados da camada Trusted
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

    query_recompra = f"""
                        select *
                        from deltalaketrusted.hemera_trusted.recompra_consolidado
                    """

    df = execute_query(conn, query_recompra)
 

    df_agrupado = df.groupby(['data_arquivo', 'id_titulo', 'data_fechamento']).agg(
                            valor_pagamento=('valor_pagamento', 'sum'),  
                            data_lancamento=('data_lancamento', 'max')
                            ).reset_index()

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_agrupado['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_agrupado['year'] = df_agrupado['data_fechamento'].apply(lambda x: x.year if pd.notnull(x) else None)
    df_agrupado['month'] = df_agrupado['data_fechamento'].apply(lambda x: x.month if pd.notnull(x) else None)

    df_agrupado.reset_index(drop=True, inplace=True)

    total_colunas = df_agrupado[['valor_pagamento']].sum()
    total_colunas = total_colunas.apply(lambda x: f"{x:,.2f}")
    print(total_colunas)


    df_agrupado.rename(columns={
        'valor_pagamento': 'valor_recompra'
    }, inplace=True)


    colunas_finais = ['data_fechamento', 'id_titulo', 'valor_recompra', 'data_lancamento','year', 'month', 'atualizado_em']
    # Aplicando a função para renomear as colunas no DataFrame
    df_agrupado = df_agrupado[colunas_finais]


    # Padronizando Outputs
    df_agrupado['data_fechamento'] = pd.to_datetime(df_agrupado['data_fechamento']).dt.strftime('%Y-%m-%d')
    df_agrupado['data_lancamento'] = pd.to_datetime(df_agrupado['data_lancamento']).dt.strftime('%Y-%m-%d')
    df_agrupado['id_titulo'] = df_agrupado['id_titulo'].astype(str).str.zfill(10)
    df_agrupado['valor_recompra'] = pd.to_numeric(df_agrupado['valor_recompra'], errors='coerce').round(2)
    df_agrupado['year'] = df_agrupado['year'].astype(str)
    df_agrupado['month'] = df_agrupado['month'].astype(str)
    df_agrupado['atualizado_em'] = pd.to_datetime(df_agrupado['atualizado_em']).dt.strftime('%Y-%m-%d %H:%M:%S')

    print(df_agrupado)


    print('Salvando dados na Refined...')

    storage_options_refined = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "hemera-refined"
    FOLDER_DESTINATION_REFINED = "recompra/delta"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_agrupado,
        storage_options=storage_options_refined,
        mode="overwrite"
    )
 
 
    print('Arquivo salvo com sucesso!')