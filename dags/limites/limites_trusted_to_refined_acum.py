import deltalake
import pandas as pd
import minio as Minio
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from airflow.utils.log.logging_mixin import LoggingMixin
import numpy as np
from io import BytesIO
from dateutil.relativedelta import relativedelta


def limites_acum(access_params=None, **kwargs):

    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    # valida se é dia 28
    if now.day != 1:
        print('Dia considerado:')
        print(now)

        print("Task ignorada: só roda no dia 01.")
        exit()

    print("Executando task...")


   ### CONECTANDO COM O TRINO

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
    

    ### BASE DE LIMITES

    query_limites = f"""
                    select *
                    from deltalaketrusted.limites.limite
                    """

    df_limites = execute_query (conn, query_limites)


    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    # Data de referência = um mês antes
    ref = now - relativedelta(months=1)

    df_limites['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_limites['year'], df_limites['month'], df_limites['day'] = now.year, now.month, now.day

    # ano_mes de referência (mês anterior)
    df_limites['anomes_ref'] = ref.strftime('%Y-%m')

    print("Tratamento dos dados concluído")

    # Exiba o DataFrame
    print(df_limites)

    # Exportando dados para a camada Trusted
        
    storage_options_refined = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "limites"
    FOLDER_DESTINATION_REFINED = "limite_acum"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_limites, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options_refined,
        mode="append"
    )
