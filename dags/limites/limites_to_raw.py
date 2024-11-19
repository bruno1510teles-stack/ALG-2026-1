# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import numpy as np
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable

def limites_to_raw(access_params=None, **kwargs):

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


    # Lista de cedentes
    fornecedores = ['agrichem', 'arcelor', 'asusimplementos', 'cadubo', 'ceufer', 
                    'ciacor', 'ciatintas', 'discor', 'megaleste']

    # Lista para armazenar os DataFrames
    bases_fornecedores = []

    # Iterando sobre cada fornecedor
    for fornecedor in fornecedores:
        limite_query = f"""
        select * from postgres.ccred_schema_prd_default.vw_limite_sacado_v3 
        where pgid = '{fornecedor}' and cedente_principal = true
        """
        base = execute_query(conn, limite_query)  # Executa a query
        bases_fornecedores.append(base)  # Adiciona o DataFrame à lista

    # Unificando todos os DataFrames
    df_unificado = pd.concat(bases_fornecedores, ignore_index=True)

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_unificado['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_unificado['year'], df_unificado['month'], df_unificado['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    # Verificando quantidade de CNPJ por cedente
    contagem_pgid = df_unificado.groupby('pgid').size().reset_index(name='quantidade')
    print(contagem_pgid)

    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_raw'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_raw'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_raw']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_TRUSTED = "limites"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_TRUSTED}", 
            df_unificado, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
        )
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")