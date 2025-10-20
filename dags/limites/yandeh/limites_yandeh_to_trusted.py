# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import minio as Minio
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
import os
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from decimal import Decimal, ROUND_DOWN

def limites_yandeh(access_params=None, **kwargs):


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
    
    #Query Trino
    query_limites = f"""
        SELECT 
            s.codigo_sacado
            , s.numero_cnpj_sacado_formatado
            , vls.cpf_cnpj_sacado as numero_cnpj_sacado
            , vls.nome_sacado
            , vls.limite_atribuido as valor_limite
            , vls.limite_disponivel  
        FROM postgres.ccred_schema_{Variable.get('STAGE')}_default.vw_limite_sacado vls
        INNER JOIN postgres.ccred_schema_{Variable.get('STAGE')}_default.sacado s on s.id = vls.id_sacado
        WHERE vls.sumarizado = FALSE
        GROUP BY 
            s.codigo_sacado
            , vls.nome_sacado
            , vls.cpf_cnpj_sacado
            , s.numero_cnpj_sacado_formatado
            , vls.limite_atribuido
            , vls.limite_disponivel
    """

    df_limites = execute_query(conn, query_limites)
    df_limites = df_limites.reset_index(drop=True)

    # Função para ajustar os valores ao formato decimal
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None
        else:
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
    
    # Aplicar a função nas colunas de valor 
    df_limites['valor_limite'] = df_limites['valor_limite'].apply(ajustar_decimal)
    df_limites['limite_disponivel'] = df_limites['limite_disponivel'].apply(ajustar_decimal)
    
    
    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_limites['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_limites['year'], df_limites['month'], df_limites['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")


    # Exportando dados para a camada Trusted
    # # Conectando na Trusted        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "limites"
    FOLDER_DESTINATION_TRUSTED = "limites_yandeh"
    
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_limites, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )