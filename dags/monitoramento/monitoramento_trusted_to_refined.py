# Carregando libs
import pandas as pd
import numpy as np
import re
from datetime import datetime, timezone, timedelta
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from unidecode import unidecode
from airflow.models import Variable
from deltalake import write_deltalake


def trusted_to_refined (access_params=None,  **kwargs):
 
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
    

    #Query do trino
    query_atualizacoes = f"""
        SELECT 
            'deltalakerefined' AS camada,
            table_schema,
            table_name,
            column_name
        FROM deltalakerefined.information_schema.columns
        WHERE column_name = 'atualizado_em'
        UNION ALL
        SELECT 
            'deltalaketrusted' AS camada,
            table_schema,
            table_name,
            column_name
        FROM deltalaketrusted.information_schema.columns
        WHERE column_name = 'atualizado_em'
        and table_name <> 'boletos'
        """
       
    df_atualizacoes = execute_query(conn, query_atualizacoes)

    union_queries = []

    for idx, row in df_atualizacoes.iterrows():
        camada = row['camada']
        schema = row['table_schema']
        tabela = row['table_name']
        #max_expr = "format_datetime(MAX(TRY_CAST(atualizado_em AS TIMESTAMP)), 'yyyy-MM-dd HH:mm:ss')"

        query = (f'''
            SELECT 
                CAST('{camada}' AS VARCHAR(99)) AS camada, 
                CAST('{schema}' AS VARCHAR(99)) AS schema, 
                CAST('{tabela}' AS VARCHAR(99)) AS tabela, 
                CAST(MAX(atualizado_em) AS VARCHAR(99)) AS ultima_atualizacao 
            FROM {camada}."{schema}"."{tabela}"
            '''
        )

        union_queries.append(query)


    # Junta e executa    
    final_query = " UNION ALL ".join(union_queries)
    df_monitoramento = execute_query(conn, final_query)
    
    df_final = df_monitoramento.reset_index(drop=True)
    df_final = df_final.astype(str)
    
    # Atribuindo data
    
    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day
    
    
    df_final.loc[
    (df_final["schema"] == "monitoramento") &
    (df_final["tabela"] == "monitoramento_tabelas"),
    "ultima_atualizacao"
    ] = df_final["atualizado_em"]
        
        
    print("Tratamento dos dados concluído")
 
 
    # Exportando dados para a camada refined
    # # Conectando na refined        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    
    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "monitoramento"
    FOLDER_DESTINATION_REFINED = "monitoramento"
 
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_final,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )