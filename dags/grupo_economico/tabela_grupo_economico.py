# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import os
from airflow.models import Variable


def grupo_economico_to_trusted(access_params=None,  **kwargs):

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
    

    # Query Trino
    query_grupo_economico = f"""
        WITH serasa AS (
            SELECT 
                cnpj_raiz,
                razao_social 
            FROM deltalakerefined.receita_federal.dados_cadastrais
        )
        SELECT DISTINCT
            s.razao_social
        --	,p_participante."name" AS nome_fantasia
            ,pi_participantes.value AS raiz_cnpj
            ,pi_participantes."type" AS tipo_identificacao_sacado
            ,pr_participantes."role" AS role_sacado
            ,prr_participantes."type" AS tipo_vinculo
            ,pr_grupo."role" AS role_grupo
            ,pi_grupo.value AS nome_grupo
            ,pi_grupo."type" AS tipo_identificacao_grupo
        FROM postgres.prts_schema_{Variable.get('STAGE')}_default.party_identification pi_sacado
        INNER JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party_role pr_sacado
            ON pr_sacado.party_id = pi_sacado.party_id
            AND pr_sacado."role" = 'EG_MEMBER'
        INNER JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party_role_relationship prr
            ON prr.to_party_role_id = pr_sacado.id
            AND prr."type" = 'ECONOMIC_GROUP_TO_EG_MEMBER'
            AND prr.date_thru IS NULL
        INNER JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party_role pr_grupo
            ON pr_grupo.id = prr.from_party_role_id
            AND pr_grupo."role" = 'ECONOMIC_GROUP'
        INNER JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party_identification pi_grupo
            ON pi_grupo.party_id = pr_grupo.party_id
            AND pi_grupo."type" = 'PGID'
        INNER JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party p_sacado
            ON p_sacado.id = pi_sacado.party_id
        LEFT JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party_role_relationship prr_participantes
            ON prr_participantes.from_party_role_id = pr_grupo.id
            AND prr_participantes."type" = 'ECONOMIC_GROUP_TO_EG_MEMBER'
            AND prr_participantes.date_thru IS NULL
        LEFT JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party_role pr_participantes
            ON pr_participantes.id = prr_participantes.to_party_role_id
            AND pr_participantes."role" = 'EG_MEMBER'
        LEFT JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party_identification pi_participantes
            ON pi_participantes.party_id = pr_participantes.party_id
            AND pi_participantes."type" = 'PGAS'
        LEFT JOIN postgres.prts_schema_{Variable.get('STAGE')}_default.party p_participante
            ON p_participante.id = pi_participantes.party_id
        LEFT JOIN serasa s
            ON s.cnpj_raiz = pi_participantes.value
        WHERE pi_sacado."type" = 'PGAS'
    """
    
    df_grupo_economico = execute_query(conn, query_grupo_economico)
    df_grupo_economico = df_grupo_economico.reset_index(drop=True)

    
    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_grupo_economico['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_grupo_economico['year'], df_grupo_economico['month'], df_grupo_economico['day'] = now.year, now.month, now.day
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
    BUCKET_SOURCE_TRUSTED = "grupo-economico"
    FOLDER_DESTINATION_TRUSTED = "tabela_grupo_economico"
    
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_grupo_economico, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )