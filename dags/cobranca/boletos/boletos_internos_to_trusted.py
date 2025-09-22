# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import os
from airflow.models import Variable


def boletos_internos_to_trusted(access_params=None,  **kwargs):

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
    query_boletos = f"""
        SELECT
        b.numero_sequencial_titulo		
        ,b.codigo_filial	
        ,b.codigo_empresa		
        ,b.numero_titulo		
        ,b.codigo_cedente		
        ,b.cedente_id			
        ,b.codigo_sacado	
        ,b.sacado_id			
        ,b.status_titulo			
        ,b.status_liquidez		
        ,b.codigo_situacao_titulo			
        ,b.data_emissao			
        ,b.data_efetivacao			
        ,b.data_vencimento
        ,b.data_original_vencimento			
        ,b.data_baixa			
        ,CAST(b.valor_face AS double) AS valor_face		
        ,CAST(b.valor_titulo AS double) AS valor_titulo		
        ,CAST(b.valor_baixado AS double) AS valor_baixado	
        ,CAST(b.valor_desagio AS double) AS valor_desagio	
        ,b.rotulo	
        ,b.codigo_estagio_titulo		
        ,b.numero_nota_fiscal	
        ,b.numero_nfe		
        ,b.nome_fantasia_sacado		
        ,b.nome_sacado		
        ,b.cnpj_sacado		
        ,b.uf_sacado		
        ,b.cidade_sacado		
        ,b.cnpj_cedente			
        ,b.nome_cedente			
        ,b.nome_fantasia_cedente	
        ,b.safra_concessao			
        ,b.safra_vencimento		
        ,b.safra_baixa			
        ,b.atualizado_em	
        ,b.year		
        ,b.month	
        ,b.day
            ,nfv.vendedor_fornecedor
            ,nfv.escritorio_vendas
            ,nfv.vendedor_alpe
            ,cnae.segmento as segmento_banco
            ,cnae.sub_segmento
            ,cnae.secao_final
            ,cnae.divisao_final
            ,date_diff('day', b.data_baixa, b.data_vencimento) AS dif_dias
            ,CASE WHEN b.data_baixa IS NOT NULL THEN 1 ELSE 0 END AS flag_boleto
        FROM deltalaketrusted.payments.boletos_internos b
        LEFT JOIN deltalaketrusted.payments.nota_fiscal_vendedor nfv
            ON b.numero_nfe = nfv.numero_nfe
        LEFT JOIN deltalakerefined.cnae.depara_cnae cnae
            ON cnae.cnpj_completo = LPAD(REGEXP_REPLACE(b.cnpj_sacado, '[./-]', ''), 14, '0')
    """
    
    df_boletos = execute_query(conn, query_boletos)
    df_boletos = df_boletos.reset_index(drop=True)

    
    # Atribuindo data
    #now = datetime.now(tz=timezone(timedelta(hours=-3)))
    #df_boletos['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    #df_boletos['year'], df_boletos['month'], df_boletos['day'] = now.year, now.month, now.day
    #print("Tratamento dos dados concluído")


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
    BUCKET_SOURCE_TRUSTED = "cobranca"
    FOLDER_DESTINATION_TRUSTED = "boletos_internos"
    
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_boletos, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )