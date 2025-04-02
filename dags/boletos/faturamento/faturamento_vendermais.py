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

def faturamento_to_trusted(access_params=None,  **kwargs):

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
        fp.id, fp.numero_nfe, fp.numero_pedido, fp.numero_cnpj_sacado as cnpj_sacado, s.nome_sacado, pc.cpf_cnpj_sem_formato as cnpj_cedente, pc.nome as nome_cedente ,date(fp.data_fatura) as data_fatura, 
        fp.valor_faturado as valor_fatura, fp.status_fatura, sefaz.status as status_fatura_sefaz
    from 
        postgres.ccred_schema_{Variable.get('STAGE')}_default.fatura_pedido fp
        inner join postgres.ccred_schema_{Variable.get('STAGE')}_default.pedido p on fp.numero_pedido = p.id
        inner join postgres.ccred_schema_{Variable.get('STAGE')}_default.pessoa_cedente pc on p.cedente_id  = pc.id 
        inner join postgres.ccred_schema_{Variable.get('STAGE')}_default.sacado s on p.sacado_id = s.id
        left join postgres.ccred_schema_{Variable.get('STAGE')}_default.status_nfe sefaz on sefaz.chave_nfe = fp.numero_nfe 
    where 
        fp.status_fatura <> 'CANCELADO'
        and pc.codigo_cedente not in (12, 188, 6910, 14099, 40585, 99241, 101880, 13974, 14688, 105372, 109619, 109151, 107738, 108454, 108455, 10798, 109485, 123326, 109485, 112294, 130500)
    """
    fatura = execute_query(conn, query_fatura)

    print(f"Quantidade de linhas no DataFrame 'fatura': {fatura.shape[0]}")


    # Função para ajustar os valores ao formato decimal(8, 2)
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None  # Mantém valores nulos como estão
        else:
            # Limitar para no máximo 8 dígitos, com 2 casas decimais
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

    # Aplicar a função na coluna 'valor_titulo'
    fatura['valor_fatura'] = fatura['valor_fatura'].apply(ajustar_decimal)
    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    fatura['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    fatura['year'], fatura['month'], fatura['day'] = now.year, now.month, now.day
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
    BUCKET_SOURCE_TRUSTED = "payments"
    FOLDER_DESTINATION_TRUSTED = "faturamento"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        fatura, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )