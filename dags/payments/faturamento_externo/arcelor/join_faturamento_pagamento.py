# Importanto Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np
import unicodedata
from airflow.models import Variable


def join_faturamento_pagamento(access_params=None, **kwargs):

    # Conectando com o banco de dados Trino
    conn = connect(
        host=Variable.get("TRINO_ENDPOINT"),
        port=Variable.get("TRINO_PORT"),
        user=Variable.get("TRINO_USER"),
        auth=BasicAuthentication(Variable.get("TRINO_USER"), Variable.get("TRINO_PASSWORD")),
        http_scheme="https",
    )


    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)
    

    # Query Faturamento Trusted
    query_fat_trusted = f"""
                        select 
                            * 
                        from deltalaketrusted.payments.faturamento_externo_arcelor
                        """

    fat_trusted = execute_query(conn, query_fat_trusted)
    fat_trusted = fat_trusted.drop_duplicates()

    print(f"Quantidade de linhas no DataFrame 'Faturamento': {fat_trusted.shape[0]}")


    cols_202 = [c for c in fat_trusted.columns if c.startswith("202")]

    fat_trusted["soma_faturamento"] = fat_trusted[cols_202].sum(axis=1)

    fat_trusted["soma_faturamento"] = fat_trusted["soma_faturamento"].apply(lambda x: f"{x:.0f}")


    # Removendo o que não tem Faturamento Arcelor

    fat_trusted["soma_faturamento"] = pd.to_numeric(fat_trusted["soma_faturamento"], errors="coerce")
    fat_trusted = fat_trusted[fat_trusted["soma_faturamento"] != 0].reset_index(drop=True)

    fat_trusted = fat_trusted.drop(columns=["soma_faturamento"])

    print(f"Quantidade de linhas no DataFrame 'Faturamento' após remover faturamentos com soma de 0: {fat_trusted.shape[0]}")



    # Query Pagamento Trusted
    query_pag_trusted = f"""
                        select 
                            * 
                        from deltalaketrusted.payments.pagamento_externo_arcelor
                        """

    pag_trusted = execute_query(conn, query_pag_trusted)
    pag_trusted = pag_trusted.drop_duplicates()

    print(f"Quantidade de linhas no DataFrame 'Pagamento': {pag_trusted.shape[0]}")

    pd.set_option('display.float_format', '{:.0f}'.format)



    # Tratando Faturamento Trusted

    fat_trusted = fat_trusted.drop(columns=['razao_social', 'unidade_consolidada','cidade', 'uf', 'cnae_principal', 'atualizado_em', 'year', 'month', 'day'])

    # Agrupando Faturamento Trusted
    fat_trusted = fat_trusted.groupby(['raiz_cnpj'], as_index=False).max()

    # Pivot Faturamento Trusted
    fat_trusted = fat_trusted.melt(id_vars=['raiz_cnpj'], 
                                var_name='anomes', 
                                value_name='valor_fat')


    # Tratando Pagamento Trusted

    pag_trusted = pag_trusted.drop(columns=['atualizado_em', 'year', 'month', 'day', 'unidade_consolidada'])


    # Agrupando Pagamento Trusted
    pag_trusted = pag_trusted.groupby(['raiz_cnpj'], as_index=False).max()

    # Pivot Pagamento Trusted
    pag_trusted = pag_trusted.melt(id_vars=['raiz_cnpj'], 
                                    value_vars=[col for col in pag_trusted.columns if col not in ['raiz_cnpj']], 
                                    var_name='auxiliar', 
                                    value_name='valor_pag')
    


    pag_trusted['anomes'] = pag_trusted['auxiliar'].str[-6:]  # Ano no final

    pag_trusted['tipo_valor'] = pag_trusted['auxiliar'].str[:-7]   # Mês no final

    pag_trusted = pag_trusted.drop(columns=['auxiliar'])


    pag_trusted = pag_trusted.pivot_table(index=['raiz_cnpj', 'anomes'], 
                            columns='tipo_valor', 
                            values='valor_pag', 
                            aggfunc='max').reset_index()

    pag_trusted = pag_trusted.drop(columns=['prazo_medio_atrasado'])



    # Merge Faturamento e Pagamento

    fat_pag = pd.merge(
        fat_trusted, 
        pag_trusted, 
        on=['raiz_cnpj', 'anomes'], 
        how='outer'
    )

    fat_pag = fat_pag.fillna(0)


    # Incluindo Razao Social Sacado

    # Extraindo os CNPJs do DataFrame 'fat_pag' e convertendo-os para uma lista
    cnpjs = fat_pag['raiz_cnpj'].unique().tolist()

    tamanho = len(cnpjs) // 3

    cnpj_part_1 = cnpjs[:tamanho]
    cnpj_part_2 = cnpjs[tamanho:2*tamanho]
    cnpj_part_3 = cnpjs[2*tamanho:]

    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str_1 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_1])
    cnpjs_str_2 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_2])
    cnpjs_str_3 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_3])


    query_receita_1 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_1})
                    """


    query_receita_2 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_2})
                    """

    query_receita_3 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_3})
                    """

    receita_1 = execute_query(conn, query_receita_1)
    receita_2 = execute_query(conn, query_receita_2)
    receita_3 = execute_query(conn, query_receita_3)

    receita = pd.concat([receita_1, receita_2, receita_3], ignore_index=True)

    fat_pag = pd.merge(
        fat_pag, 
        receita, 
        on=['raiz_cnpj'], 
        how='left'
    )

    fat_pag['razao_social'] = fat_pag['razao_social'].fillna('')


    # Definindo fornecedor base Arcelor
    fat_pag['fornecedor'] = "ARCELOR"


    # Query Faturamento Belgo - Gerando Fat Pag Join Belgo

    query_fat_belgo = f"""
                        with base_tratada as (
                        
                            select
                                substring(cnpj, 1, 8) as cnpj_raiz,
                                data_lancamento_anomes as anomes_ref,
                                valor as valor_fat,
                                case when (data_compensacao is null and dias_diff_pagamento > 0) then valor else 0 end as vlr_inad_corrente,
                                case when (data_compensacao is not null and dias_diff_pagamento > 0) then valor else 0 end as vlr_receb_atrasado,
                                
                                case when (data_compensacao is not null and dias_diff_pagamento >= 16 and dias_diff_pagamento <= 30) then valor else 0 end as vlr_receb_atrasado_16_a_30,
                                case when (data_compensacao is not null and dias_diff_pagamento >= 1 and dias_diff_pagamento <= 5) then valor else 0 end as vlr_receb_atrasado_1_a_5,
                                case when (data_compensacao is not null and dias_diff_pagamento >= 6 and dias_diff_pagamento <= 15) then valor else 0 end as vlr_receb_atrasado_6_a_15,
                                case when (data_compensacao is not null and dias_diff_pagamento > 30) then valor else 0 end as vlr_receb_atrasado_maior_30,
                                case when (data_compensacao is not null) then valor else 0 end as vlr_recebido,
                                
                                case when (data_compensacao is null) then valor else 0 end as vlr_total_titulo_aberto,
                                case when (data_compensacao is null and dias_diff_pagamento >= 16 and dias_diff_pagamento <= 30) then valor else 0 end as vlr_vencido_16_a_30,
                                case when (data_compensacao is null and dias_diff_pagamento >= 6 and dias_diff_pagamento <= 15) then valor else 0 end as vlr_vencido_6_a_15,
                                case when (data_compensacao is null and dias_diff_pagamento >= 1 and dias_diff_pagamento <= 5) then valor else 0 end as vlr_vencido_ate_5,
                                case when (data_compensacao is null and dias_diff_pagamento > 30) then valor else 0 end as vlr_vencido_maior_30,
                                
                                nome_cliente as razao_social
                                
                                
                            from (
                                    select
                                        *,
                                        date_diff('day', cb.vencimento_liquido, coalesce(cb.data_compensacao, DATE '2025-11-11')) AS dias_diff_pagamento,
                                        date_format(data_lancamento, '%Y%m') AS data_lancamento_anomes
                                    from minioraw.planejamento_comercial.clientes_belgo as cb ) as sub
                        )
                        
                        select
                            cnpj_raiz as raiz_cnpj,
                            anomes_ref as anomes,
                            sum(valor_fat) as valor_fat,
                            sum(vlr_inad_corrente) as vlr_inad_corrente,
                            sum(vlr_receb_atrasado) as vlr_receb_atrasado,
                            sum(vlr_receb_atrasado_16_a_30) as vlr_receb_atrasado_16_a_30,
                            sum(vlr_receb_atrasado_1_a_5) as vlr_receb_atrasado_1_a_5,
                            sum(vlr_receb_atrasado_6_a_15) as vlr_receb_atrasado_6_a_15,
                            sum(vlr_receb_atrasado_maior_30) as vlr_receb_atrasado_maior_30,
                            sum(vlr_recebido) as vlr_recebido,
                            sum(vlr_total_titulo_aberto) as vlr_total_titulo_aberto,
                            sum(vlr_vencido_16_a_30) as vlr_vencido_16_a_30,
                            sum(vlr_vencido_6_a_15) as vlr_vencido_6_a_15,
                            sum(vlr_vencido_ate_5) as vlr_vencido_ate_5,
                            sum(vlr_vencido_maior_30) as vlr_vencido_maior_30,
                            max(e.razao_social) as razao_social,
                            'BELGO' as fornecedor
                        from base_tratada as bt
                        left join (select  distinct
                                            cnpj_raiz as raiz_cnpj,
                                            razao_social
                                    from deltalaketrusted.receita_federal.empresas) as e
                        on bt.cnpj_raiz = e.raiz_cnpj 
                        group by 1, 2
                        """

    fat_pag_belgo = execute_query(conn, query_fat_belgo)


    fat_pag_final = pd.concat([fat_pag, fat_pag_belgo], ignore_index=True)


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    fat_pag_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    fat_pag_final['year'], fat_pag_final['month'], fat_pag_final['day'] = now.year, now.month, now.day

    print('Exportando...')

    fat_pag_final = fat_pag_final.reset_index(drop=True)


    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
    storage_options = {
        "AWS_ACCESS_KEY_ID": Variable.get("MINIO_TRUSTED_ACCESS_KEY"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("MINIO_TRUSTED_SECRET_KEY"),
        "AWS_ENDPOINT_URL": f"https://{Variable.get('MINIO_TRUSTED_ENDPOINT')}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = 'payments'
    FOLDER_DESTINATION_TRUSTED = 'faturamento_externo/fat_pag'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        fat_pag_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )