# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
import numpy as np
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from airflow.utils.log.logging_mixin import LoggingMixin
from deltalake import write_deltalake, DeltaTable
import joblib


def cria_tabela_pagamento_historico_task (access_params=None, **kwargs):

    ### CONECTANDO COM O TRINO
    conn = connect(
        host='trino.alpe.com.br',
        port=443,
        user='vinicius_teixeira',
        auth=BasicAuthentication('vinicius_teixeira', 'TEjcv)-+b}o!QL5CM2:p'),
        http_scheme="https",)
    

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    

    print('Conexão bem sucedida!')
    
    print('Iniciando extração de dados Serasa...')
    
    ### QUERY SERASA

    query_serasa = f"""
                    with infos_pagamento_serasa as (
                    
                        SELECT  r.id AS id_base,
                                r.created_date,
                                ir.document_number as cnpj,
                                (ra.range_value_from + ra.range_value_to) / 2.0 AS media,
                                ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY md.id ASC) AS ordem_md,
                                CAST('20' || substring(md."month", 5, 2) AS integer) AS ano,
                                CASE substring(md."month", 1, 3)
                                    WHEN 'JAN' THEN 1
                                    WHEN 'FEV' THEN 2
                                    WHEN 'MAR' THEN 3
                                    WHEN 'ABR' THEN 4
                                    WHEN 'MAI' THEN 5
                                    WHEN 'JUN' THEN 6
                                    WHEN 'JUL' THEN 7
                                    WHEN 'AGO' THEN 8
                                    WHEN 'SET' THEN 9
                                    WHEN 'OUT' THEN 10
                                    WHEN 'NOV' THEN 11
                                    WHEN 'DEZ' THEN 12
                                END AS mes
                            FROM postgres.exrp_prd_default.report r
                            INNER JOIN postgres.exrp_prd_default.identification_report ir                    ON ir.id = r.identification_report_id
                            INNER JOIN postgres.exrp_prd_default.advanced_commercial_payment_history acph    ON acph.id = r.advanced_commercial_payment_history_id
                            INNER JOIN postgres.exrp_prd_default.payment_history ph                          ON ph.id = acph.payment_history_id
                            INNER JOIN postgres.exrp_prd_default.month_detailed md                           ON md.month_detail_id = ph.month_detail_id
                            INNER JOIN postgres.exrp_prd_default."range" ra                                  ON md.id = ra.month_detailed_id 
                            WHERE ra.name = 'PONTUAL'
                    )
                    select 
                        sub2.raiz_cnpj,
                        
                        'SERASA' as fonte_informacao,
                        
                        sub2.total_compras_ultimos_6_meses,
                        sub2.valor_compras_ultimos_6_meses,
                        
                        sub2.total_compras_ultimos_8_meses,
                        sub2.valor_compras_ultimos_8_meses,
                        
                        sub2.total_compras_ultimos_10_meses,
                        sub2.valor_compras_ultimos_10_meses,
                        
                        case when sub2.total_compras_ultimos_6_meses = 0 then 0 else sub2.valor_compras_ultimos_6_meses / sub2.total_compras_ultimos_6_meses end as media_compras_ultimos_6_meses,
                        case when sub2.total_compras_ultimos_8_meses = 0 then 0 else sub2.valor_compras_ultimos_8_meses / sub2.total_compras_ultimos_8_meses end as media_compras_ultimos_8_meses,
                        case when sub2.total_compras_ultimos_10_meses = 0 then 0 else sub2.valor_compras_ultimos_10_meses / sub2.total_compras_ultimos_10_meses end as media_compras_ultimos_10_meses,
                        
                        sub2.media_pagamento_total
                            
                    from (
                            select
                                sub1.*,
                                ROW_NUMBER() OVER (
                                        PARTITION BY sub1.raiz_cnpj
                                        ORDER BY TRY_CAST(sub1.created_date AS DATE) DESC
                                    ) AS rn
                            from (
                                    select 	id_base, 
                                            substring(cnpj, 1, 8) as raiz_cnpj, 
                                            created_date,
                                            count(*) as total_compras,
                                            sum(case when date_parse(CAST(ano AS varchar) || '-' || lpad(CAST(mes AS varchar), 2, '0') || '-01','%Y-%m-%d') 
                                                >= date_trunc('month', current_date) - interval '5' month then 1 else 0 end ) as total_compras_ultimos_6_meses,
                                            sum(case when date_parse(CAST(ano AS varchar) || '-' || lpad(CAST(mes AS varchar), 2, '0') || '-01','%Y-%m-%d') 
                                                >= date_trunc('month', current_date) - interval '7' month then 1 else 0 end ) as total_compras_ultimos_8_meses,
                                            sum(case when date_parse(CAST(ano AS varchar) || '-' || lpad(CAST(mes AS varchar), 2, '0') || '-01','%Y-%m-%d') 
                                                >= date_trunc('month', current_date) - interval '9' month then 1 else 0 end ) as total_compras_ultimos_10_meses,
                                            
                                            sum(case when date_parse(CAST(ano AS varchar) || '-' || lpad(CAST(mes AS varchar), 2, '0') || '-01','%Y-%m-%d') 
                                                >= date_trunc('month', current_date) - interval '5' month then media else 0 end ) as valor_compras_ultimos_6_meses,
                                            sum(case when date_parse(CAST(ano AS varchar) || '-' || lpad(CAST(mes AS varchar), 2, '0') || '-01','%Y-%m-%d') 
                                                >= date_trunc('month', current_date) - interval '7' month then media else 0 end ) as valor_compras_ultimos_8_meses,
                                            sum(case when date_parse(CAST(ano AS varchar) || '-' || lpad(CAST(mes AS varchar), 2, '0') || '-01','%Y-%m-%d') 
                                                >= date_trunc('month', current_date) - interval '9' month then media else 0 end ) as valor_compras_ultimos_10_meses,
                                            
                                            avg(media) as media_pagamento_total
                                    from infos_pagamento_serasa
                                    group by id_base, 2, created_date ) as sub1 ) as sub2
                    where sub2.rn = 1
                    """

    df_serasa = execute_query (conn, query_serasa)

    print('Quantidade de linhas que retornaram em df_serasa:')
    print(len(df_serasa))

    print('Iniciando extração de dados Alpe...')

    ### QUERY ALPE

    query_alpe = f"""
                    with infos_pagamento_alpe as (
                            select  substring(regexp_replace(cnpj_sacado, '[./-]', ''), 1, 8) as raiz_cnpj,
                                    safra_concessao,
                                    sum(valor_face) as vop
                            from deltalaketrusted.payments.boletos_internos as bi
                            group by 1, safra_concessao
                    )
                    
                    select
                        sub2.raiz_cnpj,
                        
                        'ALPE' as fonte_informacao,
                        
                        sub2.total_compras_ultimos_6_meses,
                        sub2.valor_compras_ultimos_6_meses,
                        
                        sub2.total_compras_ultimos_8_meses,
                        sub2.valor_compras_ultimos_8_meses,
                        
                        sub2.total_compras_ultimos_10_meses,
                        sub2.valor_compras_ultimos_10_meses,
                        
                        case when sub2.total_compras_ultimos_6_meses = 0 then 0 else sub2.valor_compras_ultimos_6_meses / sub2.total_compras_ultimos_6_meses end as media_compras_ultimos_6_meses,
                        case when sub2.total_compras_ultimos_8_meses = 0 then 0 else sub2.valor_compras_ultimos_8_meses / sub2.total_compras_ultimos_8_meses end as media_compras_ultimos_8_meses,
                        case when sub2.total_compras_ultimos_10_meses = 0 then 0 else sub2.valor_compras_ultimos_10_meses / sub2.total_compras_ultimos_10_meses end as media_compras_ultimos_10_meses,
                        
                        sub2.media_pagamento_total
                    from (
                        select
                            raiz_cnpj,
                            
                            sum(case when safra_concessao >= date_trunc('month', current_date) - interval '5' month then 1 else 0 end) as total_compras_ultimos_6_meses,
                            sum(case when safra_concessao >= date_trunc('month', current_date) - interval '7' month then 1 else 0 end) as total_compras_ultimos_8_meses,
                            sum(case when safra_concessao >= date_trunc('month', current_date) - interval '9' month then 1 else 0 end) as total_compras_ultimos_10_meses,
                            
                            sum(case when safra_concessao >= date_trunc('month', current_date) - interval '5' month then vop else 0 end) as valor_compras_ultimos_6_meses,
                            sum(case when safra_concessao >= date_trunc('month', current_date) - interval '7' month then vop else 0 end) as valor_compras_ultimos_8_meses,
                            sum(case when safra_concessao >= date_trunc('month', current_date) - interval '9' month then vop else 0 end) as valor_compras_ultimos_10_meses,
                            
                            avg(vop) as media_pagamento_total
                        
                        from infos_pagamento_alpe
                        group by raiz_cnpj ) as sub2
                    """

    df_alpe = execute_query (conn, query_alpe)

    print('Quantidade de linhas que retornaram em df_alpe:')
    print(len(df_alpe))


    print('Iniciando extração de dados Arcelor...')

    ### QUERY ARCELOR

    query_arcelor = f"""
                    with infos_faturamento_arcelor as (	
                        select 
                            raiz_cnpj,
                            try_cast(date_parse(CAST(anomes AS varchar) || '01', '%Y%m%d') as date) as safra,
                            sum(valor_fat) as faturamento_mensal
                        from deltalaketrusted.payments.fat_pag_join 
                        group by raiz_cnpj, 2
                    ),
                    
                    referencia as (
                        select max(safra) AS data_referencia
                        FROM infos_faturamento_arcelor
                    )
                    
                    select
                        sub2.raiz_cnpj,
                        
                        'ARCELOR' as fonte_informacao,
                        
                        sub2.total_compras_ultimos_6_meses,
                        sub2.valor_compras_ultimos_6_meses,
                        
                        sub2.total_compras_ultimos_8_meses,
                        sub2.valor_compras_ultimos_8_meses,
                        
                        sub2.total_compras_ultimos_10_meses,
                        sub2.valor_compras_ultimos_10_meses,
                        
                        case when sub2.total_compras_ultimos_6_meses = 0 then 0 else sub2.valor_compras_ultimos_6_meses / sub2.total_compras_ultimos_6_meses end as media_compras_ultimos_6_meses,
                        case when sub2.total_compras_ultimos_8_meses = 0 then 0 else sub2.valor_compras_ultimos_8_meses / sub2.total_compras_ultimos_8_meses end as media_compras_ultimos_8_meses,
                        case when sub2.total_compras_ultimos_10_meses = 0 then 0 else sub2.valor_compras_ultimos_10_meses / sub2.total_compras_ultimos_10_meses end as media_compras_ultimos_10_meses,
                        
                        sub2.media_pagamento_total
                    from (
                        SELECT
                            b.raiz_cnpj,
                            SUM(CASE WHEN b.safra >= date_add('month', -5, r.data_referencia) THEN 1 ELSE 0 END) AS total_compras_ultimos_6_meses,
                            SUM(CASE WHEN b.safra >= date_add('month', -7, r.data_referencia) THEN 1 ELSE 0 END) AS total_compras_ultimos_8_meses,
                            SUM(CASE WHEN b.safra >= date_add('month', -9, r.data_referencia) THEN 1 ELSE 0 END) AS total_compras_ultimos_10_meses,
                            
                            SUM(CASE WHEN b.safra >= date_add('month', -5, r.data_referencia) THEN faturamento_mensal ELSE 0 END) AS valor_compras_ultimos_6_meses,
                            SUM(CASE WHEN b.safra >= date_add('month', -7, r.data_referencia) THEN faturamento_mensal ELSE 0 END) AS valor_compras_ultimos_8_meses,
                            SUM(CASE WHEN b.safra >= date_add('month', -9, r.data_referencia) THEN faturamento_mensal ELSE 0 END) AS valor_compras_ultimos_10_meses,
                            
                            avg(faturamento_mensal) as media_pagamento_total
                        FROM infos_faturamento_arcelor b
                        CROSS JOIN referencia r
                        GROUP BY b.raiz_cnpj ) as sub2
                    where sub2.valor_compras_ultimos_10_meses > 0
                    """

    df_arcelor = execute_query (conn, query_arcelor)

    print('Quantidade de linhas que retornaram em df_arcelor:')
    print(len(df_arcelor))


    ## CONCATENANDO TODOS OS DFS

    print('Concatenando DFs:')

    df_final = pd.concat([df_serasa, df_alpe, df_arcelor], ignore_index=True)

    print('Quantidade de linhas depois de juntar DFs:')
    print(len(df_final))


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

    df_final = df_final.reset_index(drop=True)


    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 


    try:
        logger.info("Iniciando salvamento das informações")
            
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
            "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_TRUSTED = 'payments'
        FOLDER_DESTINATION_TRUSTED = 'pagamento_historico'

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
            df_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")