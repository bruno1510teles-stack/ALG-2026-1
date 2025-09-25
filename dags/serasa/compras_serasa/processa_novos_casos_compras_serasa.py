# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from io import BytesIO
from decimal import Decimal, ROUND_DOWN
import logging
import numpy as np
import time
import os

def processa_serasa_diario (access_params=None,  **kwargs):

    # Conectando ao Trino para Leitura
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
    

    # HOJE
    hoje = datetime.today().date()

    # 10 DIAS ATRÁS
    dez_dias_atras = hoje - timedelta(days=20)


    print('Buscando novos casos de compra do serasa...')

    ### QUERY NOVAS COMPRAS DO SERASA

    query = f"""
            with base_data as (
                select re.id id, re.created_date data_consulta, rc.json_content
                    ,substring(last_re.value,1,8) cnpj_raiz , re.reports_id
                from postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                inner join postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
                inner join (select min(re.id) re_id, rc.json_content, pi2.value
                                ,ROW_NUMBER() OVER (PARTITION BY pi2.value ORDER BY json_content desc) AS rn
                            from postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_definition rd on rd.id = re.definition_id and rd."type" = 'RELATORIO_AVANCADO_PJ_ANALITICO'
                            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
                            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_involvement ri on ri.report_execution_id = re.id
                            inner join postgres.exrp_{Variable.get('STAGE')}_default.party_identification pi2 on pi2.party_id = ri.party_id
                            where re.resolution = 'DONE'
                            group by rc.json_content, pi2.value ) as last_re 
                    on last_re.re_id = re.id
                where try_cast( re.created_date as date ) >= try_cast( '{dez_dias_atras}' as date )
        ),
        restritivos_pj as (
                with tipo_pendencia(tipo, descricao) as (
                    values 
                        ('PEFIN', 'PEFIN')
                        ,('REFIN', 'REFIN')
                        ,('COLLECTION_RECORDS', 'DIVIDA VENCIDA')
                        ,('CHECK', 'CHEQUE')
                        ,('NOTARY', 'PROTESTO')
                        ,('BANKRUPTSPATICIPATION', 'FALENCIA')
                        ,('JUDGEMENTFILINGS', 'ACAO JUDICIAL')
                    )
                    select distinct bd.id, cast(bd.data_consulta as date) data_consulta
                        ,bd.cnpj_raiz, tp.descricao grupo_ocorrencia, s.count quantidade_ocorrencia
                        ,s.first_occurrence ano_mes_primeiro, s.last_occurrence ano_mes_ultimo
                        ,coalesce(s.balance, 0) valor_total
                    from tipo_pendencia tp 
                    inner join base_data bd on true
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = bd.reports_id
                    left join postgres.exrp_{Variable.get('STAGE')}_default.report r on rs.id = r.reports_id
                    left join postgres.exrp_{Variable.get('STAGE')}_default.negative_data nd on nd.id = r.negative_data_id
                    left join postgres.exrp_{Variable.get('STAGE')}_default.negative_data_item ndi on ndi.id in (
                                nd.pefin_id, 
                                nd.refin_id, 
                                nd.collection_records_id, 
                                nd.check_id,
                                nd.notary_id)
                                and ndi."_object_type" = tp.tipo
                    left join postgres.exrp_{Variable.get('STAGE')}_default.facts f on f.id = r.facts_id
                    left join postgres.exrp_{Variable.get('STAGE')}_default.bankrupts b on b.id = f.bankrupts_id and tp.tipo = 'BANKRUPTSPATICIPATION'
                    left join postgres.exrp_{Variable.get('STAGE')}_default.judgement_filings jf on jf.id = f.judgement_filings_id and tp.tipo = 'JUDGEMENTFILINGS'
                    left join postgres.exrp_{Variable.get('STAGE')}_default.summary s on s.id in (ndi.summary_id, b.summary_id, jf.summary_id)
        ),
        
        total_restritivos_pj as (
                    select t1.id as id, t1.data_consulta, t1.cnpj_raiz,
                        sum(valor_total) as total_restritivos
                    from restritivos_pj t1
                    group by t1.id, t1.data_consulta, t1.cnpj_raiz
        ),
        
        qtde_cheque_pj as (
                    select rpj.id, coalesce(rpj.quantidade_ocorrencia,0) as qtd_cheque
                    from restritivos_pj rpj
                    where grupo_ocorrencia = 'CHEQUE'
        ),
        
        restritivos_socios as (
                    select bd.id, p.id partner_id, p.kind_person
                        ,p.document, p.document_branch, document_digit
                        ,case 
                            when d.debt_type = 'BANKRUPTSPATICIPATION' then 'FALENCIA'
                            when d.debt_type = 'CHECKCCF' then 'CHEQUE'
                            when d.debt_type = 'COLLECTIONRECORDS' then 'DIVIDA VENCIDA' --Parece retornar apenas a última
                            when d.debt_type = 'FINANCIAL' then 'REFIN'
                            when d.debt_type = 'JUDGEMENTFILINGS' then 'ACAO JUDICIAL'
                            when d.debt_type = 'MARKET' then 'PEFIN'
                            when d.debt_type = 'NOTARY' then 'PROTESTO' end as grupo_ocorrencia
                        ,s.count quantidade_ocorrencia
                        ,s.last_occurrence ano_mes_ultimo
                        ,coalesce(s.balance, 0) valor_total
                    from base_data bd
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = bd.reports_id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 on of2.id = rs.optional_features_id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.qsa_complete_report qcr on qcr.id = of2.qsa_complete_report_id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.person p on p.qsa_complete_report_id = qcr.id and p."_object_type" = 'PARTNER'
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.debt d on d.person_id = p.id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.summary s on s.id = d.summary_id
        ),
        
        qtde_cheque_pf as (
                    select rs.id, coalesce(sum(rs.quantidade_ocorrencia),0) cheque_pf
                    from restritivos_socios rs
                    where grupo_ocorrencia = 'CHEQUE'
                    group by rs.id
        ),
        
        total_restritivos_socio as (
                    select t1.id as id, sum(valor_total) as restritivos_pf
                    from restritivos_socios t1
                    group by t1.id
        ),
        
        score_pj as (
                    select bd.id, s.score as score_positivo_pj
                        ,case when s.message  = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' then 1 else 0 end as grande_empresa
                    from base_data bd
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = bd.reports_id
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 on of2.id = rs.optional_features_id
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.score s on s.id = of2.score_id
        )  
                select 
                    trp.id,
                    trp.data_consulta,
                    trp.cnpj_raiz,
                    score.score_positivo_pj,
                    cast(score.grande_empresa as int) as grande_empresa,
                    coalesce(trp.total_restritivos, 0) as restritivos_pj,
                    coalesce(trs.restritivos_pf, 0) as restritivos_pf,
                    coalesce(cpf.cheque_pf, 0) as cheque_pf,
                    coalesce(cpj.qtd_cheque, 0) as cheque_pj
                from total_restritivos_pj trp
                    left join qtde_cheque_pj cpj on trp.id = cpj.id
                    left join qtde_cheque_pf cpf on trp.id = cpf.id
                    left join total_restritivos_socio trs on trp.id = trs.id
                    left join score_pj score on trp.id = score.id
            """

    df_serasa = execute_query (conn, query)

    df_serasa['total_restritivos'] = df_serasa['restritivos_pj'] + df_serasa['restritivos_pf']
    df_serasa['total_cheques'] = df_serasa['cheque_pf'] + df_serasa['cheque_pj']


    # Normalizando tipos
    df_serasa["id"] = df_serasa["id"].astype(str)
    df_serasa["cnpj_raiz"] = df_serasa["cnpj_raiz"].astype(str)

    df_serasa["score_positivo_pj"] = pd.to_numeric(df_serasa["score_positivo_pj"], errors="coerce").astype(float)

    for col in ["grande_empresa", "restritivos_pj", "restritivos_pf", "cheque_pf", "cheque_pj", "total_restritivos", "total_cheques"]:
        df_serasa[col] = pd.to_numeric(df_serasa[col], errors="coerce").fillna(0).astype(int)


    print('Base de novos casos tratadas...')
    print('Carregando acumulada...')


    query_acumulada = f"""
            select * from deltalaketrusted.serasa.historico_compras_serasa
            """

    df_acumulado = execute_query (conn, query_acumulada)


    ### Regra para atualizar essas novas compras na nossa acumulada

    ids_novos = df_serasa["id"].drop_duplicates()

    ### Removendo do acumulado os ids que já existem no novo
    df_acumulado_filtrado = df_acumulado[~df_acumulado["id"].isin(ids_novos)]

    ### Concatenando acumulado filtrado + novos
    df_final = pd.concat([df_acumulado_filtrado, df_serasa], ignore_index=True)

    print('--------------------------------------------')
    print('Novos casos de compra:')
    print(len(df_serasa))
    print('--------------------------------------------')
    print('Casos na acumulada:')
    print(len(df_acumulado))
    print('--------------------------------------------')
    print('Casos após desconsiderar casos novos que já estão na acumulada:')
    print(len(df_acumulado_filtrado))
    print('--------------------------------------------')
    print('Casos tabela final:')
    print(len(df_final))


    # Remove a coluna "nome_coluna"
    df_final = df_final.drop(columns=["atualizado_em", "year", "month", "day"])
    
    # Colunas de data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")


    # Configuração do Delta Lake
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    BUCKET_SOURCE_TRUSTED = "serasa-trusted"
    FOLDER_DESTINATION_TRUSTED = "historico_compras_serasa"

    # Escrevendo no Delta Lake com schema fixado
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        df_final,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )