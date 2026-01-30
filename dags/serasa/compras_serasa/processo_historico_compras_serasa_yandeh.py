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


def processa_historico_serasa_yandeh (access_params=None,  **kwargs):

    ### Conectando com o Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(rows, columns=columns)
    

    print('Processando Query das Compras do Serasa...')


    ### QUERY COMPRAS SERASA YANDEH

    query   =   f"""
                with base_data as (
                                            select 
                                                re.id id
                                                ,re.created_date data_consulta
                                                ,rc.json_content
                                                ,substring(last_re.value,1,8) cnpj_raiz
                                                ,re.reports_id
                                                ,last_re.tipo_produto
                                            from 
                                                postgres.exrp_prd_default.report_execution re
                                                inner join postgres.exrp_prd_default.report_content rc on rc.id = re.content_id
                                                inner join (
                                                            select 
                                                                min(re.id) re_id
                                                                ,rc.json_content
                                                                ,rd."type" as tipo_produto
                                                                ,pi2.value
                                                            from postgres.exrp_prd_default.report_execution re
                                                                inner join postgres.exrp_prd_default.report_definition rd on rd.id = re.definition_id and rd."type" in ('SERASA_RELATO_YANDEH', 'RELATORIO_DADOS_AVULSOS_PJ_YANDEH')
                                                                inner join postgres.exrp_prd_default.report_content rc on rc.id = re.content_id
                                                                inner join postgres.exrp_prd_default.report_involvement ri on ri.report_execution_id = re.id
                                                                inner join postgres.exrp_prd_default.party_identification pi2 on pi2.party_id = ri.party_id
                                                            where
                                                                re.resolution = 'DONE'
                                                            group by  
                                                                rc.json_content
                                                                ,pi2.value
                                                                ,rd."type"
                                            ) last_re on last_re.re_id = re.id
                )
                
                ,restritivos_pj as (
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
                                            select distinct
                                                bd.id
                                                ,cast(bd.data_consulta as date) data_consulta
                                                ,bd.cnpj_raiz
                                                ,bd.tipo_produto
                                                ,tp.descricao grupo_ocorrencia
                                                ,s.count quantidade_ocorrencia
                                                ,s.first_occurrence ano_mes_primeiro
                                                ,s.last_occurrence ano_mes_ultimo
                                                ,coalesce(s.balance, 0) valor_total
                                            from tipo_pendencia tp 
                                                inner join base_data bd on true
                                                inner join postgres.exrp_prd_default.reports rs on rs.id = bd.reports_id
                                                left join postgres.exrp_prd_default.report r on rs.id = r.reports_id
                                                left join postgres.exrp_prd_default.negative_data nd on nd.id = r.negative_data_id
                                                left join postgres.exrp_prd_default.negative_data_item ndi on ndi.id in (
                                                        nd.pefin_id, 
                                                        nd.refin_id, 
                                                        nd.collection_records_id, 
                                                        nd.check_id,
                                                        nd.notary_id)
                                                        and ndi."_object_type" = tp.tipo
                                                left join postgres.exrp_prd_default.facts f on f.id = r.facts_id
                                                left join postgres.exrp_prd_default.bankrupts b on b.id = f.bankrupts_id and tp.tipo = 'BANKRUPTSPATICIPATION'
                                                left join postgres.exrp_prd_default.judgement_filings jf on jf.id = f.judgement_filings_id and tp.tipo = 'JUDGEMENTFILINGS'
                                                left join postgres.exrp_prd_default.summary s on s.id in (ndi.summary_id, b.summary_id, jf.summary_id)
                )
                
                ,total_restritivos_pj as (
                                            select 
                                                t1.id as id,
                                                t1.data_consulta,
                                                t1.tipo_produto,
                                                t1.cnpj_raiz,
                                                sum(case when t1.grupo_ocorrencia = 'PEFIN' then quantidade_ocorrencia else 0 end) as qtd_pefin_pj,
                                                sum(case when t1.grupo_ocorrencia = 'PEFIN' then valor_total else 0 end) as valor_pefin_pj,
                                                
                                                sum(case when t1.grupo_ocorrencia = 'REFIN' then quantidade_ocorrencia else 0 end) as qtd_refin_pj,
                                                sum(case when t1.grupo_ocorrencia = 'REFIN' then valor_total else 0 end) as valor_refin_pj,
                                                
                                                sum(case when t1.grupo_ocorrencia = 'DIVIDA VENCIDA' then quantidade_ocorrencia else 0 end) as qtd_divida_vencida_pj,
                                                sum(case when t1.grupo_ocorrencia = 'DIVIDA VENCIDA' then valor_total else 0 end) as valor_divida_vencida_pj,
                                                
                                                sum(case when t1.grupo_ocorrencia = 'CHEQUE' then quantidade_ocorrencia else 0 end) as qtd_cheque_pj,
                                                sum(case when t1.grupo_ocorrencia = 'CHEQUE' then valor_total else 0 end) as valor_cheque_pj,
                                                
                                                sum(case when t1.grupo_ocorrencia = 'PROTESTO' then quantidade_ocorrencia else 0 end) as qtd_protesto_pj,
                                                sum(case when t1.grupo_ocorrencia = 'PROTESTO' then valor_total else 0 end) as valor_protesto_pj,
                                                
                                                sum(case when t1.grupo_ocorrencia = 'FALENCIA' then quantidade_ocorrencia else 0 end) as qtd_falencia_pj,
                                                sum(case when t1.grupo_ocorrencia = 'FALENCIA' then valor_total else 0 end) as valor_falencia_pj,
                                                
                                                sum(case when t1.grupo_ocorrencia = 'ACAO JUDICIAL' then quantidade_ocorrencia else 0 end) as qtd_acao_judicial_pj,
                                                sum(case when t1.grupo_ocorrencia = 'ACAO JUDICIAL' then valor_total else 0 end) as valor_acao_judicial_pj,
                                                
                                                sum(quantidade_ocorrencia) as qtd_total_restritivos_pj,
                                                sum(valor_total) as valor_total_restritivos_pj
                                                
                                            from restritivos_pj t1
                                            group by
                                                t1.id,
                                                t1.data_consulta,
                                                t1.tipo_produto,
                                                t1.cnpj_raiz
                )	    
                
                ,restritivos_socios as (
                                            select distinct
                                                bd.id
                                                ,p.kind_person
                                                ,p.document
                                                ,p.document_branch
                                                ,document_digit
                                                ,case 
                                                    when d.debt_type = 'BANKRUPTSPATICIPATION' then 'FALENCIA'
                                                    when d.debt_type = 'CHECKCCF' then 'CHEQUE'
                                                    when d.debt_type = 'COLLECTIONRECORDS' then 'DIVIDA VENCIDA'
                                                    when d.debt_type = 'FINANCIAL' then 'REFIN'
                                                    when d.debt_type = 'JUDGEMENTFILINGS' then 'ACAO JUDICIAL'
                                                    when d.debt_type = 'MARKET' then 'PEFIN'
                                                    when d.debt_type = 'NOTARY' then 'PROTESTO'
                                                end as grupo_ocorrencia
                                                ,s.count quantidade_ocorrencia
                                                ,s.last_occurrence ano_mes_ultimo
                                                ,coalesce(s.balance, 0) valor_total
                                            from base_data bd
                                                inner join postgres.exrp_prd_default.reports rs on rs.id = bd.reports_id
                                                inner join postgres.exrp_prd_default.optional_features of2 on of2.id = rs.optional_features_id
                                                inner join postgres.exrp_prd_default.qsa_complete_report qcr on qcr.id = of2.qsa_complete_report_id
                                                inner join postgres.exrp_prd_default.person p on p.qsa_complete_report_id = qcr.id
                                                inner join postgres.exrp_prd_default.debt d on d.person_id = p.id
                                                inner join postgres.exrp_prd_default.summary s on s.id = d.summary_id
                )
                , total_restritivos_pf as (
                
                                            select
                                                rs.id,
                                                sum(case when rs.grupo_ocorrencia = 'PEFIN' then quantidade_ocorrencia else 0 end) as qtd_pefin_pf,
                                                    sum(case when rs.grupo_ocorrencia = 'PEFIN' then valor_total else 0 end) as valor_pefin_pf,
                                                    
                                                    sum(case when rs.grupo_ocorrencia = 'REFIN' then quantidade_ocorrencia else 0 end) as qtd_refin_pf,
                                                    sum(case when rs.grupo_ocorrencia = 'REFIN' then valor_total else 0 end) as valor_refin_pf,
                                                    
                                                    sum(case when rs.grupo_ocorrencia = 'DIVIDA VENCIDA' then quantidade_ocorrencia else 0 end) as qtd_divida_vencida_pf,
                                                    sum(case when rs.grupo_ocorrencia = 'DIVIDA VENCIDA' then valor_total else 0 end) as valor_divida_vencida_pf,
                                                    
                                                    sum(case when rs.grupo_ocorrencia = 'CHEQUE' then quantidade_ocorrencia else 0 end) as qtd_cheque_pf,
                                                    sum(case when rs.grupo_ocorrencia = 'CHEQUE' then valor_total else 0 end) as valor_cheque_pf,
                                                    
                                                    sum(case when rs.grupo_ocorrencia = 'PROTESTO' then quantidade_ocorrencia else 0 end) as qtd_protesto_pf,
                                                    sum(case when rs.grupo_ocorrencia = 'PROTESTO' then valor_total else 0 end) as valor_protesto_pf,
                                                    
                                                    sum(case when rs.grupo_ocorrencia = 'FALENCIA' then quantidade_ocorrencia else 0 end) as qtd_falencia_pf,
                                                    sum(case when rs.grupo_ocorrencia = 'FALENCIA' then valor_total else 0 end) as valor_falencia_pf,
                                                    
                                                    sum(case when rs.grupo_ocorrencia = 'ACAO JUDICIAL' then quantidade_ocorrencia else 0 end) as qtd_acao_judicial_pf,
                                                    sum(case when rs.grupo_ocorrencia = 'ACAO JUDICIAL' then valor_total else 0 end) as valor_acao_judicial_pf,
                                                    
                                                    sum(quantidade_ocorrencia) as qtd_total_restritivos_pf,
                                                    sum(valor_total) as valor_total_restritivos_pf
                                                
                                            from restritivos_socios as rs
                                            group by rs.id
                )
                    
                ,score_pj as (
                                            select 
                                                bd.id
                                                ,s.score "Score Positivo PJ"
                                                ,case when s.message  = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' then 1 else 0 end as grande_empresa
                                            from base_data bd
                                                inner join postgres.exrp_prd_default.reports rs on rs.id = bd.reports_id
                                                inner join postgres.exrp_prd_default.optional_features of2 on of2.id = rs.optional_features_id
                                                inner join postgres.exrp_prd_default.score s on s.id = of2.score_id
                )
                
                select 
                    trp.*,
                    trpf.*,
                    score."Score Positivo PJ" as score_positivo_pj,
                    score.grande_empresa
                from total_restritivos_pj trp
                left join score_pj score on trp.id = score.id
                left join total_restritivos_pf trpf on trp.id = trpf.id
                """


    df_serasa_yandeh = execute_query (conn, query)

    print(f"Quantidade de linhas no DataFrame 'df_serasa_yandeh': {df_serasa_yandeh.shape[0]}")


    # Normalizando Tipos de Dados
    df_serasa_yandeh["id"] = df_serasa_yandeh["id"].astype(str)
    df_serasa_yandeh["data_consulta"] = (
        pd.to_datetime(df_serasa_yandeh["data_consulta"], errors="coerce")
        .dt.tz_localize(None)
        .dt.normalize()
    )
    df_serasa_yandeh["cnpj_raiz"] = df_serasa_yandeh["cnpj_raiz"].astype(str)

    df_serasa_yandeh["score_positivo_pj"] = pd.to_numeric(df_serasa_yandeh["score_positivo_pj"], errors="coerce").astype(float)

    for col in [
        "qtd_pefin_pj", "valor_pefin_pj",
        "qtd_refin_pj", "valor_refin_pj",
        "qtd_divida_vencida_pj", "valor_divida_vencida_pj",
        "qtd_cheque_pj", "valor_cheque_pj",
        "qtd_protesto_pj", "valor_protesto_pj",
        "qtd_falencia_pj", "valor_falencia_pj",
        "qtd_acao_judicial_pj", "valor_acao_judicial_pj",
        "qtd_total_restritivos_pj", "valor_total_restritivos_pj",
        "qtd_pefin_pf", "valor_pefin_pf",
        "qtd_refin_pf", "valor_refin_pf",
        "qtd_divida_vencida_pf", "valor_divida_vencida_pf",
        "qtd_cheque_pf", "valor_cheque_pf",
        "qtd_protesto_pf", "valor_protesto_pf",
        "qtd_falencia_pf", "valor_falencia_pf",
        "qtd_acao_judicial_pf", "valor_acao_judicial_pf",
        "qtd_total_restritivos_pf", "valor_total_restritivos_pf"
    ]:
        df_serasa_yandeh[col] = (
            pd.to_numeric(df_serasa_yandeh[col], errors="coerce")
            .fillna(0)
            .astype(int)
        )
    
    df_serasa_yandeh = df_serasa_yandeh.loc[:, ~df_serasa_yandeh.columns.duplicated()].copy()


    # Colunas de data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_serasa_yandeh['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_serasa_yandeh['year'], df_serasa_yandeh['month'], df_serasa_yandeh['day'] = now.year, now.month, now.day
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
    FOLDER_DESTINATION_TRUSTED = "historico_compras_serasa_yandeh"

    # Escrevendo no Delta Lake com schema fixado
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        df_serasa_yandeh,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )