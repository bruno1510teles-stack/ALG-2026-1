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


def processa_historico_serasa (access_params=None,  **kwargs):

    ### Conectando com o Trino
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
    

    print('Processando Query 1 do Serasa...')


    ### QUERY 1 DO SERASA

    query_serasa_1 = f"""
                        with base_data as (
                                                        select 
                                                            re.id id
                                                            ,re.created_date data_consulta
                                                            ,rc.json_content
                                                            ,substring(last_re.value,1,8) cnpj_raiz
                                                            ,re.reports_id
                                                        from 
                                                            postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
                                                            inner join (
                                                                select 
                                                                    min(re.id) re_id
                                                                    ,rc.json_content
                                                                    ,pi2.value
                                                                    ,ROW_NUMBER() OVER (PARTITION BY pi2.value ORDER BY json_content desc) AS rn
                                                                from postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                                                                    inner join postgres.exrp_{Variable.get('STAGE')}_default.report_definition rd on rd.id = re.definition_id and rd."type" in ('RELATORIO_AVANCADO_PJ_ANALITICO')
                                                        inner join postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
                                                        inner join postgres.exrp_{Variable.get('STAGE')}_default.report_involvement ri on ri.report_execution_id = re.id
                                                        inner join postgres.exrp_{Variable.get('STAGE')}_default.party_identification pi2 on pi2.party_id = ri.party_id
                                                        where
                                                            re.resolution = 'DONE'
                                                        group by  
                                                            rc.json_content
                                                            ,pi2.value
                                                        ) last_re on last_re.re_id = re.id and rn=1
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
                                                            ,tp.descricao grupo_ocorrencia
                                                            ,s.count quantidade_ocorrencia
                                                            ,s.first_occurrence ano_mes_primeiro
                                                            ,s.last_occurrence ano_mes_ultimo
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
                            )
                            
                            ,total_restritivos_pj as (
                                                        select 
                                                            t1.id as id,
                                                            t1.data_consulta,
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
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = bd.reports_id
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 on of2.id = rs.optional_features_id
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.qsa_complete_report qcr on qcr.id = of2.qsa_complete_report_id
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.person p on p.qsa_complete_report_id = qcr.id
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.debt d on d.person_id = p.id
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.summary s on s.id = d.summary_id
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
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = bd.reports_id
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 on of2.id = rs.optional_features_id
                                                            inner join postgres.exrp_{Variable.get('STAGE')}_default.score s on s.id = of2.score_id
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

    df_serasa_1 = execute_query(conn, query_serasa_1)

    print(f"Quantidade de linhas no DataFrame 'df_serasa_1': {df_serasa_1.shape[0]}")



    print('Processando Query 2 do Serasa...')

    ### QUERY 2 DO SERASA

    query_serasa_2 = f"""
                            with total_restritivos_pj as (
                                        select
                                                t1.org_id as id,
                                                t1.data_consulta,
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
                                                
                                        from ( select 	distinct org.id org_id,
                                                        date(split_part(org.data_hora_consulta, ' ', 1)) AS data_consulta,
                                                        org.cnpj_raiz, remp.id, remp.grupo_ocorrencia, remp.quantidade_ocorrencia,
                                                        remp.ano_mes_primeiro, remp.ano_mes_ultimo, remp.moeda, remp.codigo_natureza,
                                                        remp.fonte, remp.titular_pendencia, coalesce(remp.valor_total, 0) as valor_total
                                                from deltalaketrusted.serasa.organizacoes org
                                                left join deltalaketrusted.serasa.resumo_restritivo remp
                                                on remp.id = org.id and remp.titular_pendencia = CONCAT('0', org.cnpj_raiz)) as t1
                                        group by 1, 2, 3
        )
            
                                ,total_restritivos_pf as (
                                                                select 
                                                                    t1.id as id,
                                                                    sum(case when t1.grupo_ocorrencia = 'PEFIN' then quantidade_ocorrencia else 0 end) as qtd_pefin_pf,
                                                                    sum(case when t1.grupo_ocorrencia = 'PEFIN' then valor_total else 0 end) as valor_pefin_pf,
                                                                    
                                                                    sum(case when t1.grupo_ocorrencia = 'REFIN' then quantidade_ocorrencia else 0 end) as qtd_refin_pf,
                                                                    sum(case when t1.grupo_ocorrencia = 'REFIN' then valor_total else 0 end) as valor_refin_pf,
                                                                    
                                                                    sum(case when t1.grupo_ocorrencia = 'DIVIDA VENCIDA' then quantidade_ocorrencia else 0 end) as qtd_divida_vencida_pf,
                                                                    sum(case when t1.grupo_ocorrencia = 'DIVIDA VENCIDA' then valor_total else 0 end) as valor_divida_vencida_pf,
                                                                    
                                                                    sum(case when t1.grupo_ocorrencia = 'CHEQUE' then quantidade_ocorrencia else 0 end) as qtd_cheque_pf,
                                                                    sum(case when t1.grupo_ocorrencia = 'CHEQUE' then valor_total else 0 end) as valor_cheque_pf,
                                                                    
                                                                    sum(case when t1.grupo_ocorrencia = 'PROTESTO' then quantidade_ocorrencia else 0 end) as qtd_protesto_pf,
                                                                    sum(case when t1.grupo_ocorrencia = 'PROTESTO' then valor_total else 0 end) as valor_protesto_pf,
                                                                    
                                                                    sum(case when t1.grupo_ocorrencia = 'FALENCIA' then quantidade_ocorrencia else 0 end) as qtd_falencia_pf,
                                                                    sum(case when t1.grupo_ocorrencia = 'FALENCIA' then valor_total else 0 end) as valor_falencia_pf,
                                                                    
                                                                    sum(case when t1.grupo_ocorrencia = 'ACAO JUDICIAL' then quantidade_ocorrencia else 0 end) as qtd_acao_judicial_pf,
                                                                    sum(case when t1.grupo_ocorrencia = 'ACAO JUDICIAL' then valor_total else 0 end) as valor_acao_judicial_pf,
                                                                    
                                                                    sum(quantidade_ocorrencia) as qtd_total_restritivos_pf,
                                                                    sum(valor_total) as valor_total_restritivos_pf
                                                                    
                                                                from ( 	select distinct socios.id org_id, rsocio.id, rsocio.grupo_ocorrencia,
                                                                            rsocio.quantidade_ocorrencia, rsocio.ano_mes_primeiro, rsocio.ano_mes_ultimo,
                                                                            rsocio.moeda, rsocio.codigo_natureza, rsocio.fonte, rsocio.titular_pendencia,
                                                                            coalesce(rsocio.valor_total, 0) as valor_total
                                                                        from deltalaketrusted.serasa.socios socios
                                                                        left join deltalaketrusted.serasa.resumo_restritivo rsocio
                                                                            on rsocio.id = socios.id and rsocio.titular_pendencia = socios.documento_socio ) as t1
                                                                group by 1
                                    )
                                    
                                    ,score_pj as (
                                            
                                                                    select sc.id, sc.valor_score as score_positivo_pj,
                                                                        case when sc.probabilidade_inadimplencia_mensagem  = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' then 1 else 0 end as grande_empresa
                                                                        from deltalaketrusted.serasa.score sc
                                                
                                    )      
                                        select 
                                            trp.*,
                                            trpf.*,
                                            score.score_positivo_pj,
                                            score.grande_empresa
                                        from total_restritivos_pj trp
                                        left join total_restritivos_pf trpf 
                                            on trp.id = trpf.id
                                        left join score_pj score
                                            on trp.id = score.id
                            """

    df_serasa_2 = execute_query(conn, query_serasa_2)

    print(f"Quantidade de linhas no DataFrame 'df_serasa_2': {df_serasa_2.shape[0]}")


    ### Concatenando Resultados
    df_serasa = pd.concat([df_serasa_1, df_serasa_2], ignore_index=True)


    df_serasa.head()


    # Normalizando Tipos de Dados
    df_serasa["id"] = df_serasa["id"].astype(str)
    df_serasa["data_consulta"] = (
        pd.to_datetime(df_serasa["data_consulta"], errors="coerce")
        .dt.tz_localize(None)
        .dt.normalize()
    )
    df_serasa["cnpj_raiz"] = df_serasa["cnpj_raiz"].astype(str)

    df_serasa["score_positivo_pj"] = pd.to_numeric(df_serasa["score_positivo_pj"], errors="coerce").astype(float)

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
        df_serasa[col] = (
            pd.to_numeric(df_serasa[col], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    df_serasa = df_serasa.loc[:, ~df_serasa.columns.duplicated()]

    # Colunas de data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_serasa['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_serasa['year'], df_serasa['month'], df_serasa['day'] = now.year, now.month, now.day
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
        df_serasa,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )