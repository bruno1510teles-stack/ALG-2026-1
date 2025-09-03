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
import numpy as np
import time

def rating_mais_recente(access_params=None,  **kwargs):

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
    

    # Query Propostas
    query_propostas = f"""
            WITH propostas_aprovadas AS (
                SELECT 
                    DATE(p.data_criado) AS data_criado,
                    DATE(p.data_resolvido) AS data_resolvido,
                    p.issue_key,
                    SUBSTR(LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0'), 1, 8) AS cnpj_raiz,
                    LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0') AS cnpj_sacado,
                    dc.razao_social,
                    p.politica,
                    p.limite_pedido,
                    p.limite_aprovado,
                    p.gerente_tratado,
                    ROW_NUMBER() OVER (
                        PARTITION BY SUBSTR(LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0'), 1, 8)
                        ORDER BY TRY_CAST(p.data_resolvido AS DATE) DESC
                    ) AS rn
                FROM deltalaketrusted.jira.propostas p
                LEFT JOIN deltalakerefined.receita_federal.dados_cadastrais dc
                    ON LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0') = LPAD(REGEXP_REPLACE(dc.cnpj_sem_formatacao, '[^0-9]', ''), 14, '0')
                WHERE p.decisao = 'APROVADO'
            )
            SELECT *
            FROM propostas_aprovadas
            WHERE rn = 1
"""
    propostas = execute_query(conn, query_propostas)

    # Fazendo inner join do cnpj_raiz da tabela de Propostas com a Query 2
    query_serasa_2 = f"""
            WITH 
                propostas_com_cnpjs AS (
                    SELECT DISTINCT 
                        SUBSTR(LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0'), 1, 8) AS cnpj_raiz
                    FROM deltalaketrusted.jira.propostas p
                ),
            
                base_data AS (
                    SELECT 
                        re.id AS id,
                        re.created_date AS data_consulta,
                        rc.json_content,
                        SUBSTRING(last_re.value,1,8) AS cnpj_raiz,
                        re.reports_id
                    FROM 
                        postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.report_content rc ON rc.id = re.content_id
                        INNER JOIN (
                            SELECT 
                                MIN(re.id) AS re_id,
                                rc.json_content,
                                pi2.value,
                                ROW_NUMBER() OVER (PARTITION BY pi2.value ORDER BY json_content DESC) AS rn
                            FROM postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                                INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.report_definition rd 
                                    ON rd.id = re.definition_id AND rd."type" = 'RELATORIO_AVANCADO_PJ_ANALITICO'
                                INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.report_content rc 
                                    ON rc.id = re.content_id
                                INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.report_involvement ri 
                                    ON ri.report_execution_id = re.id
                                INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.party_identification pi2 
                                    ON pi2.party_id = ri.party_id
                            WHERE re.resolution = 'DONE'
                                AND SUBSTRING(pi2.value,1,8) IN (SELECT cnpj_raiz FROM propostas_com_cnpjs)
                            GROUP BY rc.json_content, pi2.value
                        ) last_re ON last_re.re_id = re.id AND rn = 1
                ),
            
                tipo_pendencia(tipo, descricao) AS (
                    VALUES 
                        ('PEFIN', 'PEFIN'),
                        ('REFIN', 'REFIN'),
                        ('COLLECTION_RECORDS', 'DIVIDA VENCIDA'),
                        ('CHECK', 'CHEQUE'),
                        ('NOTARY', 'PROTESTO'),
                        ('BANKRUPTSPATICIPATION', 'FALENCIA'),
                        ('JUDGEMENTFILINGS', 'ACAO JUDICIAL')
                ),
            
                restritivos_pj AS (
                    SELECT DISTINCT
                        bd.id,
                        CAST(bd.data_consulta AS DATE) AS data_consulta,
                        bd.cnpj_raiz,
                        tp.descricao AS grupo_ocorrencia,
                        s.count AS quantidade_ocorrencia,
                        s.first_occurrence AS ano_mes_primeiro,
                        s.last_occurrence AS ano_mes_ultimo,
                        COALESCE(s.balance, 0) AS valor_total
                    FROM tipo_pendencia tp 
                        INNER JOIN base_data bd ON TRUE
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.reports rs ON rs.id = bd.reports_id
                        LEFT JOIN postgres.exrp_{Variable.get('STAGE')}_default.report r ON rs.id = r.reports_id
                        LEFT JOIN postgres.exrp_{Variable.get('STAGE')}_default.negative_data nd ON nd.id = r.negative_data_id
                        LEFT JOIN postgres.exrp_{Variable.get('STAGE')}_default.negative_data_item ndi 
                            ON ndi.id IN (
                                nd.pefin_id, 
                                nd.refin_id, 
                                nd.collection_records_id, 
                                nd.check_id,
                                nd.notary_id
                            ) AND ndi."_object_type" = tp.tipo
                        LEFT JOIN postgres.exrp_{Variable.get('STAGE')}_default.facts f ON f.id = r.facts_id
                        LEFT JOIN postgres.exrp_{Variable.get('STAGE')}_default.bankrupts b ON b.id = f.bankrupts_id AND tp.tipo = 'BANKRUPTSPATICIPATION'
                        LEFT JOIN postgres.exrp_{Variable.get('STAGE')}_default.judgement_filings jf ON jf.id = f.judgement_filings_id AND tp.tipo = 'JUDGEMENTFILINGS'
                        LEFT JOIN postgres.exrp_{Variable.get('STAGE')}_default.summary s ON s.id IN (ndi.summary_id, b.summary_id, jf.summary_id)
                ),
            
                total_restritivos_pj AS (
                    SELECT 
                        t1.id,
                        t1.data_consulta,
                        t1.cnpj_raiz,
                        SUM(valor_total) AS "TOTAL RESTRITIVOS"
                    FROM restritivos_pj t1
                    GROUP BY t1.id, t1.data_consulta, t1.cnpj_raiz
                ),
            
                qtde_cheque_pj AS (
                    SELECT 
                        rpj.id,
                        COALESCE(rpj.quantidade_ocorrencia, 0) AS "QTD CHEQUE"
                    FROM restritivos_pj rpj
                    WHERE grupo_ocorrencia = 'CHEQUE'
                ),
            
                restritivos_socios AS (
                    SELECT 
                        bd.id,
                        p.id AS partner_id,
                        p.kind_person,
                        p.document,
                        p.document_branch,
                        p.document_digit,
                        CASE 
                            WHEN d.debt_type = 'BANKRUPTSPATICIPATION' THEN 'FALENCIA'
                            WHEN d.debt_type = 'CHECKCCF' THEN 'CHEQUE'
                            WHEN d.debt_type = 'COLLECTIONRECORDS' THEN 'DIVIDA VENCIDA'
                            WHEN d.debt_type = 'FINANCIAL' THEN 'REFIN'
                            WHEN d.debt_type = 'JUDGEMENTFILINGS' THEN 'ACAO JUDICIAL'
                            WHEN d.debt_type = 'MARKET' THEN 'PEFIN'
                            WHEN d.debt_type = 'NOTARY' THEN 'PROTESTO'
                        END AS grupo_ocorrencia,
                        s.count AS quantidade_ocorrencia,
                        s.last_occurrence AS ano_mes_ultimo,
                        COALESCE(s.balance, 0) AS valor_total
                    FROM base_data bd
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.reports rs ON rs.id = bd.reports_id
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 ON of2.id = rs.optional_features_id
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.qsa_complete_report qcr ON qcr.id = of2.qsa_complete_report_id
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.person p ON p.qsa_complete_report_id = qcr.id AND p."_object_type" = 'PARTNER'
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.debt d ON d.person_id = p.id
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.summary s ON s.id = d.summary_id
                ),
            
                qtde_cheque_pf AS (
                    SELECT 
                        rs.id,
                        COALESCE(SUM(rs.quantidade_ocorrencia), 0) AS "CHEQUE PF"
                    FROM restritivos_socios rs
                    WHERE grupo_ocorrencia = 'CHEQUE'
                    GROUP BY rs.id
                ),
            
                total_restritivos_socio AS (
                    SELECT 
                        t1.id,
                        SUM(valor_total) AS "RESTRITIVOS PF"
                    FROM restritivos_socios t1
                    GROUP BY t1.id
                ),
            
                score_pj AS (
                    SELECT 
                        bd.id,
                        s.score AS "Score Positivo PJ",
                        CASE 
                            WHEN s.message = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' THEN 1 
                            ELSE 0 
                        END AS grande_empresa
                    FROM base_data bd
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.reports rs ON rs.id = bd.reports_id
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 ON of2.id = rs.optional_features_id
                        INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.score s ON s.id = of2.id
                ),
            
                RankedSocios AS (
                    SELECT 
                        id,
                        documento_socio,
                        percentual_capital,
                        ROW_NUMBER() OVER (PARTITION BY id ORDER BY percentual_capital DESC, documento_socio) AS rn
                    FROM (
                        SELECT 
                            bd.id,
                            CASE 
                                WHEN p.kind_person = 'J' THEN p.document || p.document_branch || p.document_digit
                                ELSE p.document || p.document_digit
                            END AS documento_socio,
                            p.percentage_capital AS percentual_capital
                        FROM base_data bd
                            INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.reports rs ON rs.id = bd.reports_id
                            INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 ON of2.id = rs.optional_features_id
                            INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.qsa_complete_report qcr ON qcr.id = of2.qsa_complete_report_id
                            INNER JOIN postgres.exrp_{Variable.get('STAGE')}_default.person p ON p.qsa_complete_report_id = qcr.id AND p."_object_type" = 'PARTNER'
                    ) t1
                )
            
            SELECT 
                trp.id,
                trp.data_consulta,
                trp.cnpj_raiz,
                score."Score Positivo PJ",
                score.grande_empresa,
                COALESCE(trp."TOTAL RESTRITIVOS", 0) AS "TOTAL RESTRITIVOS",
                COALESCE(cpj."QTD CHEQUE", 0) AS "QTD CHEQUE",
                COALESCE(cpf."CHEQUE PF", 0) AS "CHEQUE PF",
                COALESCE(trs."RESTRITIVOS PF", 0) AS "RESTRITIVOS PF",
                rs.documento_socio AS "CPF do Principal Socio"
            FROM total_restritivos_pj trp
                LEFT JOIN qtde_cheque_pj cpj ON trp.id = cpj.id
                LEFT JOIN qtde_cheque_pf cpf ON trp.id = cpf.id
                LEFT JOIN total_restritivos_socio trs ON trp.id = trs.id
                LEFT JOIN score_pj score ON trp.id = score.id
                LEFT JOIN RankedSocios rs ON trp.id = rs.id AND rs.rn = 1
"""
    serasa2 = execute_query(conn, query_serasa_2)
    
    print(f"Quantidade de CNPJs que retornou da base_retorno_serasa2: {serasa2.shape[0]}")

    # Fazendo inner join do cnpj_raiz da tabela de Propostas com a Query 1
    query_serasa_1 = f"""
            WITH 
                propostas_com_cnpjs AS (
                    SELECT DISTINCT 
                        SUBSTR(LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0'), 1, 8) AS cnpj_raiz
                    FROM deltalaketrusted.jira.propostas p
                ),
            
                total_restritivos_pj AS (
                    SELECT 
                        t1.org_id AS id,
                        t1.data_consulta,
                        t1.cnpj_raiz,
                        SUM(valor_total) AS "TOTAL RESTRITIVOS"
                    FROM (
                        SELECT DISTINCT
                            org.id AS org_id,
                            DATE(SPLIT_PART(org.data_hora_consulta, ' ', 1)) AS data_consulta,
                            org.cnpj_raiz,
                            remp.id,
                            remp.grupo_ocorrencia,
                            remp.quantidade_ocorrencia,
                            remp.ano_mes_primeiro,
                            remp.ano_mes_ultimo,
                            remp.moeda,
                            remp.codigo_natureza,
                            remp.fonte,
                            remp.titular_pendencia,
                            COALESCE(remp.valor_total, 0) AS valor_total
                        FROM deltalaketrusted.serasa.organizacoes org
                        LEFT JOIN deltalaketrusted.serasa.resumo_restritivo remp
                            ON remp.id = org.id
                            AND remp.titular_pendencia = CONCAT('0', org.cnpj_raiz)
                        INNER JOIN propostas_com_cnpjs pc ON org.cnpj_raiz = pc.cnpj_raiz
                    ) t1
                    GROUP BY
                        t1.org_id,
                        t1.data_consulta,
                        t1.cnpj_raiz
                ),
            
                qtde_cheque_pj AS (
                    SELECT 
                        t1.org_id AS id,
                        SUM(t1."QTD CHEQUE") AS "QTD CHEQUE"
                    FROM (
                        SELECT DISTINCT
                            org.id AS org_id,
                            org.cnpj_raiz,
                            remp.id,
                            remp.grupo_ocorrencia,
                            remp.quantidade_ocorrencia,
                            remp.ano_mes_primeiro,
                            remp.ano_mes_ultimo,
                            remp.moeda,
                            remp.codigo_natureza,
                            remp.fonte,
                            remp.titular_pendencia,
                            COALESCE(remp.quantidade_ocorrencia, 0) AS "QTD CHEQUE"
                        FROM deltalaketrusted.serasa.organizacoes org
                        LEFT JOIN deltalaketrusted.serasa.resumo_restritivo remp
                            ON remp.id = org.id
                            AND remp.titular_pendencia = CONCAT('0', org.cnpj_raiz)
                        INNER JOIN propostas_com_cnpjs pc ON org.cnpj_raiz = pc.cnpj_raiz
                        WHERE remp.grupo_ocorrencia = 'CHEQUE'
                    ) t1
                    GROUP BY
                        t1.org_id
                ),
            
                qtde_cheque_pf AS (
                    SELECT 
                        t1.org_id AS id,
                        SUM(t1."CHEQUE PF") AS "CHEQUE PF"
                    FROM (
                        SELECT DISTINCT
                            socios.id AS org_id,
                            rsocio.id,
                            rsocio.grupo_ocorrencia,
                            rsocio.quantidade_ocorrencia,
                            rsocio.ano_mes_primeiro,
                            rsocio.ano_mes_ultimo,
                            rsocio.moeda,
                            rsocio.codigo_natureza,
                            rsocio.fonte,
                            rsocio.titular_pendencia,
                            COALESCE(rsocio.quantidade_ocorrencia, 0) AS "CHEQUE PF"
                        FROM deltalaketrusted.serasa.socios socios
                        LEFT JOIN deltalaketrusted.serasa.resumo_restritivo rsocio
                            ON rsocio.id = socios.id
                            AND rsocio.titular_pendencia = socios.documento_socio
                        INNER JOIN deltalaketrusted.serasa.organizacoes org
                            ON socios.id = org.id
                        INNER JOIN propostas_com_cnpjs pc 
                            ON org.cnpj_raiz = pc.cnpj_raiz
                        WHERE rsocio.grupo_ocorrencia = 'CHEQUE'
                    ) t1
                    GROUP BY t1.org_id
                ),
            
                total_restritivos_socio AS (
                    SELECT 
                        t1.org_id AS id,
                        SUM(valor_total) AS "RESTRITIVOS PF"
                    FROM (
                        SELECT DISTINCT
                            socios.id AS org_id,
                            rsocio.id,
                            rsocio.grupo_ocorrencia,
                            rsocio.quantidade_ocorrencia,
                            rsocio.ano_mes_primeiro,
                            rsocio.ano_mes_ultimo,
                            rsocio.moeda,
                            rsocio.codigo_natureza,
                            rsocio.fonte,
                            rsocio.titular_pendencia,
                            COALESCE(rsocio.valor_total, 0) AS valor_total
                        FROM deltalaketrusted.serasa.socios socios
                        LEFT JOIN deltalaketrusted.serasa.resumo_restritivo rsocio
                            ON rsocio.id = socios.id
                            AND rsocio.titular_pendencia = socios.documento_socio
                        INNER JOIN deltalaketrusted.serasa.organizacoes org
                            ON socios.id = org.id
                        INNER JOIN propostas_com_cnpjs pc
                            ON org.cnpj_raiz = pc.cnpj_raiz
                    ) t1
                    GROUP BY t1.org_id
                ),
            
                score_pj AS (
                    SELECT 
                        sc.id, 
                        sc.valor_score AS "Score Positivo PJ",
                        CASE 
                            WHEN sc.probabilidade_inadimplencia_mensagem = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' 
                            THEN 1 ELSE 0 
                        END AS grande_empresa
                    FROM deltalaketrusted.serasa.score sc
                ),
            
                consulta_mais_recente AS (
                    SELECT
                        org.cnpj_raiz,
                        MAX(DATE(SPLIT_PART(org.data_hora_consulta, ' ', 1))) AS consulta_mais_recente
                    FROM deltalaketrusted.serasa.organizacoes org
                    INNER JOIN propostas_com_cnpjs pc ON org.cnpj_raiz = pc.cnpj_raiz
                    GROUP BY org.cnpj_raiz
                ),
            
                RankedSocios AS (
                    SELECT 
                        org.id, 
                        so.documento_socio, 
                        so.percentual_capital,
                        ROW_NUMBER() OVER (PARTITION BY org.id ORDER BY so.percentual_capital DESC, so.documento_socio) AS rn
                    FROM deltalaketrusted.serasa.organizacoes org
                    LEFT JOIN deltalaketrusted.serasa.socios so ON org.id = so.id
                    INNER JOIN propostas_com_cnpjs pc ON org.cnpj_raiz = pc.cnpj_raiz
                )
            
            SELECT 
                trp.id,
                trp.data_consulta,
                trp.cnpj_raiz,
                score."Score Positivo PJ",
                score.grande_empresa,
                COALESCE(trp."TOTAL RESTRITIVOS", 0) AS "TOTAL RESTRITIVOS",
                COALESCE(cpj."QTD CHEQUE", 0) AS "QTD CHEQUE",
                COALESCE(cpf."CHEQUE PF", 0) AS "CHEQUE PF",
                COALESCE(trs."RESTRITIVOS PF", 0) AS "RESTRITIVOS PF",
                rs.documento_socio AS "CPF do Principal Socio"
            FROM total_restritivos_pj trp
            LEFT JOIN qtde_cheque_pj cpj ON trp.id = cpj.id
            LEFT JOIN qtde_cheque_pf cpf ON trp.id = cpf.id
            LEFT JOIN total_restritivos_socio trs ON trp.id = trs.id
            LEFT JOIN score_pj score ON trp.id = score.id
            LEFT JOIN RankedSocios rs ON trp.id = rs.id
            WHERE rs.rn = 1
"""
    serasa1 = execute_query(conn, query_serasa_1)
    
    print(f"Quantidade de CNPJs que retornou da base_retorno_serasa1: {serasa1.shape[0]}")

    # Concatenando dataframes da Query 1 e Query 2 do Serasa
    base_score_consolidado = pd.concat([serasa1, serasa2])
    
    # Ordenar por data (mais antiga primeiro) e pegar a primeira linha por cnpj_raiz
    base_score_consolidado = base_score_consolidado.sort_values(['cnpj_raiz', 'data_consulta'], ascending=[True, False])
    base_score_consolidado = base_score_consolidado.drop_duplicates(subset=['cnpj_raiz'], keep='first')
    
    # Converte para datetime
    base_score_consolidado['data_consulta'] = pd.to_datetime(base_score_consolidado['data_consulta'])
    
    # Garantir que as datas estejam no formato datetime
    propostas['data_resolvido'] = pd.to_datetime(propostas['data_resolvido'])
    propostas['data_criado'] = pd.to_datetime(propostas['data_criado'])
    
    # Cruzando Propostas com Base Score Consolidada (Query 1 e Query 2)
    propostas_com_score = pd.merge(propostas, base_score_consolidado, on ='cnpj_raiz', how='left')
    
    print(f"Quantidade de linhas df_propostas: {propostas.shape[0]}")
    print(f"Quantidade de linhas após cruzamento: {propostas_com_score.shape[0]}")


    # Verificando cnpj_raiz da base df_propostas_com_score e atribuindo na Query de Pontualidade
    cnpjs = propostas_com_score['cnpj_raiz'].unique()
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query = f"({ids_query})"

    query_pontualidade = (
            f"""
            with pontualidade_alpe as (
                select substring(regexp_replace(cnpj_sacado, '[^0-9]', ''),1,8) as cnpj_raiz, nome_sacado, 
                sum(case when status_titulo = 'NO PRAZO' then valor_face else 0 END) as valor_pago_no_prazo,
                sum(case when status_titulo = 'FORA DO PRAZO' then valor_face else 0 end) as valor_pago_fora_do_prazo,
                sum(case when status_titulo = 'VENCIDO' then valor_face else 0 end) as valor_nao_pago,
                sum(valor_face) as valor_performado,
                (sum(case when status_titulo = 'NO PRAZO' then valor_face else 0 end)) / (sum(valor_face)) * 100 as pontualidade
            from 
                deltalaketrusted.payments.boletos_internos 
            where 
                status_titulo not in ('A VENCER') 
            group by 
                substring(regexp_replace(cnpj_sacado, '[^0-9]', ''),1,8), nome_sacado
            ),
            pontualidade_arcelor as(
            select
                raiz_cnpj as cnpj_raiz,
                pontualidade
            from 
                deltalakerefined.motor.pontualidade  
            ),
            base_pontualidade as (
            select 
            coalesce(palpe.cnpj_raiz, par.cnpj_raiz) as cnpj_raiz, min(coalesce(palpe.pontualidade, par.pontualidade)) as pontualidade
            from pontualidade_alpe palpe
            full join pontualidade_arcelor par on palpe.cnpj_raiz = par.cnpj_raiz
            group by
            coalesce(palpe.cnpj_raiz, par.cnpj_raiz)
            )
            select 
                *
            from 
                base_pontualidade
            where
            cnpj_raiz in {ids_query}
    """
    )

    base_pontualidade = execute_query(conn, query_pontualidade)

    print(f"Quantidade de CNPJs que retornou da base_pontualidade: {base_pontualidade.shape[0]}")

    # Cruzando df_propostas_com_score com a Base de Pontualidade
    base_analisar = pd.merge(propostas_com_score, base_pontualidade, on = ['cnpj_raiz'], how = 'left')
    
    print(f"Quantidade de linhas df_propostas_com_score: {propostas_com_score.shape[0]}")
    print(f"Quantidade de linhas após cruzamento: {base_analisar.shape[0]}")

    base_analisar['valor_total_restritivos'] = base_analisar['TOTAL RESTRITIVOS'] + base_analisar['RESTRITIVOS PF']
    base_analisar['flag_restritivo'] = base_analisar['valor_total_restritivos'].apply(lambda x: 1 if x > 10 else 0)
    base_analisar['grande_empresa'] = base_analisar['grande_empresa'].fillna(0).astype(int)
    base_analisar['Score Positivo PJ'] = base_analisar['Score Positivo PJ'].fillna(0).astype(int)

    base_analisar.rename(
        columns={
            "Score Positivo PJ": "score",
            "grande_empresa": "empresa_grande",
            "TOTAL RESTRITIVOS": "total_restritivos_pj",
            "QTD CHEQUE": "qtd_cheque",
            "CHEQUE PF": "qtd_cheque_pf",
            "RESTRITIVOS PF": "total_restritivos_pf",
            "CPF do Principal Socio": "cpf_socio_principal"
        },
        inplace=True,
    )

    def set_resultado(row, ram1='Sem Informacao', ram2='Sem Informacao', decisao='MESA', parecer=None):
        row['ramificacao'] = ram1
        row['ramificacao_2'] = ram2
        row['decisao'] = decisao
        if parecer is not None:
            row['parecer'] = parecer
        return row


    # Regras sem HP
    def regra_aprovacao_limite_sem_hp(row, ram1, ram2):
        limite = 0
        if limite <= 100000:
            return set_resultado(row, ram1, ram2, 'APROVADO', 'Motor - Aprovado')
        else:
            return set_resultado(row, ram1, ram2, 'MESA', 'Motor - Aprovado, contudo, sem alçada. Direcionar para avaliação da mesa de crédito')
        
    def aplica_regras_sem_hp(row):
        # Checa se alguma das variáveis essenciais está ausente (NaN)
        if pd.isnull(row[['score', 'valor_total_restritivos']]).any():
            return set_resultado(row, 'Sem Informacao', 'Sem Informacao', 'MESA')

        if row['empresa_grande'] == 1:
            return set_resultado(row, 'B6', 'B3', 'MESA')

        if row['flag_restritivo'] == 0:
            if row['score'] > 900:
                return regra_aprovacao_limite_sem_hp(row, 'A6', 'A1')
            elif row['score'] > 700:
                return regra_aprovacao_limite_sem_hp(row, 'A7', 'A2')
            elif row['score'] > 600:
                return regra_aprovacao_limite_sem_hp(row, 'A8', 'A3')
            elif row['score'] > 316:
                return set_resultado(row, 'B4', 'B1', 'MESA')
            else:
                return set_resultado(row, 'C5', 'C1', 'REPROVADO')

        if row['valor_total_restritivos'] < 500000:
            if row['score'] > 316:
                return set_resultado(row, 'B5', 'B2', 'MESA')
            else:
                return set_resultado(row, 'C6', 'C2', 'REPROVADO')
        else:
            return set_resultado(row, 'D4', 'D1', 'REPROVADO')
        

    # Regras com HP
    def regra_aprovacao_limite_com_hp(row, ram1, ram2):
        limite = 0
        if limite <= 100000:
            return set_resultado(row, ram1, ram2, 'APROVADO', 'Motor - Aprovado')
        else:
            return set_resultado(row, ram1, ram2, 'MESA', 'Motor - Aprovado, contudo, sem alçada. Direcionar para avaliação da mesa de crédito')
        
    def aplica_regras_com_hp(row):
        # Checa se alguma das variáveis essenciais está ausente (NaN)
        if pd.isnull(row[['score', 'valor_total_restritivos']]).any():
            return set_resultado(row, 'Sem Informacao', 'Sem Informacao', 'MESA')

        if row['empresa_grande'] == 1:
            return set_resultado(row, 'B3', 'B3', 'MESA')

        if row['pontualidade'] >= 99:
            if row['flag_restritivo'] == 0:
                if row['score'] > 900:
                    return regra_aprovacao_limite_com_hp(row, 'AA', 'AA')
                elif row['score'] > 700:
                    return regra_aprovacao_limite_com_hp(row, 'A1', 'A1')
                elif row['score'] > 500:
                    return regra_aprovacao_limite_com_hp(row, 'A2', 'A2')
                elif row['score'] > 316:
                    return set_resultado(row, 'B1', 'B1', 'MESA')
                else:
                    return set_resultado(row, 'C1', 'C1', 'REPROVADO')
            elif row['valor_total_restritivos'] < 500000:
                if row['score'] > 316:
                    return set_resultado(row, 'B2', 'B2', 'MESA')
                else:
                    return set_resultado(row, 'C2', 'C2', 'REPROVADO')
            else:
                return set_resultado(row, 'D1', 'D1', 'REPROVADO')

        elif row['pontualidade'] >= 40:
            if row['flag_restritivo'] == 0 and row['pontualidade'] > 90:
                if row['score'] > 900:
                    return regra_aprovacao_limite_com_hp(row, 'A3', 'A1')
                elif row['score'] > 700:
                    return regra_aprovacao_limite_com_hp(row, 'A4', 'A2')
                elif row['score'] > 600:
                    return regra_aprovacao_limite_com_hp(row, 'A5', 'A3')
                elif row['score'] > 316:
                    return set_resultado(row, 'B3', 'B1', 'MESA')
                else:
                    return set_resultado(row, 'C3', 'C1', 'REPROVADO')
            elif row['valor_total_restritivos'] < 500000:
                if row['score'] > 316:
                    return set_resultado(row, 'B4', 'B2', 'MESA')
                else:
                    return set_resultado(row, 'C4', 'C2', 'REPROVADO')
            else:
                return set_resultado(row, 'D2', 'D1', 'REPROVADO')
        
        else:
            return set_resultado(row, 'D3', 'D1', 'REPROVADO')


    # Criando DF
    def aplicar_regra_certa(row):
        if pd.isnull(row['pontualidade']):
            return aplica_regras_sem_hp(row)
        else:
            return aplica_regras_com_hp(row)

    df_final_cenario2 = base_analisar.apply(aplicar_regra_certa, axis=1)

    # Tratamento base
    df_final_cenario2['ramificacao_final'] = df_final_cenario2['ramificacao'] + ' | ' + df_final_cenario2['ramificacao_2']

    # Função para ajustar os valores ao formato decimal(8, 2)
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None  # Mantém valores nulos como estão
        valor_str = str(valor).strip().replace(',', '.')  # Normaliza string
        if valor_str == '':
            return None  # Trata string vazia como None
        try:
            return Decimal(valor_str).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        except Exception:
            return None

    if df_final_cenario2 is not None and not df_final_cenario2.empty:
        # Aplicar a função nas colunas desejadas
        df_final_cenario2['limite_pedido'] = df_final_cenario2['limite_pedido'].apply(ajustar_decimal)
        df_final_cenario2['limite_aprovado'] = df_final_cenario2['limite_aprovado'].apply(ajustar_decimal)
        df_final_cenario2['total_restritivos_pj'] = df_final_cenario2['total_restritivos_pj'].apply(ajustar_decimal)
        df_final_cenario2['total_restritivos_pf'] = df_final_cenario2['total_restritivos_pf'].apply(ajustar_decimal)
        df_final_cenario2['valor_total_restritivos'] = df_final_cenario2['valor_total_restritivos'].apply(ajustar_decimal)

        # Converter para float e arredondar para 2 casas decimais
        df_final_cenario2['limite_pedido'] = df_final_cenario2['limite_pedido'].astype(float).round(2)
        df_final_cenario2['limite_aprovado'] = df_final_cenario2['limite_aprovado'].astype(float).round(2)
        df_final_cenario2['total_restritivos_pj'] = df_final_cenario2['total_restritivos_pj'].astype(float).round(2)
        df_final_cenario2['total_restritivos_pf'] = df_final_cenario2['total_restritivos_pf'].astype(float).round(2)
        df_final_cenario2['valor_total_restritivos'] = df_final_cenario2['valor_total_restritivos'].astype(float).round(2)

    # Colunas de texto
    colunas_string = ['razao_social', 'parecer', 'id', 'cpf_socio_principal']

    for coluna in colunas_string:
        df_final_cenario2[coluna] = df_final_cenario2[coluna].fillna('').astype('string')


    # Colunas de data
    # Converte para datetime, depois converte para date (sem hora)
    df_final_cenario2['data_consulta'] = df_final_cenario2['data_consulta'].dt.date
    df_final_cenario2['data_criado'] = df_final_cenario2['data_criado'].dt.date
    df_final_cenario2['data_resolvido'] = df_final_cenario2['data_resolvido'].dt.date


    # Tratamento colunas para inteiro com suporte a nulos
    colunas_int = [
        'pontualidade', 'score', 'empresa_grande',
        'flag_restritivo', 'qtd_cheque', 'qtd_cheque_pf'
    ]

    for col in colunas_int:
        df_final_cenario2[col] = (
            pd.to_numeric(df_final_cenario2[col], errors='coerce')  # converte para numérico com NaNs
            .round(0)                                               # arredonda para zero casas decimais
            .astype('Int64')                                        # converte para inteiro com suporte a nulos
        )

    # Remove colunas
    df_final_cenario2 = df_final_cenario2.drop(
        columns=['decisao', 'politica', 'limite_pedido', 'limite_aprovado', 'gerente_tratado', 'parecer'],errors='ignore')

    # Reordena as colunas
    ordem_colunas = [
        'data_criado', 'data_resolvido', 'issue_key', 'cnpj_raiz', 'cnpj_sacado','razao_social','pontualidade', 
        'ramificacao', 'ramificacao_2', 'ramificacao_final', 'id', 'data_consulta', 'score', 
        'empresa_grande', 'total_restritivos_pj', 'total_restritivos_pf', 'valor_total_restritivos', 
        'flag_restritivo', 'qtd_cheque', 'qtd_cheque_pf', 'cpf_socio_principal'
    ]

    # Aplica a ordem e reseta o índice
    df_final_cenario2 = df_final_cenario2[ordem_colunas].reset_index(drop=True)

    # Colunas de data
    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final_cenario2['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final_cenario2['year'], df_final_cenario2['month'], df_final_cenario2['day'] = now.year, now.month, now.day
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
    FOLDER_DESTINATION_TRUSTED = "rating_mais_recente_serasa"

    # Escrevendo no Delta Lake com schema fixado
    write_deltalake(
    f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
    df_final_cenario2,
    partition_by=["year", "month", "day"],
    storage_options=storage_options,
    mode="overwrite"
    )
