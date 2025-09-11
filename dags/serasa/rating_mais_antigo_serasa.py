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

def rating_mais_antigo(access_params=None,  **kwargs):

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
                        ORDER BY TRY_CAST(p.data_resolvido AS DATE) ASC
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
    propostas = execute_query (conn, query_propostas)

    cnpjs = propostas['cnpj_raiz'].unique()
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query = f"({ids_query})"

    query_serasa_2 = f"""
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
                            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_definition rd on rd.id = re.definition_id and rd."type" = 'RELATORIO_AVANCADO_PJ_ANALITICO'
                            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
                            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_involvement ri on ri.report_execution_id = re.id
                            inner join postgres.exrp_{Variable.get('STAGE')}_default.party_identification pi2 on pi2.party_id = ri.party_id
                        where
                            re.resolution = 'DONE'
                            and substring(pi2.value,1,8) in {ids_query}
                        group by  
                            rc.json_content
                            ,pi2.value
                    )last_re on last_re.re_id = re.id 
                    --and rn=1	
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
                    sum(valor_total) as "TOTAL RESTRITIVOS"
                from restritivos_pj t1
                group by
                    t1.id,
                    t1.data_consulta,
                    t1.cnpj_raiz
            )
            ,qtde_cheque_pj as (
                select 
                    rpj.id
                    ,coalesce(rpj.quantidade_ocorrencia,0) "QTD CHEQUE"
                from restritivos_pj rpj
                where grupo_ocorrencia = 'CHEQUE'
            )
            ,restritivos_socios as (
                select 
                    bd.id
                    ,p.id partner_id
                    ,p.kind_person
                    ,p.document
                    ,p.document_branch
                    ,document_digit
                    ,case 
                        when d.debt_type = 'BANKRUPTSPATICIPATION' then 'FALENCIA'
                        when d.debt_type = 'CHECKCCF' then 'CHEQUE'
                        when d.debt_type = 'COLLECTIONRECORDS' then 'DIVIDA VENCIDA' --Parece retornar apenas a última
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
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.person p on p.qsa_complete_report_id = qcr.id and p."_object_type" = 'PARTNER'
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.debt d on d.person_id = p.id
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.summary s on s.id = d.summary_id
            )
            ,qtde_cheque_pf as (
                select 
                    rs.id
                    ,coalesce(sum(rs.quantidade_ocorrencia),0) "CHEQUE PF"
                from restritivos_socios rs
                where grupo_ocorrencia = 'CHEQUE'
                group by rs.id
            )
            ,total_restritivos_socio as (
                select 
                    t1.id as id,
                    sum(valor_total) as "RESTRITIVOS PF"
                from restritivos_socios t1
                group by
                    t1.id
            )
            ,score_pj as (
                select 
                    bd.id
                    ,s.score "Score Positivo PJ"
                    ,case when s.message  = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' then 1 else 0 end as grande_empresa
                from base_data bd
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = bd.reports_id
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 on of2.id = rs.optional_features_id
                    inner join postgres.exrp_{Variable.get('STAGE')}_default.score s on s.id = of2.id
            )
            ,consulta_mais_recente AS (
                select 
                bd.id
                ,bd.cnpj_raiz
                ,date(bd.data_consulta) as consulta_mais_recente 
                from base_data	bd
            )
            ,RankedSocios AS (
                select
                    id
                    ,documento_socio
                    ,percentual_capital
                    ,ROW_NUMBER() OVER (PARTITION BY id ORDER BY percentual_capital DESC, documento_socio) AS rn
                from (
                    select 
                        bd.id
                        ,case 
                            when p.kind_person = 'J' then p.document || p.document_branch || p.document_digit
                            else p.document || p.document_digit
                        end documento_socio
                        ,p.percentage_capital percentual_capital
                    from base_data bd
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = bd.reports_id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.optional_features of2 on of2.id = rs.optional_features_id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.qsa_complete_report qcr on qcr.id = of2.qsa_complete_report_id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.person p on p.qsa_complete_report_id = qcr.id and p."_object_type" = 'PARTNER'
                )t1
            )
            select 
                trp.id,
                trp.data_consulta,
                trp.cnpj_raiz,
                score."Score Positivo PJ",
                score.grande_empresa,
                coalesce(trp."TOTAL RESTRITIVOS", 0) as "TOTAL RESTRITIVOS",
                coalesce(cpj."QTD CHEQUE", 0) as "QTD CHEQUE",
                coalesce(cpf."CHEQUE PF", 0) as "CHEQUE PF",
                coalesce(trs."RESTRITIVOS PF", 0) as "RESTRITIVOS PF",
                rs.documento_socio as "CPF do Principal Socio"
            from total_restritivos_pj trp
                left join qtde_cheque_pj cpj on trp.id = cpj.id
                left join qtde_cheque_pf cpf on trp.id = cpf.id
                left join total_restritivos_socio trs on trp.id = trs.id
                left join score_pj score on trp.id = score.id
                --inner join consulta_mais_recente cmr on trp.cnpj_raiz = cmr.cnpj_raiz and trp.data_consulta = cmr.consulta_mais_recente
                left join RankedSocios rs ON trp.id = rs.id 
                and rs.rn = 1
                    """
    serasa2 = execute_query(conn, query_serasa_2)
    
    print(f"Quantidade de CNPJs que retornou da base_retorno_serasa2: {serasa2.shape[0]}")

    cnpjs = propostas['cnpj_raiz'].unique()
    ids_query_2 = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query_2 = f"({ids_query_2})"

    query_serasa_1 = f"""
            with 
                    total_restritivos_pj as (
            select 
                t1.org_id as id,
                t1.data_consulta,
                t1.cnpj_raiz,
                sum(valor_total) as "TOTAL RESTRITIVOS"
            from (
                select distinct
                    org.id org_id,
                    date(split_part(org.data_hora_consulta, ' ', 1)) AS data_consulta,
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
                    coalesce(remp.valor_total, 0) as valor_total
                from
                    deltalaketrusted.serasa.organizacoes org
                left join deltalaketrusted.serasa.resumo_restritivo remp
                    on
                    remp.id = org.id
                    and remp.titular_pendencia = CONCAT('0',
                    org.cnpj_raiz)
                where
                    org.cnpj_raiz in {ids_query_2}
            )t1
            group by
                t1.org_id,
                t1.data_consulta,
                t1.cnpj_raiz
                )
                ,
                    qtde_cheque_pj as (
            select 
                t1.org_id as id,
                sum(t1."QTD CHEQUE") as "QTD CHEQUE"
            from (
                select distinct
                    org.id org_id,
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
                    coalesce(remp.quantidade_ocorrencia, 0) as "QTD CHEQUE"
                from
                    deltalaketrusted.serasa.organizacoes org
                left join deltalaketrusted.serasa.resumo_restritivo remp
                    on
                    remp.id = org.id
                    and remp.titular_pendencia = CONCAT('0',
                    org.cnpj_raiz)
                where
                    org.cnpj_raiz in {ids_query_2}
                    and remp.grupo_ocorrencia = 'CHEQUE'
                ) t1
            group by
                t1.org_id
                )
                ,
                    qtde_cheque_pf as (
            select 
                t1.org_id as id,
                sum(t1."CHEQUE PF") as "CHEQUE PF"
            from (
                select distinct
                    socios.id org_id,
                    rsocio.id,
                    rsocio.grupo_ocorrencia,
                    rsocio.quantidade_ocorrencia,
                    rsocio.ano_mes_primeiro,
                    rsocio.ano_mes_ultimo,
                    rsocio.moeda,
                    rsocio.codigo_natureza,
                    rsocio.fonte,
                    rsocio.titular_pendencia,
                    coalesce(rsocio.quantidade_ocorrencia, 0) as "CHEQUE PF"
                from
                    deltalaketrusted.serasa.socios socios
                left join deltalaketrusted.serasa.resumo_restritivo rsocio
                    on
                    rsocio.id = socios.id
                    and rsocio.titular_pendencia = socios.documento_socio
                where
                    rsocio.grupo_ocorrencia = 'CHEQUE') t1
            group by
                t1.org_id
                )
                ,
                    total_restritivos_socio as (
            select 
                t1.org_id as id,
                sum(valor_total) as "RESTRITIVOS PF"
            from (
                select distinct
                    socios.id org_id,
                    rsocio.id,
                    rsocio.grupo_ocorrencia,
                    rsocio.quantidade_ocorrencia,
                    rsocio.ano_mes_primeiro,
                    rsocio.ano_mes_ultimo,
                    rsocio.moeda,
                    rsocio.codigo_natureza,
                    rsocio.fonte,
                    rsocio.titular_pendencia,
                    coalesce(rsocio.valor_total, 0) as valor_total
                from
                    deltalaketrusted.serasa.socios socios
                left join deltalaketrusted.serasa.resumo_restritivo rsocio
                    on
                    rsocio.id = socios.id
                    and rsocio.titular_pendencia = socios.documento_socio
            )t1
            group by
                t1.org_id
                )
                ,
                    score_pj as (
                select 
                    sc.id, sc.valor_score as "Score Positivo PJ",
                    case when sc.probabilidade_inadimplencia_mensagem  = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' then 1 else 0 end as grande_empresa
                from
                    deltalaketrusted.serasa.score sc
                ),
                    consulta_mais_recente AS (
                select
                    org.cnpj_raiz,
                    MAX(date(split_part(org.data_hora_consulta, ' ', 1))) AS consulta_mais_recente
                from 
                    deltalaketrusted.serasa.organizacoes org 
                where 
                    org.cnpj_raiz in {ids_query_2}
                group by
                    org.cnpj_raiz
                ),
                RankedSocios AS (
                    SELECT 
                        org.id, 
                        so.documento_socio, 
                        so.percentual_capital,
                        ROW_NUMBER() OVER (PARTITION BY org.id ORDER BY so.percentual_capital DESC, so.documento_socio) AS rn
                    FROM 
                        deltalaketrusted.serasa.organizacoes org
                    LEFT JOIN 
                        deltalaketrusted.serasa.socios so ON org.id = so.id
                    where 
                        org.cnpj_raiz in {ids_query_2}
                )
                select 
                    trp.id,
                    trp.data_consulta,
                    trp.cnpj_raiz,
                    score."Score Positivo PJ",
                    score.grande_empresa,
                    coalesce(trp."TOTAL RESTRITIVOS", 0) as "TOTAL RESTRITIVOS",
                    coalesce(cpj."QTD CHEQUE", 0) as "QTD CHEQUE",
                    coalesce(cpf."CHEQUE PF", 0) as "CHEQUE PF",
                    coalesce(trs."RESTRITIVOS PF", 0) as "RESTRITIVOS PF",
                    rs.documento_socio as "CPF do Principal Socio"
                from 
                    total_restritivos_pj trp
                left join 
                    qtde_cheque_pj cpj
                    on trp.id = cpj.id
                left join 
                    qtde_cheque_pf cpf
                    on trp.id = cpf.id
                left join 
                    total_restritivos_socio trs 
                    on trp.id = trs.id
                left join 
                    score_pj score
                    on trp.id = score.id
                left join 
                    RankedSocios rs ON trp.id = rs.id
                where 
                rs.rn = 1
    """
    serasa1 = execute_query(conn, query_serasa_1)

    print(f"Quantidade de CNPJs que retornou da base_retorno_serasa1: {serasa1.shape[0]}")

    # 1: Concatenando os DataFrames das Queries do Serasa
    base_score_consolidado = pd.concat([serasa1, serasa2])

    # Converte as colunas de data para o tipo datetime
    base_score_consolidado['data_consulta'] = pd.to_datetime(base_score_consolidado['data_consulta'])
    propostas['data_resolvido'] = pd.to_datetime(propostas['data_resolvido'])
    propostas['data_criado'] = pd.to_datetime(propostas['data_criado'])

    # 2: Cruza as propostas com TODAS as consultas do Serasa
    propostas_com_serasa = pd.merge(propostas, base_score_consolidado, on='cnpj_raiz', how='left')

    print(f"Quantidade de linhas propostas: {propostas.shape[0]}")
    print(f"Quantidade de linhas após cruzamento inicial: {propostas_com_serasa.shape[0]}")

    # 3: Filtra as consultas que ocorreram na data ou antes da aprovação da proposta
    propostas_com_serasa_filtrado = propostas_com_serasa[
        propostas_com_serasa['data_consulta'] <= propostas_com_serasa['data_resolvido']
    ]

    # 4: Para cada cnpj_raiz, pega a linha com a data de consulta mais recente de acordo a data_resolvido
    propostas_com_serasa_filtrado = propostas_com_serasa_filtrado.sort_values(
        ['cnpj_raiz', 'data_consulta'], ascending=[True, False]
    ).drop_duplicates(
        subset=['cnpj_raiz'], keep='first'
    ).reset_index(drop=True)

    # 5: Adiciona de volta as propostas que não tinham nenhuma consulta do Serasa
    propostas_sem_score = propostas_com_serasa[propostas_com_serasa['data_consulta'].isnull()]

    # 6: Concatena os dois DataFrames para ter o resultado final
    propostas_com_serasa = pd.concat([propostas_com_serasa_filtrado, propostas_sem_score])

    print(f"Quantidade final de linhas no DataFrame propostas_com_serasa: {propostas_com_serasa.shape[0]}")


    # Verificando cnpj_raiz da base propostas_com_serasa e atribuindo na Query de Pontualidade
    cnpjs = propostas_com_serasa['cnpj_raiz'].unique()
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

    # Cruzando propostas_com_serasa com a Base de Pontualidade
    base_analisar = pd.merge(propostas_com_serasa, base_pontualidade, on = ['cnpj_raiz'], how = 'left')
    
    print(f"Quantidade de linhas propostas_com_serasa: {propostas_com_serasa.shape[0]}")
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

    df_final_cenario1 = base_analisar.apply(aplicar_regra_certa, axis=1)

    # Tratamento base
    df_final_cenario1['ramificacao_final'] = df_final_cenario1['ramificacao'] + ' | ' + df_final_cenario1['ramificacao_2']

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

    if df_final_cenario1 is not None and not df_final_cenario1.empty:
        # Aplicar a função nas colunas desejadas
        df_final_cenario1['limite_pedido'] = df_final_cenario1['limite_pedido'].apply(ajustar_decimal)
        df_final_cenario1['limite_aprovado'] = df_final_cenario1['limite_aprovado'].apply(ajustar_decimal)
        df_final_cenario1['total_restritivos_pj'] = df_final_cenario1['total_restritivos_pj'].apply(ajustar_decimal)
        df_final_cenario1['total_restritivos_pf'] = df_final_cenario1['total_restritivos_pf'].apply(ajustar_decimal)
        df_final_cenario1['valor_total_restritivos'] = df_final_cenario1['valor_total_restritivos'].apply(ajustar_decimal)

        # Converter para float e arredondar para 2 casas decimais
        df_final_cenario1['limite_pedido'] = df_final_cenario1['limite_pedido'].astype(float).round(2)
        df_final_cenario1['limite_aprovado'] = df_final_cenario1['limite_aprovado'].astype(float).round(2)
        df_final_cenario1['total_restritivos_pj'] = df_final_cenario1['total_restritivos_pj'].astype(float).round(2)
        df_final_cenario1['total_restritivos_pf'] = df_final_cenario1['total_restritivos_pf'].astype(float).round(2)
        df_final_cenario1['valor_total_restritivos'] = df_final_cenario1['valor_total_restritivos'].astype(float).round(2)

    # Colunas de texto
    colunas_string = ['razao_social', 'parecer', 'id', 'cpf_socio_principal']

    for coluna in colunas_string:
        df_final_cenario1[coluna] = df_final_cenario1[coluna].fillna('').astype('string')


    # Colunas de data
    # Converte para datetime, depois converte para date (sem hora)
    df_final_cenario1['data_consulta'] = df_final_cenario1['data_consulta'].dt.date
    df_final_cenario1['data_criado'] = df_final_cenario1['data_criado'].dt.date
    df_final_cenario1['data_resolvido'] = df_final_cenario1['data_resolvido'].dt.date


    # Tratamento colunas para inteiro com suporte a nulos
    colunas_int = [
        'pontualidade', 'score', 'empresa_grande',
        'flag_restritivo', 'qtd_cheque', 'qtd_cheque_pf'
    ]

    for col in colunas_int:
        df_final_cenario1[col] = (
            pd.to_numeric(df_final_cenario1[col], errors='coerce')  # converte para numérico com NaNs
            .round(0)                                               # arredonda para zero casas decimais
            .astype('Int64')                                        # converte para inteiro com suporte a nulos
        )

    # Remove colunas
    df_final_cenario1 = df_final_cenario1.drop(
        columns=['decisao', 'politica', 'limite_pedido', 'limite_aprovado', 'gerente_tratado', 'parecer'],errors='ignore')

    # Reordena as colunas
    ordem_colunas = [
        'data_criado', 'data_resolvido', 'issue_key', 'cnpj_raiz', 'cnpj_sacado','razao_social','pontualidade', 
        'ramificacao', 'ramificacao_2', 'ramificacao_final', 'id', 'data_consulta', 'score', 
        'empresa_grande', 'total_restritivos_pj', 'total_restritivos_pf', 'valor_total_restritivos', 
        'flag_restritivo', 'qtd_cheque', 'qtd_cheque_pf', 'cpf_socio_principal'
    ]

    # Aplica a ordem e reseta o índice
    df_final_cenario1 = df_final_cenario1[ordem_colunas].reset_index(drop=True)

    # Colunas de data
    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final_cenario1['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final_cenario1['year'], df_final_cenario1['month'], df_final_cenario1['day'] = now.year, now.month, now.day
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
    FOLDER_DESTINATION_TRUSTED = "rating_mais_antigo_serasa"

    # Escrevendo no Delta Lake com schema fixado
    write_deltalake(
    f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
    df_final_cenario1,
    partition_by=["year", "month", "day"],
    storage_options=storage_options,
    mode="overwrite"
    )






