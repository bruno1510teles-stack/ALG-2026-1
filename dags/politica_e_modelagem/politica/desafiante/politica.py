# Carregando libs
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import os, pytz
import time
import base64
import requests
import boto3
from airflow.models import Variable


def executa_politica (access_params=None,  **kwargs):

    ### Configurando configurações necessárias
    # Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    ### Funções SQL
    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    

    print('Carregando base da última task...')


    # Pegando DF tarefa anterior
    ti = kwargs['ti']
    base_final_antifraude_serasa = ti.xcom_pull(task_ids='antifraude_serasa_task')
    base_antifraude_serasa = pd.DataFrame(base_final_antifraude_serasa)

    base_antifraude_serasa['cnpj_raiz'] = base_antifraude_serasa['cnpj_sem_formatacao'].astype(str).str.zfill(14).str[:8]
    df_politica_segue = base_antifraude_serasa.loc[base_antifraude_serasa['resposta_antifraude_serasa'] == "SEGUE"]


    print('Formatando cnpj antes da query...')


    cnpjs = df_politica_segue['cnpj_raiz'].unique()
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query = f"({ids_query})"
    print(ids_query)

    # Caso não retorne nenhum CNPJ como segue ele corrige para não dar erro na query. A partir disso a query retornará com as colunas mas com nenhum registro e o fluxo seguirá normalmente
    if ids_query == "()":
        ids_query = "('')"

    '''
    query_serasa = (f"""
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
                org.cnpj_raiz in {ids_query}
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
                org.cnpj_raiz in {ids_query}
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
                org.cnpj_raiz in {ids_query}
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
                    org.cnpj_raiz in {ids_query}
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
            inner join 
                consulta_mais_recente cmr
                on trp.cnpj_raiz = cmr.cnpj_raiz and trp.data_consulta = cmr.consulta_mais_recente
            left join 
                RankedSocios rs ON trp.id = rs.id
            where 
                rs.rn = 1
        """)'
    '''

    query_serasa = (
           f"""
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
                    )last_re on last_re.re_id = re.id and rn=1	
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
                inner join consulta_mais_recente cmr on trp.cnpj_raiz = cmr.cnpj_raiz and trp.data_consulta = cmr.consulta_mais_recente
                left join RankedSocios rs ON trp.id = rs.id
            where 
                rs.rn = 1
                    """
    )


    base_retorno_serasa = execute_query(conn, query_serasa)

    print(f"Quantidade de CNPJs que retornou da base_retorno_serasa: {base_retorno_serasa.shape[0]}")


    query_pontualidade = (f"""
                            select
                                raiz_cnpj as cnpj_raiz,
                                pontualidade
                            from 
                                deltalakerefined.motor.pontualidade  
                            where 
                                raiz_cnpj in {ids_query}
                        """)

    base_pontualidade = execute_query(conn, query_pontualidade)

    print(f"Quantidade de CNPJs que retornou da base_pontualidade: {base_pontualidade.shape[0]}")


    print('Criando Flags e Campos necessarios para Politica Desafiante...')


    base_analisar = pd.merge(base_retorno_serasa, base_pontualidade, on = ['cnpj_raiz'], how = 'left')

    base_analisar['valor_total_restritivos'] = base_analisar['TOTAL RESTRITIVOS'] + base_analisar['RESTRITIVOS PF']
    base_analisar['flag_restritivo'] = base_analisar['valor_total_restritivos'].apply(lambda x: 1 if x > 10 else 0)
    base_analisar['grande_empresa'] = base_analisar['grande_empresa'].fillna(0).astype(int)
    base_analisar['Score Positivo PJ'] = base_analisar['Score Positivo PJ'].fillna(0).astype(int)

    base_analisar.rename(
        columns={
            "Score Positivo PJ": "score_positivo_pj",
            "grande_empresa": "empresa_grande",
            "TOTAL RESTRITIVOS": "total_restritivos",
            "QTD CHEQUE": "qtd_cheque",
            "CHEQUE PF": "qtd_cheque_pf",
            "RESTRITIVOS PF": "total_restritivos_pf",
            "CPF do Principal Socio": "cpf_socio_principal"
        },
        inplace=True,
    )


    print('Dividindo base COM HP e SEM HP...')


    sem_hp = base_analisar[base_analisar['pontualidade'].isna()].reset_index(drop=True)
    com_hp = base_analisar[base_analisar['pontualidade'].notna()].reset_index(drop=True)


    print('Aplicando Politica...')


    # Tratando casos sem informações de pagamento

    # Função para calcular a ramificação e a decisão final
    def aplica_regras_sem_hp(row):
        # Caso o porte seja 'CORPORATE'
        if row['empresa_grande'] == 1:
            row['ramificacao'] = 'B6'
            row['ramificacao_2'] = 'B3'
            row['decisao_final'] = 'MESA'
        else:
            # Verificar se há restritivo
            if row['flag_restritivo'] == 0:
                # Se não há restritivo, aplicar a lógica de score
                if row['score_positivo_pj'] > 900:
                    row['ramificacao'] = 'A6'
                    row['ramificacao_2'] = 'A1'
                    row['decisao_final'] = 'APROVADO'
                elif row['score_positivo_pj'] > 700:
                    row['ramificacao'] = 'A7'
                    row['ramificacao_2'] = 'A2'
                    row['decisao_final'] = 'APROVADO'
                elif row['score_positivo_pj'] > 600:
                    row['ramificacao'] = 'A8'
                    row['ramificacao_2'] = 'A3'
                    row['decisao_final'] = 'APROVADO'
                elif row['score_positivo_pj'] > 316:
                    row['ramificacao'] = 'B4'
                    row['ramificacao_2'] = 'B1'
                    row['decisao_final'] = 'MESA'
                else:
                    row['ramificacao'] = 'C5'
                    row['ramificacao_2'] = 'C1'
                    row['decisao_final'] = 'REPROVADO'
            else:
                # Caso tenha restritivo
                if row['valor_total_restritivos'] < 500000:
                    # Se o restritivo for menor que 500k
                    if row['score_positivo_pj'] > 316:
                        row['ramificacao'] = 'B5'
                        row['ramificacao_2'] = 'B2'
                        row['decisao_final'] = 'MESA'
                    else:
                        row['ramificacao'] = 'C6'
                        row['ramificacao_2'] = 'C2'
                        row['decisao_final'] = 'REPROVADO'
                else:
                    # Se o restritivo for maior que 500k
                    row['ramificacao'] = 'D4'
                    row['ramificacao_2'] = 'D1'
                    row['decisao_final'] = 'REPROVADO'
        return row

    # Aplicar a função em cada linha do DataFrame 'sem_hp'
    sem_hp = sem_hp.apply(aplica_regras_sem_hp, axis=1)
    

    # Tratando casos com informações de pagamento

    # Função para calcular a ramificação e a decisão final
    def aplica_regras_com_hp(row):
        # Caso o porte seja 'CORPORATE'
        if row['empresa_grande'] == 1:
            row['ramificacao'] = 'B3'
            row['ramificacao_2'] = 'B3'
            row['decisao_final'] = 'MESA'     
        else:
            if row['pontualidade'] >= 99:
                if row['flag_restritivo'] == 0:
                    if row['score_positivo_pj'] > 900:
                        row['ramificacao'] = 'AA'
                        row['ramificacao_2'] = 'AA'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 700:
                        row['ramificacao'] = 'A1'
                        row['ramificacao_2'] = 'A1'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 500:
                        row['ramificacao'] = 'A2'
                        row['ramificacao_2'] = 'A2'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 316:
                        row['ramificacao'] = 'B1'
                        row['ramificacao_2'] = 'B1'
                        row['decisao_final'] = 'MESA'
                    else:
                        row['ramificacao'] = 'C1'
                        row['ramificacao_2'] = 'C1'
                        row['decisao_final'] = 'REPROVADO'
                else:
                    if row['valor_total_restritivos'] < 500000:
                        if row['score_positivo_pj'] > 316:
                            row['ramificacao'] = 'B2'
                            row['ramificacao_2'] = 'B2'
                            row['decisao_final'] = 'MESA'
                        else:
                            row['ramificacao'] = 'C2'
                            row['ramificacao_2'] = 'C2'
                            row['decisao_final'] = 'REPROVADO'
                    else:
                        row['ramificacao'] = 'D1'
                        row['ramificacao_2'] = 'D1'
                        row['decisao_final'] = 'REPROVADO'                    
            elif row['pontualidade'] >= 40:
                if row['flag_restritivo'] == 0 and row['pontualidade'] > 90:
                    if row['score_positivo_pj'] > 900:
                        row['ramificacao'] = 'A3'
                        row['ramificacao_2'] = 'A1'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 700:
                        row['ramificacao'] = 'A4'
                        row['ramificacao_2'] = 'A2'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 600:
                        row['ramificacao'] = 'A5'
                        row['ramificacao_2'] = 'A3'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 316:
                        row['ramificacao'] = 'B3'
                        row['ramificacao_2'] = 'B1'
                        row['decisao_final'] = 'MESA'
                    else:
                        row['ramificacao'] = 'C3'
                        row['ramificacao_2'] = 'C1'
                        row['decisao_final'] = 'REPROVADO'
                else:
                    if row['valor_total_restritivos'] < 500000:
                        if row['score_positivo_pj'] > 316:
                            row['ramificacao'] = 'B4'
                            row['ramificacao_2'] = 'B2'
                            row['decisao_final'] = 'MESA'
                        else:
                            row['ramificacao'] = 'C4'
                            row['ramificacao_2'] = 'C2'
                            row['decisao_final'] = 'REPROVADO'
                    else:
                        row['ramificacao'] = 'D2'
                        row['ramificacao_2'] = 'D1'
                        row['decisao_final'] = 'REPROVADO'
            else: 
                row['ramificacao'] = 'D3'
                row['ramificacao_2'] = 'D1'
                row['decisao_final'] = 'REPROVADO'
        return row

    # Aplicar a função em cada linha do DataFrame 'sem_hp'
    com_hp = com_hp.apply(aplica_regras_com_hp, axis=1)

    print('Tratamentos finais...')

    resultado_politica = pd.concat([com_hp, sem_hp], ignore_index=True)


    print(resultado_politica)


    print('Tratamentos Finais - Parte 1')

    resultado_politica = resultado_politica.drop('id', axis=1)

    print('Tratamentos Finais - Parte 2')
    resultado_politica = resultado_politica.drop_duplicates()

    if resultado_politica.empty:
        print("Nenhum dado encontrado nas tabelas 'com_hp' e 'sem_hp'. O DataFrame está vazio.")

        colunas = ['cnpj_raiz', 'ramificacao_final', 'decisao_final']
        resultado_politica = pd.DataFrame(columns=colunas)

    else:
        print('Tratamentos Finais - Parte 3')
        resultado_politica['ramificacao_final'] = resultado_politica['ramificacao'] + ' | ' + resultado_politica['ramificacao_2']

        print('Tratamentos Finais - Parte 4')
        resultado_politica = resultado_politica[['cnpj_raiz', 'ramificacao_final', 'decisao_final']]


    
    pd.set_option('display.max_rows', None)  # Mostra todas as linhas
    pd.set_option('display.max_columns', None)  # Mostra todas as colunas
    pd.set_option('display.width', None)  # Ajusta a largura para que o DataFrame não quebre em várias linhas
    pd.set_option('display.max_colwidth', None)  # Permite exibir o conteúdo completo de cada coluna


    # Merge da base inicial com campos filtrados com a base de decisao da politica
    print("base anti-fraude-serasa")
    print(base_antifraude_serasa)
    df_resultado = pd.merge(base_antifraude_serasa, resultado_politica, on='cnpj_raiz', how='left')

    print(df_resultado)


    df_resultado['documento_sem_formatacao'] = df_resultado['cnpj_sem_formatacao'].apply(lambda x: str(x).zfill(14))


    print('Tratando ramificação final e decisão final')

    ### Tratando ramificação final e decisão final

    # CASOS SEM INFORMAÇÃO NA POLITICA DESAFIANTE

    # RAMIFICAÇÃO FINAL
    df_resultado['ramificacao_final'] = np.where(
        (df_resultado['ramificacao_final'].isnull()) & (df_resultado['ramificacao_antifraude_serasa'] == 'AF SERASA SEGUE'),
        'B - 12',
        df_resultado['ramificacao_final']
    )

    # DECISÃO FINAL
    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (df_resultado['ramificacao_antifraude_serasa'] == 'AF SERASA SEGUE'),
        'MESA',
        df_resultado['decisao_final']
    )

 
    ### -> CASOS DE REPROVA

    ## -> ANTIFRAUDE RECEITA

    '''
    # Regra caso o CNPJ tenha caido no modelo de antifraude - RAMIFICAÇÃO FINAL
    df_resultado['ramificacao_final'] = np.where(
        (df_resultado['ramificacao_final'].isnull()) & (df_resultado['ramificacao_antifraude'] != 'AF SEGUE'),
        df_resultado['ramificacao_antifraude'],
        df_resultado['ramificacao_final']
    )

    # Regra caso o CNPJ tenha caido no modelo de antifraude - DECISÃO FINAL
    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (df_resultado['ramificacao_antifraude'] != 'AF SEGUE'),
        'REPROVADO',
        df_resultado['decisao_final']
    )'
    '''


    # Lista de valores a serem ignorados
    valores_ignorados_af = ['AF SEGUE', 'AF - MUDANÇA CIDADE', 'AF - MUDANÇA ESTADO', 'PF FUNDACAO < 2 ANOS']

    # RAMIFICAÇÃO FINAL
    df_resultado['ramificacao_final'] = np.where(
        (df_resultado['ramificacao_final'].isnull()) & (~df_resultado['ramificacao_antifraude'].isin(valores_ignorados_af)),
        df_resultado['ramificacao_antifraude'],
        df_resultado['ramificacao_final']
    )

    # DECISÃO FINAL
    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (~df_resultado['ramificacao_antifraude'].isin(valores_ignorados_af)),
        'MESA',
        df_resultado['decisao_final']
    )


    ## -> PRE FILTRO


    if 'ramificacao_pre_filtro' not in df_resultado.columns:
        # Casos que morreram na antifraude da receita que não passaram pela pre_filtro
        df_resultado['ramificacao_pre_filtro'] = np.nan
        df_resultado['limite_solicitado'] = 0


    # Regra caso a resposta do pré filtro seja diferente de "SEGUE" (morreu no pré filtro) - RAMIFICAÇÃO FINAL
    df_resultado['ramificacao_final'] = np.where(
        (df_resultado['ramificacao_final'].isnull()) & (df_resultado['ramificacao_pre_filtro'] != 'PF SEGUE'),
        df_resultado['ramificacao_pre_filtro'],
        df_resultado['ramificacao_final']
    )

    # Regra caso a resposta do pré filtro seja diferente de "SEGUE" (morreu no pré filtro) - DECISÃO FINAL
    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (df_resultado['ramificacao_pre_filtro'] != 'PF SEGUE'),
        df_resultado['resposta'],
        df_resultado['decisao_final']
    )

    ## -> ANTIFRAUDE SERASA

    '''
    # RAMIFICAÇÃO FINAL
    df_resultado['ramificacao_final'] = np.where(
        (df_resultado['ramificacao_final'].isnull()) & (df_resultado['ramificacao_antifraude_serasa'] != 'AF SERASA SEGUE'),
        df_resultado['ramificacao_antifraude_serasa'],
        df_resultado['ramificacao_final']
    )

    # DECISÃO FINAL
    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (df_resultado['ramificacao_antifraude_serasa'] != 'AF SERASA SEGUE'),
        'REPROVADO',
        df_resultado['decisao_final']
    )'
    '''


    # RAMIFICAÇÃO FINAL
    df_resultado['ramificacao_final'] = np.where(
        (df_resultado['ramificacao_final'].isnull()) & (df_resultado['ramificacao_antifraude_serasa'] != 'AF SERASA SEGUE'),
        df_resultado['ramificacao_antifraude_serasa'],
        df_resultado['ramificacao_final']
    )

    # DECISÃO FINAL
    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (df_resultado['ramificacao_antifraude_serasa'] != 'AF SERASA SEGUE'),
        'MESA',
        df_resultado['decisao_final']
    )

    # DECISÃO FINAL (CASO LIMINAR SERASA)
    df_resultado['decisao_final'] = np.where(
        (df_resultado['ramificacao_antifraude_serasa'] == 'AF - LIMINAR SERASA'),
        'REPROVADO',
        df_resultado['decisao_final']
    )


    ### -> CASOS DA MESA


    # Casos que não retornaram informações do modelo Antifraude Serasa - RAMIFICAÇÃO FINAL
    df_resultado['ramificacao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (df_resultado['ramificacao_antifraude_serasa'] == 'AF SERASA SEM INFO'),
        'B - 12',
        df_resultado['ramificacao_final']
    )

    # Casos que não retornaram informações do modelo Antifraude Serasa - DECISÃO FINAL
    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (df_resultado['ramificacao_antifraude_serasa'] == 'AF SERASA SEM INFO'),
        'MESA',
        df_resultado['decisao_final']
    )
    


    # Regra para derivar para mesa casos onde o limite solicitado seja maior que 300k
    df_resultado['decisao_final'] = np.where(
        (df_resultado['limite_solicitado'] > 300000),
        'MESA',
        df_resultado['decisao_final']
    )


    print('Tratando parecer personalizado')

    df_resultado.loc[df_resultado['decisao_final'] == 'REPROVADO', 'parecer'] = 'Motor - Recusado, dados analisados fora da politica atual'
    df_resultado.loc[df_resultado['decisao_final'] == 'MESA', 'parecer'] = 'Motor - Direcionar para avaliação da mesa de crédito'
    df_resultado.loc[df_resultado['decisao_final'] == 'APROVADO', 'parecer'] = 'Motor - Aprovado'
    df_resultado.loc[df_resultado['decisao_final'] == 'mantido', 'parecer'] = 'Motor - Limite mantido'

    print('DF com parecer:')
    print(df_resultado)


    # Criação do mapeamento de pareceres
    parecer_map = {
        "PF 1":"Motor - Recusado, impedido de operar",
        "PF 2":"Motor - Recusado, impedido de operar",
        "PF 3":"Motor - Recusado, MEI",
        "PF MEI":"Motor - Recusado, MEI",
        "PF 4":"Motor - Recusado, CNAE",
        "PF 5":"Motor - Recusado, CNAE",
        "PF CNAE":"Motor - Recusado, CNAE",
        "PF 7":"Motor - Recusado, impedido de operar",
        "PF 9":"Motor - Recusado, impedido de operar",
        "PF 10":"Motor - Recusado, impedido de operar",
        "PF CNPJ IRREGULAR":"Motor - Recusado, impedido de operar",
        "A - A11":"Motor - Recusado, apontamento/score",
        "A - A9":"Motor - Recusado, apontamento/score",
        "A - A8":"Motor - Recusado, apontamento/score",
        "A - A6":"Motor - Recusado, apontamento/score",
        "A - B7":"Motor - Recusado, apontamento/score",
        "A - B5":"Motor - Recusado, apontamento/score",
        "A - B4":"Motor - Recusado, apontamento/score",
        "A - E1":"Motor - Recusado, PD",
        "A - C7":"Motor - Recusado, apontamento/score",
        "A - C5":"Motor - Recusado, apontamento/score",
        "A - C4":"Motor - Recusado, apontamento/score",
        "A - C2":"Motor - Recusado, apontamento/score",
        "B - 11":"Motor - Recusado, apontamento/score",
        "B - 9":"Motor - Recusado, apontamento/score",
        "B - 8":"Motor - Recusado, apontamento/score",
        "B - 6":"Motor - Recusado, apontamento/score",
        "C1 | C1":"Motor - Recusado, apontamento/score",
        "C2 | C2": "Motor - Recusado, apontamento/score",
        "D1 | D1": "Motor - Recusado, apontamento/score",
        "C3 | C1": "Motor - Recusado, apontamento/score",
        "C4 | C2": "Motor - Recusado, apontamento/score",
        "D2 | D1": "Motor - Recusado, apontamento/score",
        "D3 | D1": "Motor - Recusado, apontamento/score",
        "C5 | C1": "Motor - Recusado, apontamento/score",
        "C6 | C2": "Motor - Recusado, apontamento/score",
        "D4 | D1": "Motor - Recusado, apontamento/score",
        "AF - LIMINAR SERASA": 'Motor - Recusado, risco de fraude',
        #"AF - MUDANÇA ENDEREÇO": "Motor - Recusado, risco de fraude",
        "AF - MUDANÇA CIDADE": "Motor - Recusado, risco de fraude",
        "AF - MUDANÇA ESTADO": "Motor - Recusado, risco de fraude",
        #"AF - ENDEREÇO IGUAL": "Motor - Recusado, risco de fraude",
        #"AF - CONSULTAS SERASA": "Motor - Recusado, risco de fraude"
        "AF - MUDANÇA ENDEREÇO": 'Motor - Direcionar para avaliação da mesa de crédito',
        "AF - ENDEREÇO IGUAL": 'Motor - Direcionar para avaliação da mesa de crédito',
        "AF - CONSULTAS SERASA": 'Motor - Direcionar para avaliação da mesa de crédito'
    }


    # Função que retorna o parecer personalizado ou o parecer original se não houver mapeamento
    def parecer_personalizado(row):
        decisao = row['ramificacao_final']
        parecer_atual = row['parecer']
        return parecer_map.get(decisao, parecer_atual)

    # Aplica a função ao DataFrame
    df_resultado['parecer'] = df_resultado.apply(parecer_personalizado, axis=1)


    print('Segunda validação do parecer')
    print(df_resultado)

    '''
    Comentado dia 11/04 - implementação de nova regra, logo abaixo.
    # Criando Regra para parâmetro de aprovação

    df_resultado['parecer'] = np.where(
        (df_resultado['decisao_final'] == 'APROVADO') & (df_resultado['limite_solicitado'] > 50000),
        'Motor - Aprovado, contudo, sem alçada. Direcionar para avaliação da mesa de crédito',
        df_resultado['parecer']
    )


    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'] == 'APROVADO') & (df_resultado['limite_solicitado'] <= 50000),
        'APROVADO',
        np.where(
            (df_resultado['decisao_final'] == 'APROVADO') & (df_resultado['limite_solicitado'] > 50000),
            'MESA',
            df_resultado['decisao_final']
        )
    )'
    '''


    # REGRA COM HP -> APROVANDO APENAS CLIENTES COM LIM SOLICITADO ATÉ R$ 150.000

    df_resultado['parecer'] = np.where(
        (df_resultado['decisao_final'] == 'APROVADO') &
        (df_resultado['limite_solicitado'] > 100000) &
        (
            (df_resultado['ramificacao_final'] == 'AA | AA') | 
            (df_resultado['ramificacao_final'] == 'A1 | A1') | 
            (df_resultado['ramificacao_final'] == 'A2 | A2') |
            (df_resultado['ramificacao_final'] == 'A3 | A1') |
            (df_resultado['ramificacao_final'] == 'A4 | A2') |
            (df_resultado['ramificacao_final'] == 'A5 | A3') 
        ),
        'Motor - Aprovado, contudo, sem alçada. Direcionar para avaliação da mesa de crédito',
        df_resultado['parecer']
    )

    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'] == 'APROVADO') &
        (df_resultado['limite_solicitado'] > 100000) &
        (
            (df_resultado['ramificacao_final'] == 'AA | AA') | 
            (df_resultado['ramificacao_final'] == 'A1 | A1') | 
            (df_resultado['ramificacao_final'] == 'A2 | A2') |
            (df_resultado['ramificacao_final'] == 'A3 | A1') |
            (df_resultado['ramificacao_final'] == 'A4 | A2') |
            (df_resultado['ramificacao_final'] == 'A5 | A3') 
        ),
        "MESA",
        df_resultado['decisao_final']
    )

        
    # Parecer para casos que foram aprovados pela politica, mas direcionados para a mesa, pois não tem historico de hp.

    # REGRA SEM HP -> APROVANDO APENAS CLIENTES COM LIM SOLICITADO ATÉ R$ 100.000

    df_resultado['parecer'] = np.where(
        (df_resultado['decisao_final'] == 'APROVADO') &
        (df_resultado['limite_solicitado'] > 100000) &
        (
            (df_resultado['ramificacao_final'] == 'A6 | A1') | 
            (df_resultado['ramificacao_final'] == 'A7 | A2') | 
            (df_resultado['ramificacao_final'] == 'A8 | A3')
        ),
        'Motor - Aprovado, contudo, sem alçada. Direcionar para avaliação da mesa de crédito',
        df_resultado['parecer']
    )

    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'] == 'APROVADO') &
        (df_resultado['limite_solicitado'] > 100000) &
        (
            (df_resultado['ramificacao_final'] == 'A6 | A1') | 
            (df_resultado['ramificacao_final'] == 'A7 | A2') | 
            (df_resultado['ramificacao_final'] == 'A8 | A3')
        ),
        "MESA",
        df_resultado['decisao_final']
    )




    df_resultado['valor_aprovado'] = np.where(
        (df_resultado['decisao_final'] == 'APROVADO'),
        df_resultado['limite_solicitado'],
        0
    )


    # Gerando Path
    def gerando_path(row):
        current_date = datetime.now().strftime('%Y-%m-%d')
        return f"{row['cnpj_raiz']}/{current_date}-{row['pgid']}-{row['issue_jira']}"
    # Aplicando o Path
    df_resultado['path_arquivos_minio'] = df_resultado.apply(gerando_path, axis = 1)
    df_resultado['url'] = 'https://minio-datalake.alpe.com.br/raw/browser/analise-credito/' + df_resultado['path_arquivos_minio'] + '/'

    print('Parte Final')


    df_resultado = df_resultado.rename(columns={'documento_sem_formatacao':'cnpj_ec'})

    resposta_motor_resumida = df_resultado[['issue_jira', 'decisao_final', 'cnpj_ec', 'parecer', 'ramificacao_final', 'url', 'valor_aprovado']].rename(columns={
    'cnpj_ec': 'cnpj_ec',
    'decisao_final': 'resolucao',
    'ramificacao_final': 'ramificacao',
    'url' : 'path_arquivos_minio'
    })

    print('Teste parecer final:')
    print(resposta_motor_resumida)


    #depois do path
    resposta_motor_resumida = resposta_motor_resumida[['issue_jira', 'resolucao', 'cnpj_ec', 'valor_aprovado', 'parecer', 'ramificacao', 'path_arquivos_minio']]

    print("Quantidade de CNPJs por ramificação e decisão:")
    print(resposta_motor_resumida.groupby(['issue_jira','cnpj_ec','ramificacao','resolucao'])['cnpj_ec'].size())

    print("Quantidade de CNPJs por decisão:")
    print(resposta_motor_resumida.groupby(['resolucao'])['cnpj_ec'].size())


    print('Exportando o arquivo para o MinIO...')


    # Itera sobre cada combinação de 'issue_jira' e 'CNPJ' no DataFrame
    for _, row in df_resultado.iterrows():
        issue_jira = row['issue_jira']
        cnpj = row['cnpj_ec']
            
        # Gera o nome base do arquivo combinando 'issue_jira' e 'CNPJ'
        file_base_name = 'Resposta_Motor'

        # Filtra o DataFrame resumido e detalhado para o CNPJ específico
        df_detalhado = df_resultado[df_resultado['cnpj_ec'] == cnpj]

        # Adiciona mensagens de log para depuração
        print(f"Processando CNPJ: {cnpj}")
        print(f"Detalhado DF: {df_detalhado.shape}")

        # Gera o caminho de saída usando a coluna 'path'
        file_out_detalhado = f'{row["path_arquivos_minio"]}/{file_base_name}.csv'
            
        # Salva a análise detalhada
        csv_bytes_detalhado = df_detalhado.to_csv(index=False, sep=';').encode('utf-8')
        csv_buffer_detalhado = BytesIO(csv_bytes_detalhado)

        # Salva o arquivo no bucket MinIO usando o caminho gerado
        # # Conectando na refined
        minio_raw = Minio(
            access_params['endpoint_url_raw'],
            access_key=access_params['aws_access_key_id_raw'],
            secret_key=access_params['aws_secret_access_key_raw'],
        )

        BUCKET_SOURCE_RAW = "analise-credito"

        minio_raw.put_object(
            BUCKET_SOURCE_RAW,
            file_out_detalhado,
            data=csv_buffer_detalhado,
            length=len(csv_bytes_detalhado)
    )
        
    return resposta_motor_resumida.to_dict(orient='records')