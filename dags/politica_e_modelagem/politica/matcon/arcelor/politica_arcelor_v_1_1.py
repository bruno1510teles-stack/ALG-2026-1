# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from io import BytesIO
import os, re, pytz
from confluent_kafka import Producer
import json
from datetime import datetime
from airflow.models import Variable

def execucao_politica(access_params=None,  **kwargs):
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/auxiliar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/out'

    # Pegando DF tarefa anterior
    # Recupera o objeto ti (task instance) via kwargs
    ti = kwargs['ti']
    saida_modelo_dict = ti.xcom_pull(task_ids='modelo_task')
    saida_modelo = pd.DataFrame(saida_modelo_dict)

    cnpjs = saida_modelo['cnpj_raiz'].unique()
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query = f"({ids_query})"
    print(ids_query)

    #Ajustando a formatação do CNPJ para 14 digitos
    saida_modelo['cnpj_raiz'] = saida_modelo['cnpj_raiz'].astype(str).str.zfill(8)

    # Acrescentando possui ou não HP

    sem_hp = (saida_modelo['CLASSIFICACAO'].isna()) & (saida_modelo['resposta'] == 'SEGUE')
    com_hp = (saida_modelo['CLASSIFICACAO'].notna()) & (saida_modelo['resposta'] == 'SEGUE')

    saida_modelo['possui_hp'] = None

    saida_modelo.loc[sem_hp, 'possui_hp'] = 'NAO'
    saida_modelo.loc[com_hp, 'possui_hp'] = 'SIM'

    # Definindo função de classificação
    def classificar (rating):
        if rating in ['A', 'B', 'C']:
            return 'SEGUE'
        elif rating == 'D':
            return "MESA"
        elif rating =='E':
            return 'REPROVADO'
    
    saida_modelo['resposta_modelo'] = saida_modelo['CLASSIFICACAO'].apply(classificar)

    # Separando os casos que seguem analise com HP
    seguem_analise_com_hp = saida_modelo[saida_modelo['CLASSIFICACAO'].isin(['A','B','C'])]

    # Configurando conexão Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    # Puxando dados do serasa
    cur = conn.cursor()

#     query = (f"""
#             with 
#         total_restritivos_pj as (
# select 
#     t1.org_id as id,
#     t1.data_consulta,
#     t1.cnpj_raiz,
#     sum(valor_total) as "TOTAL RESTRITIVOS"
# from (
# 	select distinct
#         org.id org_id,
#         date(split_part(org.data_hora_consulta, ' ', 1)) AS data_consulta,
#         org.cnpj_raiz,
#         remp.id,
#         remp.grupo_ocorrencia,
#         remp.quantidade_ocorrencia,
#         remp.ano_mes_primeiro,
#         remp.ano_mes_ultimo,
#         remp.moeda,
#         remp.codigo_natureza,
#         remp.fonte,
#         remp.titular_pendencia,
#         coalesce(remp.valor_total, 0) as valor_total
#     from
#         deltalaketrusted.serasa.organizacoes org
#     left join deltalaketrusted.serasa.resumo_restritivo remp
#         on
#         remp.id = org.id
#         and remp.titular_pendencia = CONCAT('0',
#         org.cnpj_raiz)
#     where
#         org.cnpj_raiz in {ids_query}
# )t1
# group by
#     t1.org_id,
#     t1.data_consulta,
#     t1.cnpj_raiz
#     )
#     ,
#         qtde_cheque_pj as (
# select 
#     t1.org_id as id,
#     sum(t1."QTD CHEQUE") as "QTD CHEQUE"
# from (
# 	select distinct
#         org.id org_id,
#         org.cnpj_raiz,
#         remp.id,
#         remp.grupo_ocorrencia,
#         remp.quantidade_ocorrencia,
#         remp.ano_mes_primeiro,
#         remp.ano_mes_ultimo,
#         remp.moeda,
#         remp.codigo_natureza,
#         remp.fonte,
#         remp.titular_pendencia,
#         coalesce(remp.quantidade_ocorrencia, 0) as "QTD CHEQUE"
#     from
#         deltalaketrusted.serasa.organizacoes org
#     left join deltalaketrusted.serasa.resumo_restritivo remp
#         on
#         remp.id = org.id
#         and remp.titular_pendencia = CONCAT('0',
#         org.cnpj_raiz)
#     where
#         org.cnpj_raiz in {ids_query}
#         and remp.grupo_ocorrencia = 'CHEQUE'
#     ) t1
# group by
#     t1.org_id
#     )
#     ,
#         qtde_cheque_pf as (
# select 
#     t1.org_id as id,
#     sum(t1."CHEQUE PF") as "CHEQUE PF"
# from (
# 	select distinct
#         socios.id org_id,
#         rsocio.id,
#         rsocio.grupo_ocorrencia,
#         rsocio.quantidade_ocorrencia,
#         rsocio.ano_mes_primeiro,
#         rsocio.ano_mes_ultimo,
#         rsocio.moeda,
#         rsocio.codigo_natureza,
#         rsocio.fonte,
#         rsocio.titular_pendencia,
#         coalesce(rsocio.quantidade_ocorrencia, 0) as "CHEQUE PF"
#     from
#         deltalaketrusted.serasa.socios socios
#     left join deltalaketrusted.serasa.resumo_restritivo rsocio
#         on
#         rsocio.id = socios.id
#         and rsocio.titular_pendencia = socios.documento_socio
#     where
#         rsocio.grupo_ocorrencia = 'CHEQUE') t1
# group by
#     t1.org_id
#     )
#     ,
#         total_restritivos_socio as (
# select 
#     t1.org_id as id,
#     sum(valor_total) as "RESTRITIVOS PF"
# from (
# 	select distinct
#         socios.id org_id,
#         rsocio.id,
#         rsocio.grupo_ocorrencia,
#         rsocio.quantidade_ocorrencia,
#         rsocio.ano_mes_primeiro,
#         rsocio.ano_mes_ultimo,
#         rsocio.moeda,
#         rsocio.codigo_natureza,
#         rsocio.fonte,
#         rsocio.titular_pendencia,
#         coalesce(rsocio.valor_total, 0) as valor_total
#     from
#         deltalaketrusted.serasa.socios socios
#     left join deltalaketrusted.serasa.resumo_restritivo rsocio
#         on
#         rsocio.id = socios.id
#         and rsocio.titular_pendencia = socios.documento_socio
# )t1
# group by
#     t1.org_id
#     )
#     ,
#         score_pj as (
#     select 
#         sc.id, sc.valor_score as "Score Positivo PJ",
#         case when sc.probabilidade_inadimplencia_mensagem  = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' then 1 else 0 end as grande_empresa
#     from
#         deltalaketrusted.serasa.score sc
#     ),
#         consulta_mais_recente AS (
#     select
#         org.cnpj_raiz,
#         MAX(date(split_part(org.data_hora_consulta, ' ', 1))) AS consulta_mais_recente
#     from 
#         deltalaketrusted.serasa.organizacoes org 
#     where 
#         org.cnpj_raiz in {ids_query}
#     group by
#         org.cnpj_raiz
#     ),
# 	RankedSocios AS (
#         SELECT 
#             org.id, 
#             so.documento_socio, 
#             so.percentual_capital,
#             ROW_NUMBER() OVER (PARTITION BY org.id ORDER BY so.percentual_capital DESC, so.documento_socio) AS rn
#         FROM 
#             deltalaketrusted.serasa.organizacoes org
#         LEFT JOIN 
#             deltalaketrusted.serasa.socios so ON org.id = so.id
#         where 
#             org.cnpj_raiz in {ids_query}
#     )
#     select 
#         trp.id,
#         trp.data_consulta,
#         trp.cnpj_raiz,
#         score."Score Positivo PJ",
#         score.grande_empresa,
#         coalesce(trp."TOTAL RESTRITIVOS", 0) as "TOTAL RESTRITIVOS",
#         coalesce(cpj."QTD CHEQUE", 0) as "QTD CHEQUE",
#         coalesce(cpf."CHEQUE PF", 0) as "CHEQUE PF",
#         coalesce(trs."RESTRITIVOS PF", 0) as "RESTRITIVOS PF",
#         rs.documento_socio as "CPF do Principal Socio"
#     from 
#         total_restritivos_pj trp
#     left join 
#         qtde_cheque_pj cpj
#         on trp.id = cpj.id
#     left join 
#         qtde_cheque_pf cpf
#         on trp.id = cpf.id
#     left join 
#         total_restritivos_socio trs 
#         on trp.id = trs.id
#     left join 
#         score_pj score
#         on trp.id = score.id
#     inner join 
#         consulta_mais_recente cmr
#         on trp.cnpj_raiz = cmr.cnpj_raiz and trp.data_consulta = cmr.consulta_mais_recente
#     left join 
#         RankedSocios rs ON trp.id = rs.id
#     where 
#         rs.rn = 1
#         """)
    

    query = (
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

    cur.execute(query)

    # Obtém os resultados
    rows = cur.fetchall()

    # Fecha o cursor e a conexão
    cur.close()
    conn.close()

    # Para pegar o nome das colunas, você pode usar cur.description
    columns = [desc[0] for desc in cur.description]
    base_retorno_serasa = pd.DataFrame(rows, columns=columns)
    print(f"Quantidade de CNPJs que retornou da base_retorno_serasa: {base_retorno_serasa.shape[0]}")


    seguem_analise_com_hp = seguem_analise_com_hp.merge(base_retorno_serasa, on = ['cnpj_raiz'], how = 'left')

    # Função com a árvore de decisão
    def politica_com_hp(linha):
        if (linha['CLASSIFICACAO'] == 'C' and
        pd.isnull(linha[['Score Positivo PJ','TOTAL RESTRITIVOS','QTD CHEQUE','RESTRITIVOS PF','CHEQUE PF']]).values.any()):
            return 'A - C8' 
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] > 0):  
                return 'A - C7'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and 
            linha['Score Positivo PJ'] == 2):
                return 'A - C6'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 1 or
            linha['Capital Social'] > 100000000)) :
                return 'A - C6'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 500000):
                return 'A - C5'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha ['TOTAL RESTRITIVOS'] > 1000 and 
            linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] < 316):
                return 'A - C4'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 1000 and 
            linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] >= 316): 
                return 'A - C3'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] < 316): 
                return 'A - C2'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] >= 316): 
                return 'A - C1'
        elif (linha['CLASSIFICACAO'] == 'B' and
        pd.isnull(linha[['Score Positivo PJ','TOTAL RESTRITIVOS','QTD CHEQUE','RESTRITIVOS PF','CHEQUE PF']]).values.any()):
            return 'A - B8' 
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] > 0):  
                return 'A - B7'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] == 2):
                return 'A - B6'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 1 or
            linha['Capital Social'] > 100000000)):
                return 'A - B6'    
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 500000):
                return 'A - B5'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 1000 and 
            linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] < 316):
                return 'A - B4'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 1000 and 
            linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] >= 316):
                return 'A - B3'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] < 316): 
                return 'A - B2'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] >= 316): 
                return 'A - B1'
        elif (linha['CLASSIFICACAO'] == 'A' and
        pd.isnull(linha[['Score Positivo PJ','TOTAL RESTRITIVOS','QTD CHEQUE','RESTRITIVOS PF','CHEQUE PF']]).values.any()):
            return 'A - A12' 
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] > 0):  
                return 'A - A11'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] == 2):
                return 'A - A10'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 1 or
            linha['Capital Social'] > 100000000)) :
                return 'A - A10'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 500000):
                return 'A - A9'
        elif(linha['CLASSIFICACAO'] == 'A' and    
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] > 1000 and 
            linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] < 316):
                return 'A - A8'
        elif(linha['CLASSIFICACAO'] == 'A' and    
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] > 1000 and 
            linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] >= 316):
                return 'A - A7'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] < 316):
                return 'A - A6'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] >= 316 and 
            linha['Score Positivo PJ'] <= 830):
                return 'A - A5'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] > 830 and
            linha['RESTRITIVOS PF'] > 500):
                return 'A - A4'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] > 830 and
            pd.isnull(linha['CPF do Principal Socio'])):
                return 'A - A3'    
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] > 830 and
            linha['RESTRITIVOS PF'] <= 500 and
            linha['idade_socio'] < 2) :
                return 'A - A2'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] > 830 and
            linha['RESTRITIVOS PF'] <= 500 and
            linha['idade_socio'] >= 2) :
                return 'A - A1'  
        return "Verificar"

    seguem_analise_com_hp['DECISAO_POLITICA'] = seguem_analise_com_hp.apply(politica_com_hp, axis=1)

    seguem_analise_com_hp['resposta_motor'] = None

      # Separando as linhas que não possuem HP para rodar a politica

    segue_analise_sem_hp = saida_modelo[saida_modelo['possui_hp'] == 'NAO']

    segue_analise_sem_hp = segue_analise_sem_hp.merge(base_retorno_serasa, on = ['cnpj_raiz'], how = 'left')

    # Função com a árvore de decisão sem HP
    def politica_sem_hp(linha):
        if pd.isnull(linha[['Score Positivo PJ','TOTAL RESTRITIVOS','QTD CHEQUE','RESTRITIVOS PF','CHEQUE PF']]).values.any():
            return 'B - 12'
        elif linha['QTD CHEQUE'] > 0: 
            return 'B - 11'
        elif(linha['QTD CHEQUE'] == 0 and 
            (linha['grande_empresa'] == 1 or
            linha['Capital Social'] > 100000000)):
                return 'B - 10'
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 500000):
                return 'B - 9'
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 1000 and 
            linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] < 316):
                return 'B - 8'
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and
            linha['TOTAL RESTRITIVOS'] > 1000 and 
            linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] >= 316): 
                return 'B - 7'
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and    
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] < 316):
                return 'B - 6'
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and    
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] >= 316 and
            linha['Score Positivo PJ'] <= 830):
                return "B - 5"
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 830 and
            pd.isnull(linha['CPF do Principal Socio'])):
                return "B - 4"
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 830 and
            pd.isnull(linha['idade_socio'])):
                return "B - 4"
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 830 and
            linha['RESTRITIVOS PF'] > 500):
                return "B - 3"
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 830 and   
            linha['RESTRITIVOS PF'] <= 500 and
            linha['idade_socio'] < 2):
                return "B - 2"
        elif(linha['QTD CHEQUE'] == 0 and
            (linha['grande_empresa'] == 0 or
            linha['Capital Social'] <= 100000000) and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 830 and   
            linha['RESTRITIVOS PF'] <= 500 and
            linha['idade_socio'] >= 2):
                return "B - 1"
       
        return "Verificar"
    

    segue_analise_sem_hp['DECISAO_POLITICA'] = segue_analise_sem_hp.apply(politica_sem_hp, axis=1)
    
    #  Juntando DFs
    cnpjs_politica_c = list(seguem_analise_com_hp['cnpj_raiz'])
    cnpjs_politica_s = list(segue_analise_sem_hp['cnpj_raiz'])

    cnpjs_politica = cnpjs_politica_c + cnpjs_politica_s

    filtro = ~saida_modelo['cnpj_raiz'].isin(cnpjs_politica)

    sem_politica = saida_modelo[filtro]

    resposta_motor = pd.concat([sem_politica, seguem_analise_com_hp, segue_analise_sem_hp], axis=0)
    
    #Aplicando decisão de politica nos classificados com E e D no score do Modelo   
    
    resposta_motor.loc[resposta_motor['CLASSIFICACAO'] == 'E', 'DECISAO_POLITICA'] = 'A - E1'
    
    resposta_motor.loc[resposta_motor['CLASSIFICACAO'] == 'D', 'DECISAO_POLITICA'] = 'A - D1' 
    
    
    resposta_motor['resposta_motor'] = None    
    
    # REPROVADO
    resposta_motor.loc[resposta_motor['DECISAO_POLITICA'].isin(['A - E1','A - C7', 'A - C5', 'A - C4','A - C2', 'A - B7', 'A - B5', 'A - B4', 'A - A11','A - A9', 'A - A8', 'A - A6', 'B - 11', 'B - 6', 'B - 9', 'B - 8']), 'resposta_motor'] = 'REPROVADO'
    
    # MESA
    resposta_motor.loc[resposta_motor['DECISAO_POLITICA'].isin(['A - A2', 'A - A3', 'A - A4', 'A - A5', 'A - A7','A - A10', 'A - B1', 'A - B2','A - B3','A - B6', 'A - C1','A - C3','A - C6','A - D1', 'B - 10', 'B - 7', 'B - 5', 'B - 4', 'B - 3', 'B - 2','B - 12', 'A - A12', 'A - B8', 'A - C8']), 'resposta_motor'] = 'MESA'
    
    # APROVADO
    resposta_motor.loc[resposta_motor['DECISAO_POLITICA'].isin(['A - A1', 'B - 1']), 'resposta_motor'] = 'MESA'


    
    filtro_na = resposta_motor['resposta_motor'].isnull()
    resposta_motor.loc[filtro_na, 'resposta_motor'] = resposta_motor.loc[filtro_na, 'resposta_modelo']

    filtro_na = resposta_motor['resposta_motor'].isnull()
    resposta_motor.loc[filtro_na, 'resposta_motor'] = resposta_motor.loc[filtro_na, 'resposta']

    resposta_motor['ramificacao_motor'] = resposta_motor['DECISAO_POLITICA']

    filtro_na = resposta_motor['DECISAO_POLITICA'].isnull()
    resposta_motor.loc[filtro_na, 'ramificacao_motor'] = resposta_motor.loc[filtro_na, 'ramificacao_pre_filtro']
    resposta_motor = resposta_motor.drop(columns=['idade_y', 'codigo_porte_empresa_y'])
    resposta_motor = resposta_motor.rename(columns={'codigo_porte_empresa_x':'codigo_porte_empresa'})
    resposta_motor = resposta_motor.rename(columns={'idade_x':'idade'})
    resposta_motor = resposta_motor.rename(columns={'documento_sem_formatacao':'cnpj_ec'})

    #DEFININDO O PARECER DO MOTOR
    resposta_motor.loc[resposta_motor['resposta_motor'] == 'REPROVADO', 'parecer'] = 'Motor - Recusado, dados analisados fora da politica atual'
    resposta_motor.loc[resposta_motor['resposta_motor'] == 'MESA', 'parecer'] = 'Motor - Direcionar para avaliação da mesa de crédito'
    resposta_motor.loc[resposta_motor['resposta_motor'] == 'mantido', 'parecer'] = 'Motor - Limite mantido'


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
        "B - 6":"Motor - Recusado, apontamento/score"
    }

    # Função que retorna o parecer personalizado ou o parecer original se não houver mapeamento
    def get_parecer_personalizado(row):
        ramificacao = row['ramificacao_motor']
        parecer_atual = row['parecer']
        # Tenta obter o parecer com base na 'ramificacao_motor'
        return parecer_map.get(ramificacao, parecer_atual)

    # Aplica a função ao DataFrame
    resposta_motor['parecer'] = resposta_motor.apply(get_parecer_personalizado, axis=1)

    # Gerando Path
    def gerando_path(row):
           current_date = datetime.now().strftime('%Y-%m-%d')
           return f"{row['cnpj_raiz']}/{current_date}-{row['pgid']}-{row['issue_jira']}"
    # Aplicando o Path
    resposta_motor['path_arquivos_minio'] = resposta_motor.apply(gerando_path, axis = 1)
    resposta_motor['url'] = 'https://minio-datalake.alpe.com.br/raw/browser/analise-credito/' + resposta_motor['path_arquivos_minio'] + '/'


    resposta_motor_resumida = resposta_motor[['issue_jira', 'resposta_motor', 'cnpj_ec', 'parecer', 'ramificacao_motor', 'url']].rename(columns={
    'cnpj_ec': 'cnpj_ec',
    'resposta_motor': 'resolucao',
    'ramificacao_motor': 'ramificacao',
    'url' : 'path_arquivos_minio'
    })

    resposta_motor_resumida['valor_aprovado'] = 0

    #depois do path
    resposta_motor_resumida = resposta_motor_resumida[['issue_jira', 'resolucao', 'cnpj_ec', 'valor_aprovado', 'parecer', 'ramificacao', 'path_arquivos_minio']]

    print("Quantidade de CNPJs por ramificação:")
    print(resposta_motor_resumida.groupby(['issue_jira','cnpj_ec','ramificacao'])['cnpj_ec'].size())

    print("Quantidade de CNPJs por parecer:")
    print(resposta_motor_resumida.groupby(['issue_jira','cnpj_ec','parecer'])['cnpj_ec'].size())


# Itera sobre cada combinação de 'issue_jira' e 'CNPJ' no DataFrame
    for _, row in resposta_motor.iterrows():
        issue_jira = row['issue_jira']
        cnpj = row['cnpj_ec']
             
        # Gera o nome base do arquivo combinando 'issue_jira' e 'CNPJ'
        file_base_name = 'Resposta_Motor'

        # Filtra o DataFrame resumido e detalhado para o CNPJ específico
        df_detalhado = resposta_motor[resposta_motor['cnpj_ec'] == cnpj]

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