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

def def_filtro_manutencao_yandeh(access_params=None,  **kwargs):

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
    query = f"""
with
nome_grupo_clientes_ativos as (
select
	distinct he.cnpj as cnpj_sacado, he.grupo_economico 
from 
	s3alpeyandeh.yandeh.hubspot_empresas he
),
base_limites as (
select
	numero_cnpj_sacado as cnpj_sacado, valor_limite as limite_alpe, limite_total as limite_yandeh
from 
	deltalaketrusted.limites.limites_yandeh la
	inner join s3alpeyandeh.yandeh.marketplace_limites ly on la.numero_cnpj_sacado = ly.cnpj_cliente and ly.classificacao = 'Limite Girotrade' 
where 
	valor_limite > 1 or limite_total > 1
),
base_badlist as (
select 
    regexp_replace(cnpj, '[./-]', '') AS cnpj_sacado, 1 as badlist 
from 
    s3alpeyandeh.yandeh.finance_badlist
),
base_inadimplente_alpe as (
select 
	distinct regexp_replace(cnpj_sacado, '[./-]', '') as cnpj_sacado, 1 as inadimplente_alpe
from 
	deltalaketrusted.yandeh.boletos_internos 
where 
	status_titulo = 'VENCIDO' 
),
base_inadimplente_yandeh as (
select 
	distinct regexp_replace(cnpj, '[./-]', '') AS cnpj_sacado, 1 as inadimplente_yandeh
from 
	s3alpeyandeh.yandeh.rm_boleto 
where 
	status_ <> 'BAIXADO'
	and vencimento < current_date 
),
base_grupo_economico as (
select 
	grupo_economico, max(greatest(coalesce(inadimplente_alpe, 0), coalesce(inadimplente_yandeh, 0))) as inadimplencia_grupo
		from (
	select 
		distinct he.grupo_economico, he.cnpj as cnpj_sacado, bia.inadimplente_alpe, biy.inadimplente_yandeh 
	from 
		s3alpeyandeh.yandeh.hubspot_empresas he 
		left join base_inadimplente_alpe bia on he.cnpj = bia.cnpj_sacado
		left join base_inadimplente_yandeh biy on he.cnpj = biy.cnpj_sacado)
group by 
	grupo_economico
),
base_inad_3_meses_alpe as (
select 
	regexp_replace(cnpj_sacado, '[./-]', '') as cnpj_sacado, sum(valor_face) as inadimplente_alpe_3_meses
from 
	deltalaketrusted.yandeh.boletos_internos 
where 
	status_titulo = 'VENCIDO'
	and data_vencimento >= current_date - INTERVAL '3' month 
	and data_vencimento < current_date
group by
	regexp_replace(cnpj_sacado, '[./-]', '')
),
base_inad_3_meses_yandeh as (
select 
	regexp_replace(cnpj, '[./-]', '') AS cnpj_sacado, sum(valor) as inadimplente_yandeh_3_meses
from 
	s3alpeyandeh.yandeh.rm_boleto 
where 
	status_ <> 'BAIXADO'
	and vencimento >= current_date - INTERVAL '3' month 
	and vencimento < current_date
group by
	regexp_replace(cnpj, '[./-]', '')
),
base_inad_12_meses_alpe as (
select 
	regexp_replace(cnpj_sacado, '[./-]', '') as cnpj_sacado, sum(valor_face) as inadimplente_alpe_12_meses
from 
	deltalaketrusted.yandeh.boletos_internos 
where 
	status_titulo = 'VENCIDO'
	and data_vencimento >= current_date - INTERVAL '12' month
	and data_vencimento < current_date
group by
	regexp_replace(cnpj_sacado, '[./-]', '')
),
base_inad_12_meses_yandeh as (
select 
	regexp_replace(cnpj, '[./-]', '') AS cnpj_sacado, sum(valor)as inadimplente_yandeh_12_meses
from 
	s3alpeyandeh.yandeh.rm_boleto 
where 
	status_ <> 'BAIXADO'
	and vencimento >= current_date - INTERVAL '12' month
	and vencimento < current_date
group by
	regexp_replace(cnpj, '[./-]', '')
),
maximo_dias_vencidos_alpe as (
select 
    distinct regexp_replace(cnpj, '[./-]', '') AS cnpj_sacado, 1 as mda_60_alpe
from 
    deltalakerefined.yandeh.maximo_dias_vencidos
where 
	dif_dias_qprof_yandeh > 60
),
maximo_dias_vencidos_yandeh as (
select 
    cnpj_sacado, max(dif_dias) as mda_60_yandeh
from (
    select regexp_replace(cnpj, '[./-]', '') AS cnpj_sacado, 
        DATE_DIFF(
            'day',
            vencimento,
            coalesce(DATE(data_alteracao), DATE(NOW()))
        ) AS dif_dias
    from 
        s3alpeyandeh.yandeh.rm_boleto 
    where 
        status_ = 'BAIXADO'
)
group by 
    cnpj_sacado
having 
	max(dif_dias) > 60
),
qtde_titulos_vencidos_alpe as (
select 
	regexp_replace(cnpj_sacado, '[./-]', '') as cnpj_sacado, 
    SUM(
        CASE
            WHEN 
                (
                    data_baixa IS NULL 
                    AND data_vencimento + INTERVAL '5' DAY < CURRENT_DATE
                )
                OR
                (
                    data_baixa IS NOT NULL 
                    AND data_baixa > data_vencimento + INTERVAL '5' DAY
                )
            THEN 1 
            ELSE 0
        END
    ) AS qtde_boletos_em_atraso_alpe
from 
	deltalaketrusted.yandeh.boletos_internos
where
    data_vencimento >= current_date - INTERVAL '12' MONTH
group by
    regexp_replace(cnpj_sacado, '[./-]', '')
),
qtde_titulos_vencidos_yandeh as (
select 
	regexp_replace(cnpj, '[./-]', '') as cnpj_sacado, 
    SUM(
        CASE
            WHEN 
                (
                    data_alteracao IS NULL 
                    AND vencimento + INTERVAL '5' DAY < CURRENT_DATE
                )
                OR
                (
                    data_alteracao IS NOT NULL 
                    AND data_alteracao > vencimento + INTERVAL '5' DAY
                )
            THEN 1 
            ELSE 0
        END
    ) AS qtde_boletos_em_atraso_yandeh
from 
	s3alpeyandeh.yandeh.rm_boleto 
where
    vencimento >= current_date - INTERVAL '12' month
    and status_ = 'BAIXADO'
group by
    regexp_replace(cnpj, '[./-]', '')
),
base_valor_pago_3_meses_alpe as (
select 
	regexp_replace(cnpj_sacado, '[./-]', '') as cnpj_sacado, sum(valor_face) as valor_pago_alpe
from 
	deltalaketrusted.yandeh.boletos_internos 
where 
	 data_vencimento >= current_date - INTERVAL '3' month
group by
	regexp_replace(cnpj_sacado, '[./-]', '')
),
base_valor_pago_3_meses_yandeh as (
select 
	regexp_replace(cnpj, '[./-]', '') as cnpj_sacado, sum(valor) as valor_pago_yandeh
from 
	s3alpeyandeh.yandeh.rm_boleto 
where 
	vencimento >= current_date - INTERVAL '3' month
group by
	regexp_replace(cnpj, '[./-]', '')
),
base_valor_pago_1_ano_alpe as (
select 
	regexp_replace(cnpj_sacado, '[./-]', '') as cnpj_sacado, sum(valor_face) as valor_pago_alpe
from 
	deltalaketrusted.yandeh.boletos_internos 
where 
	data_vencimento >= current_date - INTERVAL '12' month
group by
	regexp_replace(cnpj_sacado, '[./-]', '')
),
base_valor_pago_1_ano_yandeh as (
select 
	regexp_replace(cnpj, '[./-]', '') as cnpj_sacado, sum(valor) as valor_pago_yandeh
from 
	s3alpeyandeh.yandeh.rm_boleto 
where 
	vencimento >= current_date - INTERVAL '12' month
group by
	regexp_replace(cnpj, '[./-]', '')
),
base_score_alpe as (
with base_data as (
                            select 
                                re.id id
                                ,re.created_date data_consulta
                                ,rc.json_content
                                ,substring(last_re.value,1,8) cnpj_raiz
                                ,re.reports_id
                            from 
                                postgres.exrp_prd_default.report_execution re
                                inner join postgres.exrp_prd_default.report_content rc on rc.id = re.content_id
                                inner join (
                                    select 
                                        min(re.id) re_id
                                        ,rc.json_content
                                        ,pi2.value
                                        ,ROW_NUMBER() OVER (PARTITION BY pi2.value ORDER BY json_content desc) AS rn
                                    from postgres.exrp_prd_default.report_execution re
                                        inner join postgres.exrp_prd_default.report_definition rd on rd.id = re.definition_id and rd."type" in ('RELATORIO_DADOS_AVULSOS_PJ_YANDEH')
                                        inner join postgres.exrp_prd_default.report_content rc on rc.id = re.content_id
                                        inner join postgres.exrp_prd_default.report_involvement ri on ri.report_execution_id = re.id
                                        inner join postgres.exrp_prd_default.party_identification pi2 on pi2.party_id = ri.party_id
                                    where
                                        re.resolution = 'DONE'
                                    group by  
                                        rc.json_content
                                        ,pi2.value
                                )last_re on last_re.re_id = re.id and rn=1	
                        )
                        
        ,score_pj as (
                            select 
                                bd.id
                                ,bd.cnpj_raiz
                                ,bd.data_consulta
                                ,s.score as score_positivo_pj
                                ,case when s.message  = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' then 1 else 0 end as grande_empresa
                            from base_data bd
                                inner join postgres.exrp_prd_default.reports rs on rs.id = bd.reports_id
                                inner join postgres.exrp_prd_default.optional_features of2 on of2.id = rs.optional_features_id
                                inner join postgres.exrp_prd_default.score s on s.id = of2.score_id
        )

        ,consulta_mais_recente AS (
                            select 
                            bd.id
                            ,bd.cnpj_raiz
                            ,date(bd.data_consulta) as consulta_mais_recente 
                            from base_data	bd
        )
        select
            s.*
        from score_pj as s
        inner join consulta_mais_recente as cmr
            on s.cnpj_raiz = cmr.cnpj_raiz and try_cast(s.data_consulta as date) = cmr.consulta_mais_recente
        where s.data_consulta >= current_date - INTERVAL '180' DAY
),
base_score_yandeh as (
select 
    distinct lpad(trim(cnpj),14, '0') as cnpj_sacado, CAST(score AS INTEGER) AS score
from 
    s3alpeyandeh.yandeh.serasa_output 
where 
    data_criacao >= current_date - INTERVAL '180' DAY
),
base_adega as (
select cnpj_loja as cnpj_sacado, 1 as adega  from s3alpeyandeh.yandeh.revisao_adegas_vw where (valor_bebida / valor_nao_bebida) > 0.7
),
base_consolidada as (
select 
	bc.cnpj_sacado, substring(bc.cnpj_sacado, 1,8) as cnpj_raiz, nge.grupo_economico, coalesce(bc.limite_alpe, 0) as limite_alpe, coalesce(bc.limite_yandeh,0) as limite_yandeh, coalesce(bd.badlist, 0) as badlist,  coalesce(bia.inadimplente_alpe, 0) as inadimplente_alpe, 
	coalesce(biy.inadimplente_yandeh, 0) as inadimplente_yandeh, coalesce(ge.inadimplencia_grupo, 0) as inadimplencia_grupo, coalesce(bia3.inadimplente_alpe_3_meses, 0) as inadimplente_alpe_3_meses,
	coalesce(biy3.inadimplente_yandeh_3_meses, 0) as inadimplente_yandeh_3_meses, coalesce(bia12.inadimplente_alpe_12_meses, 0) as inadimplente_alpe_12_meses, coalesce(biy12.inadimplente_yandeh_12_meses, 0) as inadimplente_yandeh_12_meses,
	coalesce(mda.mda_60_alpe, 0) as mda_60_alpe, coalesce(mday.mda_60_yandeh, 0) as mda_60_yandeh, coalesce(qtva.qtde_boletos_em_atraso_alpe, 0) as qtde_boletos_em_atraso_alpe, coalesce(qtvy.qtde_boletos_em_atraso_yandeh, 0) as qtde_boletos_em_atraso_yandeh, 
	coalesce(vp3a.valor_pago_alpe, 0) as valor_pago_3_meses_alpe, coalesce(vp3y.valor_pago_yandeh, 0) as valor_pago_3_meses_yandeh,  coalesce(vpa.valor_pago_alpe, 0) as valor_pago_1_ano_alpe, coalesce(vpy.valor_pago_yandeh, 0) as valor_pago_1_ano_yandeh, 
	coalesce(adega.adega,0) as adega, scoralp.score_positivo_pj as score_alpe, scorayan.score as score_yandeh
from 
	base_limites bc
	left join nome_grupo_clientes_ativos nge on bc.cnpj_sacado = nge.cnpj_sacado 
	left join base_badlist bd on bc.cnpj_sacado = bd.cnpj_sacado 
	left join base_inadimplente_alpe bia on bc.cnpj_sacado = bia.cnpj_sacado 
	left join base_inadimplente_yandeh biy on bc.cnpj_sacado = biy.cnpj_sacado 
	left join base_grupo_economico ge on nge.grupo_economico = ge.grupo_economico 
	left join base_inad_3_meses_alpe bia3 on bc.cnpj_sacado = bia3.cnpj_sacado
	left join base_inad_3_meses_yandeh biy3 on bc.cnpj_sacado = biy3.cnpj_sacado 
	left join base_inad_12_meses_alpe bia12 on bc.cnpj_sacado = bia12.cnpj_sacado
	left join base_inad_12_meses_yandeh biy12 on bc.cnpj_sacado = biy12.cnpj_sacado 
	left join maximo_dias_vencidos_alpe mda on bc.cnpj_sacado = mda.cnpj_sacado
	left join maximo_dias_vencidos_yandeh mday on bc.cnpj_sacado = mday.cnpj_sacado
	left join qtde_titulos_vencidos_alpe qtva on bc.cnpj_sacado = qtva.cnpj_sacado
	left join qtde_titulos_vencidos_yandeh qtvy on bc.cnpj_sacado = qtvy.cnpj_sacado
	left join base_valor_pago_3_meses_alpe vp3a on bc.cnpj_sacado = vp3a.cnpj_sacado
	left join base_valor_pago_3_meses_yandeh vp3y on bc.cnpj_sacado = vp3y.cnpj_sacado
	left join base_valor_pago_1_ano_alpe vpa on bc.cnpj_sacado = vpa.cnpj_sacado
	left join base_valor_pago_1_ano_yandeh vpy on bc.cnpj_sacado = vpy.cnpj_sacado
	left join base_score_alpe scoralp on substring(bc.cnpj_sacado, 1,8) = scoralp.cnpj_raiz
	left join base_score_yandeh scorayan on bc.cnpj_sacado = scorayan.cnpj_sacado
	left join base_adega adega on bc.cnpj_sacado = adega.cnpj_sacado
),
base_final as (
select
	cnpj_sacado, cnpj_raiz, grupo_economico, badlist, greatest(coalesce(inadimplente_alpe, 0), coalesce(inadimplente_yandeh, 0)) as flag_inadimplente, inadimplencia_grupo, 
	(inadimplente_alpe_3_meses + inadimplente_yandeh_3_meses) as valor_inad_3_meses, (valor_pago_3_meses_alpe + valor_pago_3_meses_yandeh) as valor_pago_3_meses, ((inadimplente_alpe_3_meses + inadimplente_yandeh_3_meses) / (valor_pago_3_meses_alpe + valor_pago_3_meses_yandeh)) as pcto_inad_3_meses,
	(inadimplente_alpe_12_meses + inadimplente_yandeh_12_meses) as valor_inad_1_ano, (valor_pago_1_ano_alpe + valor_pago_1_ano_yandeh) as valor_pago_1_ano, ((inadimplente_alpe_12_meses + inadimplente_yandeh_12_meses) / (valor_pago_1_ano_alpe + valor_pago_1_ano_yandeh)) as pcto_inad_1_ano,
	greatest(coalesce(mda_60_alpe,0), coalesce(mda_60_yandeh,0)) as mda_60, greatest(coalesce(qtde_boletos_em_atraso_alpe, 0), coalesce(qtde_boletos_em_atraso_yandeh, 0)) as qtde_boletos_atrasados,
	(limite_alpe + limite_yandeh) as limite, 
	((limite_alpe + limite_yandeh) / (valor_pago_1_ano_alpe + valor_pago_1_ano_yandeh)) as pcto_limite_sobre_pgto,
	greatest(coalesce(score_alpe,0), coalesce(score_yandeh, 0)) as score, adega as flag_adega
from
	base_consolidada
)
select 
	cnpj_sacado, badlist, flag_inadimplente, inadimplencia_grupo, pcto_inad_3_meses, pcto_inad_1_ano, mda_60, qtde_boletos_atrasados, pcto_limite_sobre_pgto, score, flag_adega
from
	base_final
    """
    df = execute_query(conn, query)

    print(f"Quantidade de linhas no DataFrame 'df_query': {df.shape[0]}")

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    print(f"Quantidade de linhas no DataFrame final: {df.shape[0]}")

    # Exportando dados para a camada Trusted
    # # Conectando na Trusted        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "yandeh"
    FOLDER_DESTINATION_REFINED = "filtros/manutencao"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        df, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )