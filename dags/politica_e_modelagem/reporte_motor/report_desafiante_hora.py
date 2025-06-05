# Importando bibliotecas
import requests
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd
import numpy as np
import base64
from minio import Minio
from io import BytesIO
from requests.auth import HTTPBasicAuth
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
from tabulate import tabulate

def report_motor_desafiante_hora_hora (access_params=None):



    data_execucao = datetime.today().date()
    data_ultima_semana = data_execucao - timedelta(days=7)
    
    data_execucao_str = data_execucao.strftime('%Y-%m-%d')
    data_ultima_semana_str = data_ultima_semana.strftime('%Y-%m-%d')

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

    # Definindo a consulta
    query_acomp_desafiante = f"""
        select  
            sub.faixa_valor_solicitado as "Faixa Valor Solicitado",
            count(sub.issue_key) as "Entrantes",
            sum(case when sub.categoria_decisor = 'MESA' then 1 else 0 end) as "Derivadas Mesa",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'REPROVADO' then 1 else 0 end) as "Reprovadas Motor",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else 0 end) as "Aprovadas Motor",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.limite_aprovado else 0 end) as "Valor Aprovado Motor"
        from (
            select  
                a.*,
                vv.vop_performado,
                CASE
                    WHEN  limite_pedido <= 50000 THEN '01 - Até 50K'
                    WHEN  limite_pedido > 50000 AND  limite_pedido <= 60000 THEN '02 - 50K - 60K'
                    WHEN  limite_pedido > 60000 AND  limite_pedido <= 70000 THEN '03 - 60K - 70K'
                    WHEN  limite_pedido > 70000 AND  limite_pedido <= 80000 THEN '04 - 70K - 80K'
                    WHEN  limite_pedido > 80000 AND  limite_pedido <= 90000 THEN '05 - 80K - 90K'
                    WHEN  limite_pedido > 90000 AND  limite_pedido <= 100000 THEN '06 - 90K - 100K'
                    WHEN  limite_pedido > 100000 AND  limite_pedido <= 110000 THEN '07 - 100K - 110K'
                    WHEN  limite_pedido > 110000 AND  limite_pedido <= 120000 THEN '08 - 110K - 120K'
                    WHEN  limite_pedido > 120000 AND  limite_pedido <= 130000 THEN '09 - 120K - 130K'
                    WHEN  limite_pedido > 130000 AND  limite_pedido <= 140000 THEN '10 - 130K - 140K'
                    WHEN  limite_pedido > 140000 AND  limite_pedido <= 150000 THEN '11 - 140K - 150K'
                    ELSE '12 - >150K'
                END AS faixa_valor_solicitado
            from deltalaketrusted.jira.propostas as a
            left join deltalakerefined.payments.vop_vendermais vv
                on a.cnpj = vv.cnpj_sacado
            where a.politica = 'DESAFIANTE'
            and TRY_CAST (a.data_criado AS DATE) = DATE(timestamp '{data_execucao}')
        ) as sub
        group by sub.faixa_valor_solicitado
    """

    query_acomp_desafiante_consolidado = f"""
        select
            sub.faixa_valor_solicitado as "Faixa Valor Solicitado",
            count(sub.issue_key) as "Entrantes",
            sum(case when sub.categoria_decisor = 'MESA' then 1 else 0 end) as "Derivadas Mesa",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'REPROVADO' then 1 else 0 end) as "Reprovadas Motor",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else 0 end) as "Aprovadas Motor",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.limite_aprovado else 0 end) as "Valor Aprovado Motor",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop else 0 end) as "VOP",
            count(case when coalesce(sub.vop, 0) > 0 and sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else null end) as "Qtd VOP",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end) as "VOP Performado",
            count(case when coalesce(sub.vop_performado, 0) > 0 and sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else null end) as "Qtd VOP Performado",
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vencido else 0 end) as "Vencido",
            count(case when coalesce(sub.vencido, 0) > 0 and sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else null end) as "Qtd Vencido",
        
            case 
                when count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 then sub.cnpj end) > 0
                then round(
                    count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 and coalesce(sub.vencido, 0) > 0 then sub.cnpj end) * 100.00 /
                    count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 then sub.cnpj end), 2
                )
                else 0
            end as "% Over 1 - Qtd",
        
            case 
                when sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end, 0)) > 0
                then round(
                    sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and sub.vencido > 0 then sub.vencido else 0 end, 0)) /
                    sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end, 0)) * 100, 2
                )
                else 0
            end as "% Over 1 - R$",
        
            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_over_30 else 0 end) as "Over 30",
            count(case when coalesce(sub.vop_over_30, 0) > 0 and sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else null end) as "Qtd Over 30",
        
            case 
                when count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 then sub.cnpj end) > 0
                then round(
                    count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 and coalesce(sub.vop_over_30, 0) > 0 then sub.cnpj end) * 100.00 /
                    count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 then sub.cnpj end), 2
                )
                else 0
            end as "% Over 30 - Qtd",
        
            case 
                when sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end, 0)) > 0
                then round(
                    sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and sub.vop_over_30 > 0 then sub.vop_over_30 else 0 end, 0)) /
                    sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end, 0)) * 100, 2
                )
                else 0
            end as "% Over 30 - R$"
        
        from (
            select
                a.*,
                case
                    when limite_pedido <= 50000 then '01 - Até 50K'
                    when limite_pedido > 50000 and limite_pedido <= 60000 then '02 - 50K - 60K'
                    when limite_pedido > 60000 and limite_pedido <= 70000 then '03 - 60K - 70K'
                    when limite_pedido > 70000 and limite_pedido <= 80000 then '04 - 70K - 80K'
                    when limite_pedido > 80000 and limite_pedido <= 90000 then '05 - 80K - 90K'
                    when limite_pedido > 90000 and limite_pedido <= 100000 then '06 - 90K - 100K'
                    when limite_pedido > 100000 and limite_pedido <= 110000 then '07 - 100K - 110K'
                    when limite_pedido > 110000 and limite_pedido <= 120000 then '08 - 110K - 120K'
                    when limite_pedido > 120000 and limite_pedido <= 130000 then '09 - 120K - 130K'
                    when limite_pedido > 130000 and limite_pedido <= 140000 then '10 - 130K - 140K'
                    when limite_pedido > 140000 and limite_pedido <= 150000 then '11 - 140K - 150K'
                    else '12 - >150K'
                end as faixa_valor_solicitado,
                coalesce(b.vop, 0) as vop,
                coalesce(b.vop_performado, 0) as vop_performado,
                coalesce(b.vencido, 0) as vencido,
                coalesce(b.vop_over_30, 0) as vop_over_30
            from deltalaketrusted.jira.propostas as a
            left join (
                select 
                    cnpj_sacado,
                    sum(vop) as vop,
                    sum(vop_performado) as vop_performado,
                    sum(vencido) as vencido,
                    sum(vop_over_30) as vop_over_30
                from deltalakerefined.payments.vop_vendermais
                where TRY_CAST(safra_concessao AS DATE) >= DATE '2025-04-01'
                group by cnpj_sacado
            ) as b on a.cnpj = b.cnpj_sacado
            where politica = 'DESAFIANTE'
            and TRY_CAST(data_criado AS DATE) >= DATE '2025-04-14'
        ) as sub
        where sub.faixa_valor_solicitado is not null
        group by sub.faixa_valor_solicitado
        order by sub.faixa_valor_solicitado
        """
    query_acomp_desafiante_consolidado_100k = f"""
            select
                sub.faixa_valor_solicitado as "Faixa Valor Solicitado",
                count(sub.issue_key) as "Entrantes",
                sum(case when sub.categoria_decisor = 'MESA' then 1 else 0 end) as "Derivadas Mesa",
                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'REPROVADO' then 1 else 0 end) as "Reprovadas Motor",
                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else 0 end) as "Aprovadas Motor",
                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.limite_aprovado else 0 end) as "Valor Aprovado Motor",
                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop else 0 end) as "VOP",
                count(case when coalesce(sub.vop, 0) > 0 and sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else null end) as "Qtd VOP",
                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end) as "VOP Performado",
                count(case when coalesce(sub.vop_performado, 0) > 0 and sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else null end) as "Qtd VOP Performado",
                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vencido else 0 end) as "Vencido",
                count(case when coalesce(sub.vencido, 0) > 0 and sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else null end) as "Qtd Vencido",
            
                case 
                    when count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 then sub.cnpj end) > 0
                    then round(
                        count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 and coalesce(sub.vencido, 0) > 0 then sub.cnpj end) * 100.00 /
                        count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 then sub.cnpj end), 2
                    )
                    else 0
                end as "% Over 1 - Qtd",
            
                case 
                    when sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end, 0)) > 0
                    then round(
                        sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and sub.vencido > 0 then sub.vencido else 0 end, 0)) /
                        sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end, 0)) * 100, 2
                    )
                    else 0
                end as "% Over 1 - R$",
            
                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_over_30 else 0 end) as "Over 30",
                count(case when coalesce(sub.vop_over_30, 0) > 0 and sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else null end) as "Qtd Over 30",
            
                case 
                    when count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 then sub.cnpj end) > 0
                    then round(
                        count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 and coalesce(sub.vop_over_30, 0) > 0 then sub.cnpj end) * 100.00 /
                        count(distinct case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and coalesce(sub.vop_performado, 0) > 0 then sub.cnpj end), 2
                    )
                    else 0
                end as "% Over 30 - Qtd",
            
                case 
                    when sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end, 0)) > 0
                    then round(
                        sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' and sub.vop_over_30 > 0 then sub.vop_over_30 else 0 end, 0)) /
                        sum(coalesce(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_performado else 0 end, 0)) * 100, 2
                    )
                    else 0
                end as "% Over 30 - R$"
            
            from (
                select
                    a.*,
                    case
                        when limite_pedido <= 50000 then '01 - Até 50K'
                        when limite_pedido > 50000 and limite_pedido <= 60000 then '02 - 50K - 60K'
                        when limite_pedido > 60000 and limite_pedido <= 70000 then '03 - 60K - 70K'
                        when limite_pedido > 70000 and limite_pedido <= 80000 then '04 - 70K - 80K'
                        when limite_pedido > 80000 and limite_pedido <= 90000 then '05 - 80K - 90K'
                        when limite_pedido > 90000 and limite_pedido <= 100000 then '06 - 90K - 100K'
                        else null
                    end as faixa_valor_solicitado,
                    coalesce(b.vop, 0) as vop,
                    coalesce(b.vop_performado, 0) as vop_performado,
                    coalesce(b.vencido, 0) as vencido,
                    coalesce(b.vop_over_30, 0) as vop_over_30
                from deltalaketrusted.jira.propostas as a
                left join (
                    select 
                        cnpj_sacado,
                        sum(vop) as vop,
                        sum(vop_performado) as vop_performado,
                        sum(vencido) as vencido,
                        sum(vop_over_30) as vop_over_30
                    from deltalakerefined.payments.vop_vendermais
                    where TRY_CAST(safra_concessao AS DATE) >= DATE '2025-04-01'
                    group by cnpj_sacado
                ) as b on a.cnpj = b.cnpj_sacado
                where politica = 'DESAFIANTE'
                and TRY_CAST(data_criado AS DATE) >= DATE '2025-04-14'
            ) as sub
            where sub.faixa_valor_solicitado is not null
            group by sub.faixa_valor_solicitado
            order by sub.faixa_valor_solicitado

        """

    query_clientes_com_vop = f"""
            select
                    b.nome_sacado as "NOME SACADO", 
                    b.cnpj_sacado as "CNPJ",
                    round(coalesce(b.vop, 0), 2) as "VOP",
                    cast(b.prazo_medio as integer) as "PRAZO MÉDIO"
                from deltalaketrusted.jira.propostas as a
                left join ( 
                    select 
                        nome_sacado,
                        cnpj_sacado, 
                        sum(vop) as vop,
                        avg(prazo_medio) as prazo_medio
                    from deltalakerefined.payments.vop_vendermais vv
                    where TRY_CAST(safra_concessao AS DATE) >= DATE '2025-04-01'
                    group by nome_sacado, cnpj_sacado
                ) as b
                on a.cnpj = b.cnpj_sacado
                where politica = 'DESAFIANTE'
                AND TRY_CAST(data_criado AS DATE) >= DATE(timestamp '{data_ultima_semana}')
                and a.categoria_decisor = 'MOTOR'
                and a.decisao = 'APROVADO'
                and coalesce(b.vop, 0) > 0
            
    """

    # Verifica se a conexão foi bem-sucedida antes de executar a consulta reporte diário de propostas
    if conn is not None:
        propostas_desafiante = execute_query(conn, query_acomp_desafiante)
        propostas_desafiante_consolidado = execute_query(conn, query_acomp_desafiante_consolidado)
        propostas_desafiante_consolidado_100k = execute_query(conn, query_acomp_desafiante_consolidado_100k)
        propostas_clientes_com_vop = execute_query(conn, query_clientes_com_vop)
    else:
        propostas_desafiante = None
        propostas_desafiante_consolidado = None
        propostas_desafiante_consolidado_100k = None
        propostas_clientes_com_vop = None
        print("A consulta não foi executada porque a conexão com o Trino falhou.")

    # Cálculos dos percentuais reporte diário de propostas
    propostas_totais = propostas_desafiante['Entrantes'].sum()
    propostas_mesa = propostas_desafiante['Derivadas Mesa'].sum()
    propostas_reprov_motor = propostas_desafiante['Reprovadas Motor'].sum()
    propostas_aprov_motor = propostas_desafiante['Aprovadas Motor'].sum()
        
    if propostas_totais > 0:
        percent_mesa = (propostas_mesa / propostas_totais) * 100
        percent_reprov_motor = (propostas_reprov_motor / propostas_totais) * 100
        percent_aprov_motor = (propostas_aprov_motor / propostas_totais) * 100
    else:
        percent_mesa = 0
        percent_reprov_motor = 0
        percent_aprov_motor = 0
        
    print(percent_mesa)
    print(percent_reprov_motor)
    print(percent_aprov_motor)

    # Cálculos dos percentuais reporte consolidado de propostas
    
    propostas_totais_consolidado = propostas_desafiante_consolidado['Entrantes'].sum()
    propostas_mesa_consolidado = propostas_desafiante_consolidado['Derivadas Mesa'].sum()
    propostas_reprov_motor_consolidado = propostas_desafiante_consolidado['Reprovadas Motor'].sum()
    propostas_aprov_motor_consolidado = propostas_desafiante_consolidado['Aprovadas Motor'].sum()
    
    if propostas_totais_consolidado > 0:
        percent_mesa_consolidado = (propostas_mesa_consolidado / propostas_totais_consolidado) * 100
        percent_reprov_motor_consolidado = (propostas_reprov_motor_consolidado / propostas_totais_consolidado) * 100
        percent_aprov_motor_consolidado = (propostas_aprov_motor_consolidado / propostas_totais_consolidado) * 100
    else:
        percent_mesa_consolidado = 0
        percent_reprov_motor_consolidado = 0
        percent_aprov_motor_consolidado = 0
    
    print(percent_mesa_consolidado)
    print(percent_reprov_motor_consolidado)
    print(percent_aprov_motor_consolidado)

    # Cálculos dos percentuais reporte consolidado de propostas (até 100k)
    propostas_totais_100k = propostas_desafiante_consolidado_100k['Entrantes'].sum()
    propostas_mesa_100k = propostas_desafiante_consolidado_100k['Derivadas Mesa'].sum()
    propostas_reprov_motor_100k = propostas_desafiante_consolidado_100k['Reprovadas Motor'].sum()
    propostas_aprov_motor_100k = propostas_desafiante_consolidado_100k['Aprovadas Motor'].sum()
    
    if propostas_totais_100k > 0:
        percent_mesa_100k = (propostas_mesa_100k / propostas_totais_100k) * 100
        percent_reprov_motor_100k = (propostas_reprov_motor_100k / propostas_totais_100k) * 100
        percent_aprov_motor_100k = (propostas_aprov_motor_100k / propostas_totais_100k) * 100
    else:
        percent_mesa_100k         = 0
        percent_reprov_motor_100k = 0
        percent_aprov_motor_100k  = 0
    
    print(percent_mesa_100k)         
    print(percent_reprov_motor_100k)  
    print(percent_aprov_motor_100k)   

    # Cálculos de quantidade de VOP e Média do Prazo Médio Total

    totais_clientes_vop = propostas_clientes_com_vop[propostas_clientes_com_vop['CNPJ'] != 'Total']['CNPJ'].count()
    print(totais_clientes_vop)

    prazo_medio_real = propostas_clientes_com_vop[propostas_clientes_com_vop['CNPJ'] != 'Total']['PRAZO MÉDIO'].mean()
    prazo_medio_real = round(prazo_medio_real) if pd.notna(prazo_medio_real) else 0
    print(prazo_medio_real)


    print("Print do DF, antes da criação do markdown:")
    print(propostas_desafiante)
    print(propostas_desafiante_consolidado)
    print(propostas_desafiante_consolidado_100k)
    print(propostas_clientes_com_vop)

    #Adiciona totais no reporte diário de propostas
    totais = {
    'Faixa Valor Solicitado': 'Total',
    'Entrantes': propostas_desafiante['Entrantes'].sum(),
    'Derivadas Mesa': propostas_desafiante['Derivadas Mesa'].sum(),
    'Reprovadas Motor': propostas_desafiante['Reprovadas Motor'].sum(),
    'Aprovadas Motor': propostas_desafiante['Aprovadas Motor'].sum(),
    'Valor Aprovado Motor': propostas_desafiante ['Valor Aprovado Motor'].sum()
    
    }
    
    # Adiciona linha de totais
    propostas_desafiante = pd.concat([propostas_desafiante, pd.DataFrame([totais])], ignore_index=True)
    
    propostas_desafiante['Valor Aprovado Motor'] = propostas_desafiante['Valor Aprovado Motor'].apply(
        lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
    )

    #Adiciona totais no reporte consolidado de propostas
    totais_consolidado = {
        'Faixa Valor Solicitado': 'Total',
        'Entrantes': propostas_desafiante_consolidado['Entrantes'].sum(),
        'Derivadas Mesa': propostas_desafiante_consolidado['Derivadas Mesa'].sum(),
        'Reprovadas Motor': propostas_desafiante_consolidado['Reprovadas Motor'].sum(),
        'Aprovadas Motor': propostas_desafiante_consolidado['Aprovadas Motor'].sum(),
        'Valor Aprovado Motor': propostas_desafiante_consolidado['Valor Aprovado Motor'].sum(),
        'VOP': propostas_desafiante_consolidado['VOP'].sum(),
        'Qtd VOP': propostas_desafiante_consolidado['Qtd VOP'].sum(),
        'VOP Performado': propostas_desafiante_consolidado['VOP Performado'].sum(),
        'Qtd VOP Performado': propostas_desafiante_consolidado['Qtd VOP Performado'].sum(),
        'Vencido': propostas_desafiante_consolidado['Vencido'].sum(),
        'Qtd Vencido': propostas_desafiante_consolidado['Qtd Vencido'].sum(),
        'Over 30': propostas_desafiante_consolidado['Over 30'].sum(),
        'Qtd Over 30': propostas_desafiante_consolidado['Qtd Over 30'].sum()
    }

    # Calcula os percentuais no total
    df_validos = propostas_desafiante_consolidado[propostas_desafiante_consolidado['Faixa Valor Solicitado'] != 'Total']

    soma_qtd_vop_performado = df_validos['Qtd VOP Performado'].sum()
    soma_qtd_vencido = df_validos['Qtd Vencido'].sum()
    soma_qtd_over30 = df_validos['Qtd Over 30'].sum()

    totais_consolidado['% Over 1 - Qtd'] = round((soma_qtd_vencido / soma_qtd_vop_performado) * 100, 2) if soma_qtd_vop_performado > 0 else 0
    totais_consolidado['% Over 1 - R$'] = round((totais_consolidado['Vencido'] / totais_consolidado['VOP Performado']) * 100, 2) if totais_consolidado['VOP Performado'] > 0 else 0
    totais_consolidado['% Over 30 - Qtd'] = round((soma_qtd_over30 / soma_qtd_vop_performado) * 100, 2) if soma_qtd_vop_performado > 0 else 0
    totais_consolidado['% Over 30 - R$'] = round((totais_consolidado['Over 30'] / totais_consolidado['VOP Performado']) * 100, 2) if totais_consolidado['VOP Performado'] > 0 else 0


    # Adiciona o total ao DataFrame
    propostas_desafiante_consolidado = pd.concat(
        [propostas_desafiante_consolidado, pd.DataFrame([totais_consolidado])],
        ignore_index=True
    )

    # Formatação dos valores monetários
    for col in ['Valor Aprovado Motor', 'VOP', 'VOP Performado', 'Vencido', 'Over 30']:
        propostas_desafiante_consolidado[col] = propostas_desafiante_consolidado[col].apply(
            lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
        )
    
    # Formatação dos valores percentuais para exibir vírgula decimal e %
    percent_cols = ['% Over 1 - Qtd', '% Over 1 - R$', '% Over 30 - Qtd', '% Over 30 - R$']
    
    for col in percent_cols:
        propostas_desafiante_consolidado[col] = propostas_desafiante_consolidado[col].apply(
            lambda x: f"{x:.2f}".replace('.', ',') + '%' if isinstance(x, (int, float)) else x
        )

    # Adiciona totais no reporte consolidado de propostas até 100k
    totais_100k = {
        'Faixa Valor Solicitado': 'Total',
        'Entrantes': propostas_desafiante_consolidado_100k['Entrantes'].sum(),
        'Derivadas Mesa': propostas_desafiante_consolidado_100k['Derivadas Mesa'].sum(),
        'Reprovadas Motor': propostas_desafiante_consolidado_100k['Reprovadas Motor'].sum(),
        'Aprovadas Motor': propostas_desafiante_consolidado_100k['Aprovadas Motor'].sum(),
        'Valor Aprovado Motor': propostas_desafiante_consolidado_100k['Valor Aprovado Motor'].sum(),
        'VOP': propostas_desafiante_consolidado_100k['VOP'].sum(),
        'Qtd VOP': propostas_desafiante_consolidado_100k['Qtd VOP'].sum(),
        'VOP Performado': propostas_desafiante_consolidado_100k['VOP Performado'].sum(),
        'Qtd VOP Performado': propostas_desafiante_consolidado_100k['Qtd VOP Performado'].sum(),
        'Vencido': propostas_desafiante_consolidado_100k['Vencido'].sum(),
        'Qtd Vencido': propostas_desafiante_consolidado_100k['Qtd Vencido'].sum(),
        'Over 30': propostas_desafiante_consolidado_100k['Over 30'].sum(),
        'Qtd Over 30': propostas_desafiante_consolidado_100k['Qtd Over 30'].sum()
    }
    
    # Calcula os percentuais no total
    df_validos_100k = propostas_desafiante_consolidado_100k[propostas_desafiante_consolidado_100k['Faixa Valor Solicitado'] != 'Total']

    soma_qtd_vop_performado_100k = df_validos_100k['Qtd VOP Performado'].sum()
    soma_qtd_vencido_100k = df_validos_100k['Qtd Vencido'].sum()
    soma_qtd_over30_100k = df_validos_100k['Qtd Over 30'].sum()

    totais_100k['% Over 1 - Qtd'] = round((soma_qtd_vencido_100k / soma_qtd_vop_performado_100k) * 100, 2) if soma_qtd_vop_performado_100k > 0 else 0
    totais_100k['% Over 1 - R$'] = round((totais_100k['Vencido'] / totais_100k['VOP Performado']) * 100, 2) if totais_100k['VOP Performado'] > 0 else 0
    totais_100k['% Over 30 - Qtd'] = round((soma_qtd_over30_100k / soma_qtd_vop_performado_100k) * 100, 2) if soma_qtd_vop_performado_100k > 0 else 0
    totais_100k['% Over 30 - R$'] = round((totais_100k['Over 30'] / totais_100k['VOP Performado']) * 100, 2) if totais_100k['VOP Performado'] > 0 else 0
    
    
    
    # Adiciona o total ao DataFrame
    propostas_desafiante_consolidado_100k = pd.concat(
        [propostas_desafiante_consolidado_100k, pd.DataFrame([totais_100k])],
        ignore_index=True
    )
    
    # Formatação dos valores monetários
    for col in ['Valor Aprovado Motor', 'VOP', 'VOP Performado', 'Vencido', 'Over 30']:
        propostas_desafiante_consolidado_100k[col] = propostas_desafiante_consolidado_100k[col].apply(
            lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') 
            if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
        )
    
    # Formatação dos valores percentuais para exibir vírgula decimal e %
    percent_cols_100k = ['% Over 1 - Qtd', '% Over 1 - R$', '% Over 30 - Qtd', '% Over 30 - R$']
    
    for col in percent_cols:
        propostas_desafiante_consolidado_100k[col] = propostas_desafiante_consolidado_100k[col].apply(
            lambda x: f"{x:.2f}".replace('.', ',') + '%' if isinstance(x, (int, float)) else x
        )

    media_prazo = propostas_clientes_com_vop['PRAZO MÉDIO'].mean()

    prazo = int(media_prazo) if not np.isnan(media_prazo) else 0
    
    clientes_vop = {
        'NOME SACADO': '',
        'CNPJ': 'Total',
        'VOP': propostas_clientes_com_vop['VOP'].sum(),
        'PRAZO MÉDIO': prazo
    }
    
    # Adiciona a linha de totais ao final da tabela consolidada
    propostas_clientes_com_vop = pd.concat(
        [propostas_clientes_com_vop, pd.DataFrame([clientes_vop])],
        ignore_index=True
    
    )
    
    propostas_clientes_com_vop['VOP'] = propostas_clientes_com_vop['VOP'].apply(
        lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
    )

    # 1) Separar total - Diária
    df_total = propostas_desafiante[propostas_desafiante['Faixa Valor Solicitado'].str.strip().str.lower() == 'total']
    df_main = propostas_desafiante[propostas_desafiante['Faixa Valor Solicitado'].str.strip().str.lower() != 'total']
    
    # Ordenar faixas
    propostas_desafiante_sorted = df_main.sort_values(by='Faixa Valor Solicitado', ascending=True)
    
    # Concatenar novamente com Total no final
    propostas_desafiante_sorted = pd.concat([propostas_desafiante_sorted, df_total], ignore_index=True)
    
    
    # 2) Separar total - Consolidada
    df_total_consolidado = propostas_desafiante_consolidado[propostas_desafiante_consolidado['Faixa Valor Solicitado'].str.strip().str.lower() == 'total']
    df_main_consolidado = propostas_desafiante_consolidado[propostas_desafiante_consolidado['Faixa Valor Solicitado'].str.strip().str.lower() != 'total']
    
    # Ordenar faixas
    propostas_desafiante_consolidado_sorted = df_main_consolidado.sort_values(by='Faixa Valor Solicitado', ascending=True)
    
    # Garantir que o total consolidado apareça uma vez ao final
    propostas_desafiante_consolidado_sorted = pd.concat([propostas_desafiante_consolidado_sorted, df_total_consolidado], ignore_index=True)
    
    # 3) Separar total - Até 100k
    df_total_100k = propostas_desafiante_consolidado_100k[propostas_desafiante_consolidado_100k['Faixa Valor Solicitado'].str.strip().str.lower() == 'total']
    df_main_100k = propostas_desafiante_consolidado_100k[propostas_desafiante_consolidado_100k['Faixa Valor Solicitado'].str.strip().str.lower() != 'total']
    
    # Ordenar faixas
    propostas_desafiante_consolidado_100k_sorted = df_main_100k.sort_values(by='Faixa Valor Solicitado', ascending=True)
    
    # Concatenar novamente com Total no final
    propostas_desafiante_consolidado_100k_sorted = pd.concat([propostas_desafiante_consolidado_100k_sorted, df_total_100k], ignore_index=True)
    
    
    # 4) Separar total - VOP
    
    # Separar total sem coluna auxiliar
    df_vop = propostas_clientes_com_vop[propostas_clientes_com_vop['CNPJ'].str.strip().str.lower() != 'total'].copy()
    df_vop_total = propostas_clientes_com_vop[propostas_clientes_com_vop['CNPJ'].str.strip().str.lower() == 'total'].copy()
    
    # Converter VOP para float para ordenar
    df_vop['VOP_num'] = (
        df_vop['VOP']
        .str.replace(r'R\$\s*', '', regex=True)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .astype(float)
    )
    
    # Ordenar decrescente
    df_vop = df_vop.sort_values('VOP_num', ascending=False).drop(columns='VOP_num')
    
    # Concatenar com total e resetar índice
    propostas_clientes_com_vop_sorted = pd.concat([df_vop, df_vop_total], ignore_index=True)
    
    
    
    tabela_formatada_1 = tabulate(
        propostas_desafiante_sorted.values.tolist(),
        headers=propostas_desafiante_sorted.columns.tolist(),
        tablefmt="pretty"
    )
    
    tabela_formatada_2 = tabulate(
        propostas_desafiante_consolidado_sorted.values.tolist(),
        headers=propostas_desafiante_consolidado_sorted.columns.tolist(),
        tablefmt="pretty"
    )
    
    tabela_formatada_3 = tabulate(
        propostas_desafiante_consolidado_100k_sorted.values.tolist(),
        headers=propostas_desafiante_consolidado_100k_sorted.columns.tolist(),
        tablefmt="pretty"
    )
    
    tabela_formatada_4 = tabulate(
        propostas_clientes_com_vop_sorted.values.tolist(),
        headers=propostas_clientes_com_vop_sorted.columns.tolist(),
        tablefmt="pretty"
    )
    
    # Markdown invisível para espaçamento no Teams
    invisible_space = "\u200B"
    
    # Criar markdown final com percentuais separados por tabela
    markdown = (
        "📊 Resumo Diário de Propostas - Política Desafiante\n\n"
        f"{invisible_space}\n"
        f"📅 Data Referência: {data_execucao}\n\n"
        "---\n"
        f"🧾 % Derivadas para Mesa: {percent_mesa:.2f}%  \n"
        f"✅ % Aprovadas Motor: {percent_aprov_motor:.2f}%  \n"
        f"❌ % Reprovadas Motor: {percent_reprov_motor:.2f}%  \n\n"
        "```\n" + tabela_formatada_1 + "\n```\n"
        "---\n"
        f"{invisible_space}\n"
        "---\n"
        "📊 Resumo Consolidado\n\n"
         "---\n"
        f"🧾 % Derivadas para Mesa: {percent_mesa_consolidado:.2f}%  \n"
        f"✅ % Aprovadas Motor: {percent_aprov_motor_consolidado:.2f}%  \n"
        f"❌ % Reprovadas Motor: {percent_reprov_motor_consolidado:.2f}%  \n\n"
        "```\n" + tabela_formatada_2 + "\n```\n"
        "---\n"
        f"{invisible_space}\n"
        "---\n"
        "📊 Resumo Até 100k\n\n"
        "---\n"
        f"🧾 % Derivadas para Mesa: {percent_mesa_100k:.2f}%  \n"
        f"✅ % Aprovadas Motor: {percent_aprov_motor_100k:.2f}%  \n"
        f"❌ % Reprovadas Motor: {percent_reprov_motor_100k:.2f}%  \n\n"
        "```\n" + tabela_formatada_3 + "\n```\n"
        "---\n"
        f"{invisible_space}\n"
        "---\n"
        f"📅 SACADOS COM VOP - ÚLTIMA SEMANA {data_ultima_semana.strftime('%d/%m/%Y')} até {data_execucao.strftime('%d/%m/%Y')}\n\n"
        "---\n"
        f"💰 Sacados com VOP: {totais_clientes_vop:.0f}  \n"
        f"🕒 Prazo Médio: {prazo_medio_real:.0f} dias  \n\n"
        "```\n" + tabela_formatada_4 + "\n```"
    )

    # Exibir o markdown final
    print(markdown)

    # Função para enviar a mensagem formatada ao webhook do Teams
    def enviar_para_webhook(mensagem):
        webhook_url = "https://yandehbr.webhook.office.com/webhookb2/aff1add1-1e5e-445d-9644-f7d9ab677641@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/cd64a656b86b4db6a8a64a74153a8555/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2M-crEG-kOlO8wQffCBWAHSBeR29YtNktVPx1gvoiR4M1"
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        payload = {
            "text": mensagem
        }
        
        response = requests.post(webhook_url, json=payload, headers=headers)
        
        if response.status_code == 200:
            print("Mensagem enviada com sucesso para o Teams!")
        else:
            print(f"Falha ao enviar a mensagem. Código de status: {response.status_code}")

    
    # Enviar a mensagem combinada para o webhook
    enviar_para_webhook(markdown)