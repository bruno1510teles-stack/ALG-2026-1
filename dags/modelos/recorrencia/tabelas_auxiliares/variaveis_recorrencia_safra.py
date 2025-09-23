### IMPORTANDO BIBLIOTECAS
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from deltalake import write_deltalake, DeltaTable
from airflow.utils.log.logging_mixin import LoggingMixin


def cria_variaveis_modelo_recorrencia (access_params=None, **kwargs):

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
    

    query_data_ref = f"""
                    select distinct data_ref
                    from deltalaketrusted.payments.boletos_internos_acumulada
                    """

    data_ref_df = execute_query (conn, query_data_ref)

    datas_safras = data_ref_df['data_ref'].unique()
    datas_safras.sort()

    print(datas_safras)



    # Lista para acumular os resultados
    resultados = []

    for data in datas_safras:

        data_str = data.strftime('%Y-%m-%d')

        print('Executando fechamento para a data:')
        print(data_str)

        ### PUBLICO DA ANÁLISE (CLIENTES QUE JÁ TIVERAM NA DEVIDA SAFRA)
        
        query_publico = f"""
                select distinct 
                    substring(replace(replace(replace(cnpj_sacado, '.', ''), '/', ''), '-', ''), 1, 8) as raiz_cnpj 
                from deltalaketrusted.payments.boletos_internos_acumulada
                where data_ref = date '{data_str}'
        """
        
        df_publico = execute_query (conn, query_publico)



        #### LISTA DAS QUERIES ####

        ### PRAZO MÉDIO DAS OPERAÇÕES, DATA PRIMEIRA CONTRATAÇÃO, DATA DA ÚLTIMA CONTRATAÇÃO, DIAS DESDE A ÚLTIMA CONTRATAÇÃO, DIF DE DIAS PRIMEIRA
        ### CONTRATAÇÃO E ÚLTIMA.
        query_var1 = f"""
                    select
                        sub.raiz_cnpj_sacado as raiz_cnpj,
                        round(avg(sub.dias_prazo_op), 0) as prazo_medio_op,
                        min(cast(data_emissao as date)) as primeira_contratacao,
                        max(cast(data_emissao as date)) as ultima_contratacao,
                        DATE_DIFF('day', min(cast(data_emissao as date)), max(cast(data_emissao as date))) AS dif_dias_primeira_ultima_contratacao
                    from (
                        select
                            SUBSTRING(replace(replace(replace(cnpj_sacado, '.', ''), '/', ''), '-', ''), 1, 8) as raiz_cnpj_sacado,
                            data_baixa,
                            data_vencimento,
                            data_emissao,
                            date_diff('day', data_emissao, data_vencimento) as dias_prazo_op
                        from deltalaketrusted.payments.boletos_internos_acumulada
                        where data_ref = date '{data_str}') as sub
                    group by sub.raiz_cnpj_sacado
        """
        
        
        ### QTD MESES CONTRATADOS, MESES POSSÍVEIS DE CONTRATAÇÃO (COMPARANDO PRIMEIRA CONTRATAÇÃO E HOJE)
        
        query_var2 = f"""
                    with meses_contratacao as (		
                        select
                            count(distinct safra_concessao) as meses_possiveis_contratacao_vop
                        from deltalaketrusted.payments.boletos_internos_acumulada
                        where data_ref = date '{data_str}'
                    ),
                    
                    qtd_meses_contratados as (
        
                        select
                            sub.raiz_cnpj_sacado as raiz_cnpj,
                            count(distinct sub.safra_concessao) as qtd_meses_contratados
                        from (
                            select
                                SUBSTRING(replace(replace(replace(cnpj_sacado, '.', ''), '/', ''), '-', ''), 1, 8) as raiz_cnpj_sacado,
                                safra_concessao,
                                sum(valor_face) as vop_mensal
                            from deltalaketrusted.payments.boletos_internos_acumulada
                            where data_ref = date '{data_str}'
                            group by 1, safra_concessao ) as sub
                        group by sub.raiz_cnpj_sacado
                    )
                    
                    select a.*,
                            b.meses_possiveis_contratacao_vop
                    from qtd_meses_contratados as a
                    left join meses_contratacao as b
                        on 1 = 1
        """
        
        
        ### TEMPO MÉDIO (DIAS) ENTRE AS CONTRATAÇÕES
        
        query_var3 = f"""
                    with datas as (
                                select 	distinct
                                        substring(replace(replace(replace(cnpj_sacado, '.', ''), '/', ''), '-', ''), 1, 8) as raiz_cnpj,
                                        numero_nota_fiscal,
                                        min(cast(data_emissao as date)) as data_emissao
                                from deltalaketrusted.payments.boletos_internos_acumulada
                                where data_ref = date '{data_str}'
                                group by 1, 2 
                    ),
                    diffs AS (
                        select
                            raiz_cnpj,
                            data_emissao,
                            row_number() over (partition by raiz_cnpj order by data_emissao asc) as posicao,
                            DATE_DIFF(
                                'day',
                                lag(data_emissao) over (partition by raiz_cnpj order by data_emissao asc),
                                data_emissao
                            ) AS diff_dias
                        from datas
                    )
                    select
                        raiz_cnpj,
                        avg(diff_dias) as media_intervalo_dias_contratacoes
                    from diffs
                    group by raiz_cnpj
        """
        
        
        ### MÁXIMO DE MESES CONSECUTIVOS DE COMPRA
        
        query_var4 = f"""
                        WITH datas AS (
                            SELECT
                                substring(replace(replace(replace(cnpj_sacado, '.', ''), '/', ''), '-', ''), 1, 8) AS raiz_cnpj,
                                MIN(CAST(data_emissao AS DATE)) AS data_emissao
                            FROM deltalaketrusted.payments.boletos_internos_acumulada
                            where data_ref = date '{data_str}'
                            GROUP BY
                                substring(replace(replace(replace(cnpj_sacado, '.', ''), '/', ''), '-', ''), 1, 8),
                                date_trunc('month', CAST(data_emissao AS DATE))
                        ),
                        meses AS (
                            SELECT
                                raiz_cnpj,
                                date_trunc('month', data_emissao) AS mes
                            FROM datas
                        ),
                        sequencias AS (
                            SELECT
                                raiz_cnpj,
                                mes,
                                ROW_NUMBER() OVER (PARTITION BY raiz_cnpj ORDER BY mes) AS rn,
                                DATE_DIFF(
                                    'month',
                                    DATE '2000-01-01',
                                    mes
                                ) - ROW_NUMBER() OVER (PARTITION BY raiz_cnpj ORDER BY mes) AS grp
                            FROM meses
                        ),
                        agrupado AS (
                            SELECT
                                raiz_cnpj,
                                grp,
                                COUNT(*) AS qtd_meses_consecutivos
                            FROM sequencias
                            GROUP BY raiz_cnpj, grp
                        )
                        SELECT
                            raiz_cnpj,
                            MAX(qtd_meses_consecutivos) AS meses_consecutivos_contratado
                        FROM agrupado
                        GROUP BY raiz_cnpj
        """


        ### EXECUTANDO QUERIES
        variaveis_1 = execute_query(conn, query_var1.format(data_str=data_str))
        variaveis_2 = execute_query(conn, query_var2.format(data_str=data_str))
        variaveis_2['percent_contratacao'] = round(variaveis_2['qtd_meses_contratados'] / variaveis_2['meses_possiveis_contratacao_vop'], 2) * 100
        variaveis_3 = execute_query(conn, query_var3.format(data_str=data_str))
        variaveis_4 = execute_query(conn, query_var4.format(data_str=data_str))


        ### CRUZANDO TUDO
        df = df_publico.merge(variaveis_1, on='raiz_cnpj', how='left')
        df = df.merge(variaveis_2, on='raiz_cnpj', how='left')
        df = df.merge(variaveis_3, on='raiz_cnpj', how='left')
        df = df.merge(variaveis_4, on='raiz_cnpj', how='left')

        ### CRIANDO FAIXAS DAS VARIAVEIS PARA TESTE DO MODELO
        
        df['faixa_qtd_meses_contratados'] = np.select(
            [
                df['qtd_meses_contratados'] <= 2,
                df['qtd_meses_contratados'] <= 8,
                df['qtd_meses_contratados'] > 8
            ],
            [
                "ATÉ 2 MESES",
                "DE 2 À 8 MESES",
                "MAIS QUE 8 MESES",
            ],
            default="OUTRO"
        )


        
        df['faixa_meses_consecutivos_contratado'] = np.select(
            [
                df['meses_consecutivos_contratado'] <= 2,
                df['meses_consecutivos_contratado'] <= 4,
                df['meses_consecutivos_contratado'] > 4
            ],
            [
                "ATÉ 3 MESES",
                "DE 3 À 6 MESES",
                "MAIS QUE 6 MESES",
            ],
            default="OUTRO"
        )

        
        # Garantir que está em float com NaN reconhecido pelo pandas
        df['percent_contratacao'] = pd.to_numeric(df['percent_contratacao'], errors="coerce")
        
        df['faixa_percent_contratacao'] = np.select(
            [
                df['percent_contratacao'] <= 25,
                df['percent_contratacao'] <= 75,
                df['percent_contratacao'] <= 100
            ],
            [
                "0% - 25%",
                "25% - 75%",
                "75% - 100%",
            ],
            default="OUTRO"
        )


        
        df['faixa_intervalo_entre_compras'] = np.select(
            [
                df['media_intervalo_dias_contratacoes'].isna(),
                df['media_intervalo_dias_contratacoes'] <= 30,
                df['media_intervalo_dias_contratacoes'] <= 90,
                df['media_intervalo_dias_contratacoes'] > 90
            ],
            [
                "1 CONTRATAÇÃO",
                "<= 30 DIAS",
                "<= 90 DIAS",
                "> 90 DIAS",
            ],
            default="OUTRO"
        )

        df_modelo = df[['raiz_cnpj', 'media_intervalo_dias_contratacoes','faixa_percent_contratacao',
                    'faixa_intervalo_entre_compras', 'faixa_qtd_meses_contratados',
                    'faixa_meses_consecutivos_contratado', 'meses_consecutivos_contratado',
                    'qtd_meses_contratados']].copy()

        ### FLAGS
        df_modelo['flag_cliente_novo'] = (df_modelo['qtd_meses_contratados'] <= 2).astype(int)
        df_modelo['flag_retomou'] = ((df_modelo['media_intervalo_dias_contratacoes'] >= 150) &
                                    (df_modelo['qtd_meses_contratados'] > 2)).astype(int)

        ### APLICANDO O PIPELINE
        df_safra = df_modelo.copy()
        
        df_safra["data_ref"] = data
        
        ### ACUMULANDO RESULTADO
        resultados.append(df_safra)



    # CONCTENA TUDO NO FINAL
    df_resultado = pd.concat(resultados, ignore_index=True)


    # Garante que data_ref vai ser a primeira coluna
    cols = ["data_ref"] + [c for c in df_resultado.columns if c != "data_ref"]
    df_resultado = df_resultado[cols]


    # Adicionando colunas de data e hora
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_resultado['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_resultado['year'], df_resultado['month'], df_resultado['day'] = now.year, now.month, now.day


    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }


        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_REFINED = "modelos"
        FOLDER_DESTINATION_REFINED = "tabelas_auxiliares/recorrencia"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            df_resultado, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            # overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")