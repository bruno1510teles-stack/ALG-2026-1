
# IMPORTANDO BIBLIOTECAS

from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timezone, timedelta
from trino.auth import BasicAuthentication
from deltalake import write_deltalake
from trino.dbapi import connect
from functools import reduce
from minio import Minio
from io import BytesIO
import pandas as pd
import numpy as np
import logging
import re


def performance_visao_mob (access_params=None,  **kwargs):

    # CONECTANDO COM O TRINO

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
    


    # BASE BOLETOS INTERNOS

    query_boleto =  f"""
                    select
                        substring(regexp_replace(cnpj_sacado, '[./-]', ''), 1, 8) as raiz_cnpj,
                        *   
                    from deltalaketrusted.payments.boletos_internos
                    """

    boleto = execute_query(conn, query_boleto)
    print(f"Quantidade de linhas no DataFrame 'boleto': {boleto.shape[0]}")

    df_titulos = boleto.copy()


    # TRATANDO COLUNAS DE DATAS

    df_titulos['data_emissao']    = pd.to_datetime(df_titulos['data_emissao'])
    df_titulos['data_efetivacao'] = pd.to_datetime(df_titulos['data_efetivacao'])
    df_titulos['data_vencimento'] = pd.to_datetime(df_titulos['data_vencimento'])
    df_titulos['data_baixa']      = pd.to_datetime(df_titulos['data_baixa'], errors='coerce')


    # CALCULANDO DATAS DOS MOBs

    df_titulos['data_mob1']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=1)
    df_titulos['data_mob2']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=2)
    df_titulos['data_mob3']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=3)
    df_titulos['data_mob4']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=4)
    df_titulos['data_mob5']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=5)
    df_titulos['data_mob6']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=6)
    df_titulos['data_mob7']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=7)
    df_titulos['data_mob8']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=8)
    df_titulos['data_mob9']  =  df_titulos['data_efetivacao'] + pd.DateOffset(months=9)
    df_titulos['data_mob10'] =  df_titulos['data_efetivacao'] + pd.DateOffset(months=10)
    df_titulos['data_mob11'] =  df_titulos['data_efetivacao'] + pd.DateOffset(months=11)
    df_titulos['data_mob12'] =  df_titulos['data_efetivacao'] + pd.DateOffset(months=12)


    # CALCULANDO DIAS EM ATRASO NOS MOBS

    # MOB 1
    df_titulos['dias_em_atraso_mob1'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob1']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob1'])),
        (df_titulos['data_mob1'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB2
    df_titulos['dias_em_atraso_mob2'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob2']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob2'])),
        (df_titulos['data_mob2'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB3
    df_titulos['dias_em_atraso_mob3'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob3']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob3'])),
        (df_titulos['data_mob3'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB4
    df_titulos['dias_em_atraso_mob4'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob4']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob4'])),
        (df_titulos['data_mob4'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB5
    df_titulos['dias_em_atraso_mob5'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob5']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob5'])),
        (df_titulos['data_mob5'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB6
    df_titulos['dias_em_atraso_mob6'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob6']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob6'])),
        (df_titulos['data_mob6'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB7
    df_titulos['dias_em_atraso_mob7'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob7']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob7'])),
        (df_titulos['data_mob7'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB8
    df_titulos['dias_em_atraso_mob8'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob8']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob8'])),
        (df_titulos['data_mob8'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB 9
    df_titulos['dias_em_atraso_mob9'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob9']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob9'])),
        (df_titulos['data_mob9'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB 10
    df_titulos['dias_em_atraso_mob10'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob10']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob10'])),
        (df_titulos['data_mob10'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB 11
    df_titulos['dias_em_atraso_mob11'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob11']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob11'])),
        (df_titulos['data_mob11'] - df_titulos['data_vencimento']).dt.days,
        0 )

    # MOB 12
    df_titulos['dias_em_atraso_mob12'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob12']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob12'])),
        (df_titulos['data_mob12'] - df_titulos['data_vencimento']).dt.days,
        0 )
    

    # CALCULANDO VALOR VENCIDO NOS MOBS

    # MOB 1
    df_titulos['valor_vencido_mob1'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob1']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob1'])),
        df_titulos['valor_face'],
        0 )

    # MOB2
    df_titulos['valor_vencido_mob2'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob2']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob2'])),
        df_titulos['valor_face'],
        0 )

    # MOB3
    df_titulos['valor_vencido_mob3'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob3']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob3'])),
        df_titulos['valor_face'],
        0 )

    # MOB4
    df_titulos['valor_vencido_mob4'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob4']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob4'])),
        df_titulos['valor_face'],
        0 )

    # MOB5
    df_titulos['valor_vencido_mob5'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob5']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob5'])),
        df_titulos['valor_face'],
        0 )

    # MOB6
    df_titulos['valor_vencido_mob6'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob6']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob6'])),
        df_titulos['valor_face'],
        0 )

    # MOB7
    df_titulos['valor_vencido_mob7'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob7']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob7'])),
        df_titulos['valor_face'],
        0 )

    # MOB8
    df_titulos['valor_vencido_mob8'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob8']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob8'])),
        df_titulos['valor_face'],
        0 )

    # MOB 9
    df_titulos['valor_vencido_mob9'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob9']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob9'])),
        df_titulos['valor_face'],
        0 )

    # MOB 10
    df_titulos['valor_vencido_mob10'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob10']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob10'])),
        df_titulos['valor_face'],
        0 )

    # MOB 11
    df_titulos['valor_vencido_mob11'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob11']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob11'])),
        df_titulos['valor_face'],
        0 )

    # MOB 12
    df_titulos['valor_vencido_mob12'] = np.where (
        (df_titulos['data_vencimento'] < df_titulos['data_mob12']) &
        ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > df_titulos['data_mob12'])),
        df_titulos['valor_face'],
        0 )
    

    # GERANDO TABELA DE MOBS

    df_titulos['safra_concessao'] = df_titulos['safra_concessao'] + pd.offsets.MonthEnd(0)

    df_mobs = (
        df_titulos
        .groupby(['safra_concessao', 'raiz_cnpj'], as_index=False)
        .agg({
            'dias_em_atraso_mob1': 'max', 'dias_em_atraso_mob2': 'max', 'dias_em_atraso_mob3': 'max', 'dias_em_atraso_mob4': 'max',
            'dias_em_atraso_mob5': 'max', 'dias_em_atraso_mob6': 'max', 'dias_em_atraso_mob7': 'max', 'dias_em_atraso_mob8': 'max',
            'dias_em_atraso_mob9': 'max', 'dias_em_atraso_mob10': 'max', 'dias_em_atraso_mob11': 'max', 'dias_em_atraso_mob12': 'max',

            'valor_vencido_mob1': 'sum', 'valor_vencido_mob2': 'sum', 'valor_vencido_mob3': 'sum', 'valor_vencido_mob4': 'sum',
            'valor_vencido_mob5': 'sum', 'valor_vencido_mob6': 'sum', 'valor_vencido_mob7': 'sum', 'valor_vencido_mob8': 'sum',
            'valor_vencido_mob9': 'sum', 'valor_vencido_mob10': 'sum', 'valor_vencido_mob11': 'sum', 'valor_vencido_mob12': 'sum'
        })
    )

    df_mobs.rename(columns={'safra_concessao': 'fechamento'}, inplace=True)

    df_mobs['fechamento'] = pd.to_datetime(df_mobs['fechamento'])




    # DEFININDO VARIAVEIS DOS FECHAMENTOS (VOP, A VENCER, CARTEIRA)

    fechamentos = pd.date_range(start='2022-06-30', end=pd.Timestamp.now().replace(day=1) + pd.offsets.MonthEnd(1), freq='M')
    hoje = pd.Timestamp.now()

    # VERIFICA SE O MÊS ATUAL JÁ FECHOU
    if hoje.day != pd.Timestamp.now().days_in_month:
        # SUBSTITUI O ÚLTIMO FECHAMENTO PELA DATA DE HOJE
        fechamentos = fechamentos[:-1].append(pd.DatetimeIndex([hoje]))
    print(f"Quantidade de fechamento: {fechamentos.shape[0]}")
    fechamentos = fechamentos.normalize()

    print(fechamentos)



    ## FUNÇÃO PARA CALCULAR VARIÁVEIS

    def calcular_variaveis_fechamento (df_titulos, fechamento):

        # VARIÁVEIS COMUNITÁRIAS
        primeiro_dia_mes = fechamento.replace(day=1)

        # VOP ACUMULADO
        titulos_vop_acum = df_titulos[(df_titulos['data_efetivacao'] <= fechamento)].copy()
        
        vop_cliente_acum = titulos_vop_acum.groupby(['raiz_cnpj'])['valor_face'].sum().reset_index().rename(columns={'valor_face': 'vop_acumulado'})
        
        vop_cliente_acum['fechamento'] = fechamento

        
        # VOP SAFRA
        titulos_vop_mes = df_titulos[
                                    (df_titulos['data_efetivacao'] <= fechamento) &
                                    (df_titulos['data_efetivacao'] >= primeiro_dia_mes)
                                ].copy()
        
        vop_cliente_mes = titulos_vop_mes.groupby(['raiz_cnpj'])['valor_face'].sum().reset_index().rename(columns={'valor_face': 'vop_mes'})
        
        vop_cliente_mes['fechamento'] = fechamento


        # VOP A VENCER SAFRA
        titulos_vop_a_vencer_mes = df_titulos[
                                            (df_titulos['data_vencimento'] > fechamento) & 
                                            ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > fechamento)) &
                                            (df_titulos['data_efetivacao'] <= fechamento) &
                                            (df_titulos['data_efetivacao'] >= primeiro_dia_mes)
                                        ]
        
        vop_a_vencer_mes = titulos_vop_a_vencer_mes.groupby(['raiz_cnpj'])['valor_face'].sum().reset_index().rename(columns={'valor_face': 'vop_a_vencer_mes'})
        
        vop_a_vencer_mes['fechamento'] = fechamento


        # CRUZANDO OS DFS
        dfs = [
            vop_cliente_acum,
            vop_cliente_mes,
            vop_a_vencer_mes
        ]
        
        df_merged = reduce(
            lambda left, right: pd.merge(left, right, on=['raiz_cnpj', 'fechamento'], how='left'),
            dfs
        )
        
        return df_merged


    # CRIANDO DF PARA GUARDAR SAÍDA DA FUNÇÃO
    df_final = pd.DataFrame()

    # LOOP PARA CALCULAR SAFRA A SAFRA
    for fechamento in fechamentos:
        df_fechamento = calcular_variaveis_fechamento(df_titulos, fechamento)
        df_final = pd.concat([df_final, df_fechamento], ignore_index=True)
    
    df_final = df_final.fillna(0)



    # BASE CARTEIRA

    query_carteira =    f"""
                        select
                                safra as fechamento
                                , substring(regexp_replace(cnpj_sacado, '[./-]', ''), 1, 8) as raiz_cnpj
                                , sum(carteira) as carteira_mes
                        from deltalakerefined.payments.carteira_vendermais
                        group by 1, 2
                        """

    df_carteira = execute_query(conn, query_carteira)
    print(f"Quantidade de linhas no DataFrame 'carteira': {df_carteira.shape[0]}")



    df_carteira['fechamento'] = df_carteira['fechamento'].astype(str).str[:10]
    df_final['fechamento'] = df_final['fechamento'].astype(str).str[:10]
    df_mobs['fechamento'] = df_mobs['fechamento'].astype(str).str[:10]


    # CRUZANDO DFS FINAIS

    df_final = (
        df_final
        .merge(df_carteira, on=['raiz_cnpj', 'fechamento'], how='left')
        .merge(df_mobs, on=['raiz_cnpj', 'fechamento'], how='left')
    )


    df_final = df_final.fillna(0)

    df_final['dias_em_atraso_mob1']  = df_final['dias_em_atraso_mob1'].astype(int)
    df_final['dias_em_atraso_mob2']  = df_final['dias_em_atraso_mob2'].astype(int)
    df_final['dias_em_atraso_mob3']  = df_final['dias_em_atraso_mob3'].astype(int)
    df_final['dias_em_atraso_mob4']  = df_final['dias_em_atraso_mob4'].astype(int)
    df_final['dias_em_atraso_mob5']  = df_final['dias_em_atraso_mob5'].astype(int)
    df_final['dias_em_atraso_mob6']  = df_final['dias_em_atraso_mob6'].astype(int)
    df_final['dias_em_atraso_mob7']  = df_final['dias_em_atraso_mob7'].astype(int)
    df_final['dias_em_atraso_mob8']  = df_final['dias_em_atraso_mob8'].astype(int)
    df_final['dias_em_atraso_mob9']  = df_final['dias_em_atraso_mob9'].astype(int)
    df_final['dias_em_atraso_mob10'] = df_final['dias_em_atraso_mob10'].astype(int)
    df_final['dias_em_atraso_mob11'] = df_final['dias_em_atraso_mob11'].astype(int)
    df_final['dias_em_atraso_mob12'] = df_final['dias_em_atraso_mob12'].astype(int)


    df_final['fechamento'] = pd.to_datetime(df_final['fechamento']).dt.date


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day


    cols = ['fechamento', 'raiz_cnpj'] + [c for c in df_final.columns if c not in ['fechamento', 'raiz_cnpj']]
    df_final = df_final[cols]

    df_final = df_final.reset_index(drop=True)

    print('Exportando base para Refined...')

    # Exportando dados para a camada Refined
    # # Conectando na Trusted
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
        BUCKET_SOURCE_REFINED = "performance"
        FOLDER_DESTINATION_REFINED = "visao_mob"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            df_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite",
            #overwrite_schema=True
        )
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")