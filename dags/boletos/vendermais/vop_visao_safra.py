# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import numpy as np
import re
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from io import BytesIO
import re

def vop_visao_safra(access_params=None,  **kwargs):

    # Conectando no Trino e validando

    # Coletando dados da camada Trusted
    # Conectando com o banco
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)

    print('Importação da base...')

    query_boletos_trusted =  f"""
                            select *
                            from deltalaketrusted.payments.boletos_internos
                            """

    df = execute_query(conn, query_boletos_trusted)


    df_titulos = df.copy()


    ### Definindo fechamentos de safra
    fechamentos = pd.date_range(start='2022-06-30', end=pd.Timestamp.now().replace(day=1) + pd.offsets.MonthEnd(1), freq='M')
        # Captura a data atual
    hoje = pd.Timestamp.now()

    # Verifica se o mês atual já fechou
    if hoje.day != pd.Timestamp.now().days_in_month:
        # Substitui o último fechamento pela data atual
        fechamentos = fechamentos[:-1].append(pd.DatetimeIndex([hoje]))
    print(f"Quantidade de fechamento: {fechamentos.shape[0]}")
    fechamentos = fechamentos.normalize()
    print(fechamentos)


    ### Tratando colunas de data
    df_titulos['data_emissao'] = pd.to_datetime(df_titulos['data_emissao'])
    df_titulos['data_efetivacao'] = pd.to_datetime(df_titulos['data_efetivacao'])
    df_titulos['data_vencimento'] = pd.to_datetime(df_titulos['data_vencimento'])
    df_titulos['data_baixa'] = pd.to_datetime(df_titulos['data_baixa'], errors='coerce')



    #### VOP

    def calcular_vop_no_fechamento(df_titulos, fechamento):

        titulos_validos = df_titulos[(df_titulos['data_efetivacao'] <= fechamento)].copy()
        
        # Agrupando por cedente/cliente
        vop_cliente = titulos_validos.groupby(['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente'])['valor_face'].sum().reset_index()
        
        # Adicionar a data de fechamento na coluna para identificar
        vop_cliente['fechamento'] = fechamento
        return vop_cliente

    # Criando DataFrame para armazenar a carteira de cada cliente em cada fechamento
    vop_historica = pd.DataFrame()

    # Calculando a carteira para cada fechamento
    for fechamento in fechamentos:
        vop_no_fechamento = calcular_vop_no_fechamento(df_titulos, fechamento)
        vop_historica = pd.concat([vop_historica, vop_no_fechamento], ignore_index=True)




    #### VOP VENCIDO

    def calcular_vop_vencido_no_fechamento(df_titulos, fechamento):


        # Condição 1: Títulos que não foram pagos até o fechamento ou ainda não foram pagos
        titulos_validos = df_titulos[(df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > fechamento)]

        # Condição 2: Data de vencimento esteja dentro do período do fechamento
        titulos_validos = titulos_validos[titulos_validos['data_vencimento'] < fechamento]

        # Calcular a diferença de dias entre a data de vencimento e a data de fechamento
        titulos_validos['dias_vencido'] = (fechamento - titulos_validos['data_vencimento']).dt.days

        # Transformar a diferença em meses fracionados
        titulos_validos['meses_vencido'] = titulos_validos['dias_vencido'] // 30  # Aproximação de meses com 30 dias

        # Limitar a diferença de meses até 6 meses, e colocar "6+" para valores maiores que 5
        titulos_validos['faixa_vencido'] = np.where(titulos_validos['meses_vencido'] > 5, 
                                                    'Vencido +6 meses ou mais', 
                                                    'Vencido +' + titulos_validos['meses_vencido'].astype(str) + ' meses')

        # Agrupando por sacado/cedente/faixa
        agrupado = titulos_validos.groupby(['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente', 'faixa_vencido'])['valor_face'].sum().reset_index()

        # Usando pivot_table para transformar faixas em colunas
        vop_vencido_cliente = agrupado.pivot_table(index=['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente'], 
                                                        columns='faixa_vencido', 
                                                        values='valor_face', 
                                                        aggfunc='sum', 
                                                        fill_value=0).reset_index()

        # Adicionar a coluna "Total Vencido" somando todas as colunas de faixas
        vop_vencido_cliente['Total Vencido'] = vop_vencido_cliente.filter(like='Vencido').sum(axis=1)
        
        # Adicionar a data de fechamento como coluna de identificação
        vop_vencido_cliente['fechamento'] = fechamento

        return vop_vencido_cliente

    # Criando DataFrame para armazenar o vop vencido de cada cliente em cada fechamento
    vop_vencido = pd.DataFrame()

    # Calculando a carteira vencida para cada fechamento
    for fechamento in fechamentos:
        vop_no_fechamento = calcular_vop_vencido_no_fechamento(df_titulos, fechamento)
        vop_vencido = pd.concat([vop_vencido, vop_no_fechamento], ignore_index=True)


    # Renomeando colunas
    vop_vencido = vop_vencido.rename(columns={
        'Vencido +0 meses': 'Atraso até 30 dias',
        'Vencido +1 meses': 'Atraso de 31 a 60 dias',
        'Vencido +2 meses': 'Atraso de 61 a 90 dias',
        'Vencido +3 meses': 'Atraso de 91 a 120 dias',
        'Vencido +4 meses': 'Atraso de 121 a 150 dias',
        'Vencido +5 meses': 'Atraso de 151 a 180 dias',
        'Vencido +6 meses ou mais': 'Atraso acima de 180 dias'
    })


    # Criar a coluna 'Over 30', que é a soma de todas as colunas com atraso acima de 30 dias
    vop_vencido['Over 30'] = vop_vencido[
        ['Atraso de 31 a 60 dias', 'Atraso de 61 a 90 dias', 'Atraso de 91 a 120 dias',
        'Atraso de 121 a 150 dias', 'Atraso de 151 a 180 dias', 'Atraso acima de 180 dias']
    ].sum(axis=1)

    # Criar a coluna 'Over 60', que é a soma de todas as colunas com atraso acima de 60 dias
    vop_vencido['Over 60'] = vop_vencido[
        ['Atraso de 61 a 90 dias', 'Atraso de 91 a 120 dias',
        'Atraso de 121 a 150 dias', 'Atraso de 151 a 180 dias', 'Atraso acima de 180 dias']
    ].sum(axis=1)

    # Criar a coluna 'Over 90', que é a soma de todas as colunas com atraso acima de 90 dias
    vop_vencido['Over 90'] = vop_vencido[
        ['Atraso de 91 a 120 dias', 'Atraso de 121 a 150 dias',
        'Atraso de 151 a 180 dias', 'Atraso acima de 180 dias']
    ].sum(axis=1)



    ### VOP A VENCER

    def calcular_vop_a_vencer(df_titulos, fechamento):      
        # Condição para títulos a vencer:
        # - Data de vencimento é após o fechamento
        # - E o título não foi pago antes do fechamento (data_baixa nula ou posterior ao fechamento)
        titulos_a_vencer = df_titulos[
            (df_titulos['data_vencimento'] >= fechamento) & 
            ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > fechamento)) &
            (df_titulos['data_efetivacao'] <= fechamento)
        ]
        
        # Agrupando títulos a vencer por cliente e cedente
        vop_a_vencer = titulos_a_vencer.groupby(['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente'])['valor_face'].sum().reset_index()
        vop_a_vencer.rename(columns={'valor_face': 'Total a Vencer'}, inplace=True)
        
        
        # Retornar o DataFrame completo
        return vop_a_vencer

    # Inicializar um DataFrame vazio para armazenar o vop a vencer
    vop_a_vencer = pd.DataFrame()

    # Passo 5: Calcular o vop para cada fechamento
    for fechamento in fechamentos:
        vop_no_fechamento = calcular_vop_a_vencer(df_titulos, fechamento)
        
        # Adicionar a coluna do fechamento correspondente em cada iteração
        vop_no_fechamento['fechamento'] = fechamento
        
        # Concatenar os resultados ao DataFrame principal
        vop_a_vencer = pd.concat([vop_a_vencer, vop_no_fechamento], ignore_index=True)


    
    ### Cruzando todos os df's
    
    # Realizar o merge da vop histórico e vop vencido
    df_intermediario = pd.merge(vop_historica, vop_vencido, 
                                on=['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente', 'fechamento'], 
                                how='outer')

    # Realizar o merge do DataFrame intermediário com a carteira a vencer
    df_final = pd.merge(df_intermediario, vop_a_vencer, 
                        on=['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente', 'fechamento'], 
                        how='outer')


    # Converter as colunas envolvidas para o mesmo tipo (float)
    df_final['Total Vencido'] = df_final['Total Vencido'].astype(float)
    df_final['Total a Vencer'] = df_final['Total a Vencer'].astype(float)




    ### Tratamentos para df_final
    # Lógica para calcular 'VAGAO OVER X'
    df_final['VAGAO OVER 1'] = np.where(df_final['Total Vencido'].fillna(0) > 0, 
                                        df_final['Total Vencido'].fillna(0) + df_final['Total a Vencer'].fillna(0), 
                                        df_final['Total Vencido'].fillna(0))

    # Lógica para calcular 'VAGAO OVER 30' 
    df_final['VAGAO OVER 30'] = np.where(df_final['Over 30'].fillna(0) > 0, 
                                        df_final['Total Vencido'].fillna(0) + df_final['Total a Vencer'].fillna(0), 
                                        df_final['Over 30'].fillna(0))

    # Lógica para calcular 'VAGAO OVER 60' 
    df_final['VAGAO OVER 60'] = np.where(df_final['Over 60'].fillna(0) > 0, 
                                        df_final['Total Vencido'].fillna(0) + df_final['Total a Vencer'].fillna(0), 
                                        df_final['Over 60'].fillna(0))

    # Lógica para calcular 'VAGAO OVER 90'
    df_final['VAGAO OVER 90'] = np.where(df_final['Over 90'].fillna(0) > 0, 
                                        df_final['Total Vencido'].fillna(0) + df_final['Total a Vencer'].fillna(0), 
                                        df_final['Over 90'].fillna(0))
    




    # Calculo para rolagens
    def calcular_rolagens(df):
        # Ordenar o DataFrame por 'nome_sacado', 'nome_cedente' e 'fechamento'
        df = df.sort_values(by=['nome_sacado', 'nome_cedente', 'fechamento'])

        # 1ª ROLAGEM já calculada
        df['Total a Vencer Mês Anterior'] = df.groupby(['nome_sacado', 'nome_cedente'])['Total a Vencer'].shift(1)
        df['1ª ROLAGEM'] = df.apply(
            lambda row: float(row['Atraso até 30 dias']) / float(row['Total a Vencer Mês Anterior']) if row['Total a Vencer Mês Anterior'] != 0 else 0,
            axis=1
        )
        
        # 2ª ROLAGEM: Vencido de 31 a 60 dias no próximo mês / Atraso até 30 dias no mês atual
        df['Atraso de 31 a 60 dias Próximo Mês'] = df.groupby(['nome_sacado', 'nome_cedente'])['Atraso de 31 a 60 dias'].shift(-1)
        df['2ª ROLAGEM'] = df.apply(
            lambda row: float(row['Atraso de 31 a 60 dias Próximo Mês']) / float(row['Atraso até 30 dias']) if row['Atraso até 30 dias'] != 0 else 0,
            axis=1
        )

        # 3ª a 6ª ROLAGEM: Vencido de 151 a 180 dias daqui a 5 meses / Vencido de 31 a 60 dias no mês seguinte
        df['Atraso de 151 a 180 dias X5'] = df.groupby(['nome_sacado', 'nome_cedente'])['Atraso de 151 a 180 dias'].shift(-5)
        df['3ª a 6ª ROLAGEM'] = df.apply(
            lambda row: float(row['Atraso de 151 a 180 dias X5']) / float(row['Atraso de 31 a 60 dias Próximo Mês']) if row['Atraso de 31 a 60 dias Próximo Mês'] != 0 else 0,
            axis=1
        )

        # MULT ROLAGEM: Vencido de 151 a 180 dias daqui a 5 meses / Total a Vencer do mês anterior
        df['MULT ROLAGEM'] = df.apply(
            lambda row: float(row['Atraso de 151 a 180 dias X5']) / float(row['Total a Vencer Mês Anterior']) if row['Total a Vencer Mês Anterior'] != 0 else 0,
            axis=1
        )
        
        # Substituir NaN por 0 para as colunas de rolagem
        df[['1ª ROLAGEM', '2ª ROLAGEM', '3ª a 6ª ROLAGEM', 'MULT ROLAGEM']] = df[['1ª ROLAGEM', '2ª ROLAGEM', '3ª a 6ª ROLAGEM', 'MULT ROLAGEM']].fillna(0)

        return df

    # Aplicar a função ao DataFrame final consolidado
    df_final = calcular_rolagens(df_final)



    ### Selecionando as colunas relevantes
    df_final = df_final[[
        'fechamento', 'nome_sacado', 'cnpj_sacado', 'nome_cedente', 'cnpj_cedente', 'valor_face', 'Total a Vencer', 
        'Total Vencido', 'Atraso até 30 dias', 'Atraso de 31 a 60 dias',
        'Atraso de 61 a 90 dias', 'Atraso de 91 a 120 dias',
        'Atraso de 121 a 150 dias', 'Atraso de 151 a 180 dias',
        'Atraso acima de 180 dias', 'Over 30', 'Over 60', 'Over 90',
        'VAGAO OVER 1', 'VAGAO OVER 30', 'VAGAO OVER 60',
        'VAGAO OVER 90', 'Total a Vencer Mês Anterior', 
        'Atraso de 31 a 60 dias Próximo Mês', 
        'Atraso de 151 a 180 dias X5'
    ]].reset_index(drop=True)



    ### Renomeando colunas
    df_final = df_final.rename(columns={
        'fechamento': 'safra',
        'valor_face': 'vop',
        'Total a Vencer': 'vop_a_vencer',
        'Total Vencido': 'vop_vencido',
        'Total a Vencer Mês Anterior': 'vop_a_vencer_mes_anterior',
        'Atraso de 31 a 60 dias Próximo Mês' : 'Atraso de 31 a 60 dias mes posterior',
        'Atraso de 151 a 180 dias X5': 'Atraso de 151 a 180 dias mes X5'
    })

    def converter_para_datetime(df, colunas, formato='%Y-%m-%d'):
        for coluna in colunas:
            df[coluna] = pd.to_datetime(df[coluna], format=formato)
            df[coluna] = df[coluna].dt.date
        return df

    colunas_para_converter_datetime = ['safra']
    df_final = converter_para_datetime(df_final, colunas_para_converter_datetime)



    # Função para padronizar os nomes das colunas e tabela
    def padronizar_nome_coluna(coluna):
        # Substituir caracteres especiais por letras correspondentes
        coluna = re.sub(r'[áàãâä]', 'a', coluna)
        coluna = re.sub(r'[éèêë]', 'e', coluna)
        coluna = re.sub(r'[íìîï]', 'i', coluna)
        coluna = re.sub(r'[óòõôö]', 'o', coluna)
        coluna = re.sub(r'[úùûü]', 'u', coluna)
        coluna = re.sub(r'[ç]', 'c', coluna)
        
        # Substituir espaços por underscore
        coluna = coluna.replace(' ', '_')
        
        # Converter para minúsculas
        coluna = coluna.lower()
        
        return coluna

    # Aplicar a função em todas as colunas do DataFrame
    df_final.columns = [padronizar_nome_coluna(coluna) for coluna in df_final.columns]

    df_final.fillna(0, inplace=True)

    df_final['vop'] = df_final['vop'].astype(float)
    df_final['vop_a_vencer'] = df_final['vop_a_vencer'].astype(float)

    # Agora faça a subtração
    df_final['vop_performado'] = df_final['vop'] - df_final['vop_a_vencer']


    # Padronizando coluna valores
    colunas_valores = ['vop','vop_a_vencer','vop_performado','vop_vencido',	'atraso_ate_30_dias',	'atraso_de_31_a_60_dias',	'atraso_de_61_a_90_dias',	'atraso_de_91_a_120_dias',	
                    'atraso_de_121_a_150_dias',	'atraso_de_151_a_180_dias',	'atraso_acima_de_180_dias',	'over_30',	'over_60',	'over_90',	'vagao_over_1',	'vagao_over_30',	
                    'vagao_over_60',	'vagao_over_90',	'vop_a_vencer_mes_anterior',	'atraso_de_31_a_60_dias_mes_posterior',	'atraso_de_151_a_180_dias_mes_x5']

    df_final[colunas_valores] = df_final[colunas_valores].apply(pd.to_numeric, errors='coerce').round(2)


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day



    # Exportando dados para a camada Refined
 
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "payments"
    FOLDER_DESTINATION_REFINED = "vop_visao_safra"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        df_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )