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


def boletos_raw_to_refined_carteira(access_params=None,  **kwargs):

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
    
    # Base Boletos
    query_boleto = """
    select * from deltalaketrusted.payments.boletos_internos
    """
    boleto = execute_query(conn, query_boleto)
    print(f"Quantidade de linhas no DataFrame 'boleto': {boleto.shape[0]}")

    df_titulos = boleto



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



    ### Criando funções
    # Criando função de carteira
    def calcular_carteira_no_fechamento(df_titulos, fechamento):
        # Um título faz parte da carteira se:
        # - Ele não foi pago até a data do fechamento OU a data de pagamento for depois do fechamento
        # - O título foi emitido até a data do fechamento
        
        # Condição 1: Títulos que não foram pagos até o fechamento ou ainda não foram pagos
        titulos_validos = df_titulos[(df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > fechamento)]
        
        # Condição 2: O título foi emitido até a data do fechamento
        titulos_validos = titulos_validos[titulos_validos['data_efetivacao'] <= fechamento]
        
        # Agrupando por cedente/cliente
        carteira_cliente = titulos_validos.groupby(['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente'])['valor_face'].sum().reset_index()
        
        # Adicionar a data de fechamento na coluna para identificar
        carteira_cliente['fechamento'] = fechamento
        return carteira_cliente
    
    # Criando DataFrame para armazenar a carteira de cada cliente em cada fechamento
    carteira_historica = pd.DataFrame()

    # Calculando a carteira para cada fechamento
    for fechamento in fechamentos:
        carteira_no_fechamento = calcular_carteira_no_fechamento(df_titulos, fechamento)
        carteira_historica = pd.concat([carteira_historica, carteira_no_fechamento], ignore_index=True)



    # Criando função de carteira vencida
    def calcular_carteira_vencida_no_fechamento(df_titulos, fechamento):
        # Um titulo será considerado como carteira vencida se:
        # - Ele não foi pago até a data do fechamento OU a data de pagamento for depois do fechamento
        # - A data de vencimento esteja dentro do período do fechamento

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
        carteira_vencida_cliente = agrupado.pivot_table(index=['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente'], 
                                                        columns='faixa_vencido', 
                                                        values='valor_face', 
                                                        aggfunc='sum', 
                                                        fill_value=0).reset_index()

        # Adicionar a coluna "Total Vencido" somando todas as colunas de faixas
        carteira_vencida_cliente['Total Vencido'] = carteira_vencida_cliente.filter(like='Vencido').sum(axis=1)
        
        # Adicionar a data de fechamento como coluna de identificação
        carteira_vencida_cliente['fechamento'] = fechamento

        return carteira_vencida_cliente
    
    # Criando DataFrame para armazenar a carteira vencida de cada cliente em cada fechamento
    carteira_vencida = pd.DataFrame()

    # Calculando a carteira vencida para cada fechamento
    for fechamento in fechamentos:
        carteira_no_fechamento = calcular_carteira_vencida_no_fechamento(df_titulos, fechamento)
        carteira_vencida = pd.concat([carteira_vencida, carteira_no_fechamento], ignore_index=True)

    # Renomeando colunas
    carteira_vencida = carteira_vencida.rename(columns={
        'Vencido +0 meses': 'Atraso até 30 dias',
        'Vencido +1 meses': 'Atraso de 31 a 60 dias',
        'Vencido +2 meses': 'Atraso de 61 a 90 dias',
        'Vencido +3 meses': 'Atraso de 91 a 120 dias',
        'Vencido +4 meses': 'Atraso de 121 a 150 dias',
        'Vencido +5 meses': 'Atraso de 151 a 180 dias',
        'Vencido +6 meses ou mais': 'Atraso acima de 180 dias'
    })

    # Criar a coluna 'Over 30', que é a soma de todas as colunas com atraso acima de 30 dias
    carteira_vencida['Over 30'] = carteira_vencida[
        ['Atraso de 31 a 60 dias', 'Atraso de 61 a 90 dias', 'Atraso de 91 a 120 dias',
        'Atraso de 121 a 150 dias', 'Atraso de 151 a 180 dias', 'Atraso acima de 180 dias']
    ].sum(axis=1)

    # Criar a coluna 'Over 60', que é a soma de todas as colunas com atraso acima de 60 dias
    carteira_vencida['Over 60'] = carteira_vencida[
        ['Atraso de 61 a 90 dias', 'Atraso de 91 a 120 dias',
        'Atraso de 121 a 150 dias', 'Atraso de 151 a 180 dias', 'Atraso acima de 180 dias']
    ].sum(axis=1)

    # Criar a coluna 'Over 90', que é a soma de todas as colunas com atraso acima de 90 dias
    carteira_vencida['Over 90'] = carteira_vencida[
        ['Atraso de 91 a 120 dias', 'Atraso de 121 a 150 dias',
        'Atraso de 151 a 180 dias', 'Atraso acima de 180 dias']
    ].sum(axis=1)



    # Criando função de carteira a vencer
    def calcular_carteira_a_vencer(df_titulos, fechamento):      
        # Condição para títulos a vencer:
        # - Data de vencimento é após o fechamento
        # - E o título não foi pago antes do fechamento (data_baixa nula ou posterior ao fechamento)
        titulos_a_vencer = df_titulos[
            (df_titulos['data_vencimento'] >= fechamento) & 
            ((df_titulos['data_baixa'].isna()) | (df_titulos['data_baixa'] > fechamento)) &
            (df_titulos['data_efetivacao'] <= fechamento)
        ]
        
        # Agrupando títulos a vencer por cliente e cedente
        carteira_a_vencer = titulos_a_vencer.groupby(['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente'])['valor_face'].sum().reset_index()
        carteira_a_vencer.rename(columns={'valor_face': 'Total a Vencer'}, inplace=True)
        
        
        # Retornar o DataFrame completo
        return carteira_a_vencer
    
    # Inicializar um DataFrame vazio para armazenar a carteira a vencer
    carteira_a_vencer = pd.DataFrame()

    # Passo 5: Calcular a carteira para cada fechamento
    for fechamento in fechamentos:
        carteira_no_fechamento = calcular_carteira_a_vencer(df_titulos, fechamento)
        
        # Adicionar a coluna do fechamento correspondente em cada iteração
        carteira_no_fechamento['fechamento'] = fechamento
        
        # Concatenar os resultados ao DataFrame principal
        carteira_a_vencer = pd.concat([carteira_a_vencer, carteira_no_fechamento], ignore_index=True)



    ### Cruzando todos os df's
        
    # Realizar o merge da carteira histórica e carteira vencida
    df_intermediario = pd.merge(carteira_historica, carteira_vencida, 
                                on=['nome_sacado', 'nome_cedente', 'cnpj_sacado', 'cnpj_cedente', 'fechamento'], 
                                how='outer')

    # Realizar o merge do DataFrame intermediário com a carteira a vencer
    df_final = pd.merge(df_intermediario, carteira_a_vencer, 
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
        'valor_face': 'carteira',
        'Total a Vencer': 'carteira_em_dia',
        'Total Vencido': 'carteira_vencida',
        'Total a Vencer Mês Anterior': 'carteira_em_dia_mes_anterior',
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
    FOLDER_DESTINATION_REFINED = "carteira_vendermais"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        df_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )