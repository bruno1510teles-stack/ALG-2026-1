# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np
import calendar

def fechamento_hemera_refined(access_params=None,  **kwargs):

    # Coletando dados da camada Trusted
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


    # Data de hoje
    hoje = datetime.today()

    if hoje.day == 1:
        if hoje.month == 1:
            ano = hoje.year - 1
            mes = 12
        else:
            ano = hoje.year
            mes = hoje.month - 1
    else:
        ano = hoje.year
        mes = hoje.month

    ultimo_dia_mes = calendar.monthrange(ano, mes)[1]
    fim = datetime(ano, mes, ultimo_dia_mes)

    print(f"Ultimo fechamento considerado: {fim.date()}")

    # Geração da lista completa de 'YYYY-MM'
    para_fechamento = pd.date_range(start='2024-01-01', end=fim, freq='M').strftime('%Y-%m').tolist()


    lista_dfs = []

    for mes_ano in para_fechamento:

            # Derivar o último dia do mês automaticamente
        ano, mes = map(int, mes_ano.split('-'))
        ultimo_dia = calendar.monthrange(ano, mes)[1]
        data_fechamento = f"{mes_ano}-{ultimo_dia:02d}"

        print(f'\nIniciando coleta de Dados Hemera para a data: {data_fechamento}')


        query_hemera =  f"""
            WITH estoque_detalhado AS (
                SELECT 
                    est.*, 
                    coalesce(ret.valor_retorno, 0) AS valor_retorno, 
                    ret.data_lancamento AS data_lancamento_retorno,
                    coalesce(rec.valor_recompra, 0) AS valor_recompra, 
                    rec.data_lancamento AS data_lancamento_recompra
                FROM deltalakerefined.hemera_refined.estoque est
                    LEFT JOIN deltalakerefined.hemera_refined.retorno ret
                        ON est.id_titulo = ret.id_titulo AND est.data_referencia = substr(CAST(ret.data_fechamento AS VARCHAR), 1, 7)
                    LEFT JOIN deltalakerefined.hemera_refined.recompra rec
                        ON est.id_titulo = rec.id_titulo AND est.data_referencia = substr(CAST(rec.data_fechamento AS VARCHAR), 1, 7)
            )
            SELECT 
                ed.*, 
                (
                    SELECT SUM(r.valor_recompra)
                    FROM deltalakerefined.hemera_refined.recompra r
                    WHERE r.id_titulo = ed.id_titulo 
                    AND CAST(r.data_fechamento AS DATE) <= DATE '{data_fechamento}'
                ) AS valor_recompra_acumulada
            FROM 
                estoque_detalhado ed
            WHERE ed.data_referencia = '{mes_ano}'
        """

        hemera = execute_query(conn, query_hemera)

        print(f"Data: {mes_ano} — Linhas retornadas: {hemera.shape[0]}")
        df_hemera = hemera.copy()

        print('Dados da hemera coletado com sucesso!')
        

        # Extraindo os CNPJs do DataFrame 'df_hemera' e convertendo-os para uma lista
        titulos = df_hemera['numero_titulo'].unique().tolist()
        # Convertendo a lista para uma string no formato adequado para o SQL
        titulos_str = ', '.join([f"'{cnpj}'" for cnpj in titulos])


        query_cedente = f"""
                    select 
                        concat(LPAD(numero_titulo , 10, '0'), cnpj_sacado) as chave, nome_cedente as nome_cedente_query
                    from 
                        deltalaketrusted.payments.boletos_internos 
                    where 
                        LPAD(numero_titulo , 10, '0') IN ({titulos_str})
                """

        cedente = execute_query(conn, query_cedente)
        print('Dados de cedente coletado com sucesso!')
        print(f"Quantidade de linhas no DataFrame 'cedente': {cedente.shape[0]}")



        print('Iniciando coleta de pagamentos')
        query_pagamento = f"""
            select 
                concat(lpad(bt.numero_titulo,10, '0'),s.numero_cnpj_sacado_formatado) as chave, coalesce(sum(dt.valor),0) as valor_descontado_nota_fn
            from 
                postgres.ccred_schema_prd_default.credito_libra cl
                inner join postgres.ccred_schema_prd_default.credito_boleto cb on cl.id = cb.credito_libra_id
                inner join postgres.ccred_schema_prd_default.boleto_titulo bt on cb.boleto_titulo_id = bt.id
                inner join postgres.ccred_schema_prd_default.desconto_titulo dt on cb.id = dt.credito_boleto_id
                inner join postgres.ccred_schema_prd_default.sacado s on bt.sacado_id = s.id
            where 
                dt.tipo_desconto_id in (1,2,3,14)
                and LPAD(bt.numero_titulo , 10, '0') IN ({titulos_str})
            group by
                concat(lpad(bt.numero_titulo,10, '0'),s.numero_cnpj_sacado_formatado)
        """

        pagamento = execute_query(conn, query_pagamento)
        print('Dados de pagamentos coletado com sucesso!')
        print(f"Quantidade de linhas no DataFrame 'pagamentos': {pagamento.shape[0]}")



        # Trazendo o nome do cedente
        df_hemera['chave'] = df_hemera['numero_titulo'].astype(str) + df_hemera['cnpj_sacado'].astype(str)
        df_hemera = pd.merge(df_hemera, cedente, on='chave', how='left')
        df_hemera = pd.merge(df_hemera, pagamento, on='chave', how='left')
        df_hemera.reset_index(drop=True, inplace=True)


        print('Iniciando coleta de Dados da Selic')

        # Trazendo dados da SELIC
        client = Minio(
            access_params['endpoint_url_raw'],
            access_key=access_params['aws_access_key_id_raw'],
            secret_key=access_params['aws_secret_access_key_raw'],
		)


        # Definindo bucket e caminho do arquivo
        BUCKET_SOURCE_RAW = "auxiliares"
        FOLDER_DESTINATION_RAW = 'selic'
        file_name = 'AUXILIAR_SELIC.xlsx'
        file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'

        try:
            # Obtendo o arquivo do MinIO
            response = client.get_object(BUCKET_SOURCE_RAW, file_path)
            
            # Lendo os dados
            file_data = BytesIO(response.read())
            df_selic = pd.read_excel(file_data)
            
            # Fechando o response para liberar recursos
            response.close()
            response.release_conn()
            
            # Exibindo e confirmando a coleta dos dados
            print('Dados da Selic coletados com sucesso!')

        except Exception as e:
            print(f"Erro ao coletar dados da Selic: {e}")

        print('Todos Dados para tratamento coletados com sucesso!!')

        ## Iniciando tratamento
        print('Iniciando tratamento')

        df_selic = df_selic[['DATA', 'FATOR_DIARIO_VENDERMAIS', 'FATOR_DIARIO_TRADICIONAL', 'FATOR_DIARIO_CONGLOMERADO']]

        print(df_selic)



        # Criando colunas
        df_hemera['delta_pdd'] = df_hemera['pdd_final'] - df_hemera['pdd_inicial']

        df_hemera['data_baixa'] = np.where(
            df_hemera['valor_retorno'] != 0,  # Condição
            df_hemera['data_lancamento_retorno'],  # Se a condição for verdadeira
            df_hemera['data_lancamento_recompra']  # Se a condição for falsa
        )

        # Regra do valor_aquisição
        df_hemera['data_fechamento'] = pd.to_datetime(df_hemera['data_fechamento'], errors='coerce')
        df_hemera['data_aquisicao'] = pd.to_datetime(df_hemera['data_aquisicao'], errors='coerce')
        df_hemera['primeiro_dia_mes'] = df_hemera['data_fechamento'].apply(lambda x: x.replace(day=1))
        df_hemera['primeiro_dia_mes'] = pd.to_datetime(df_hemera['primeiro_dia_mes'], errors='coerce')
        df_hemera['data_baixa'] = pd.to_datetime(df_hemera['data_baixa'])
        df_hemera['primeiro_dia_mes'] = df_hemera['primeiro_dia_mes'].dt.tz_localize(None)
        df_hemera['data_fechamento'] = df_hemera['data_fechamento'].dt.tz_localize(None)


        df_hemera['valor_aquisicao_ok'] = np.where(
            (df_hemera['data_aquisicao'] >= df_hemera['primeiro_dia_mes']) & 
            (df_hemera['data_aquisicao'] <= df_hemera['data_fechamento']),
            df_hemera['valor_aquisicao'],
            0
        )


        df_hemera['valor_recompra_acumulada'] = np.where(
            df_hemera['valor_retorno'] != 0,
            0,
            np.where(
                (df_hemera['cnpj_cedente'] == '35.914.008/0001-48') & (df_hemera['data_aquisicao'] < pd.to_datetime('2024-10-18')),
                (np.maximum(df_hemera['valor_retorno'], df_hemera['valor_recompra'])),
                df_hemera['valor_recompra_acumulada']
            )
        )    

        df_hemera['valor_baixa'] = df_hemera['valor_retorno'] + df_hemera['valor_recompra']


        # Criando condições para cálculo do spread_bruto_fidc
        # Cálculos intermediários para legibilidade
        condicao1 = df_hemera['estoque_valor_presente_final'] - df_hemera['estoque_valor_presente_inicial']
        condicao2 = df_hemera['estoque_valor_presente_final']  - df_hemera['valor_aquisicao_ok'] 
        condicao3 = df_hemera['valor_baixa'] - df_hemera['valor_aquisicao_ok'] 
        condicao4 = df_hemera['valor_retorno'] - df_hemera['estoque_valor_presente_inicial']
        condicao5 = df_hemera[['valor_retorno', 'valor_recompra_acumulada', 'valor_recompra']].max(axis=1) - df_hemera['estoque_valor_presente_inicial']
        condicao6 = df_hemera[['valor_recompra_acumulada', 'valor_recompra']].max(axis=1) - df_hemera['estoque_valor_presente_inicial']
        # Aplicação do np.where com as variáveis
        df_hemera['spread_bruto_fidc'] = np.where(
            (df_hemera['estoque_valor_presente_final'] > 0) & (df_hemera['estoque_valor_presente_inicial'] > 0),
            condicao1,
            np.where(
                ((df_hemera['estoque_valor_presente_inicial'] == 0) | (df_hemera['estoque_valor_presente_inicial'].isna())) & (df_hemera['estoque_valor_presente_final'] > 0),
                condicao2,
                np.where(
                    ((df_hemera['estoque_valor_presente_inicial'] == 0) | (df_hemera['estoque_valor_presente_inicial'].isna())) & ((df_hemera['estoque_valor_presente_final'] == 0) | (df_hemera['estoque_valor_presente_final'].isna())),
                    condicao3,
                    np.where(
                        ((df_hemera['estoque_valor_presente_final'] == 0) | (df_hemera['estoque_valor_presente_final'].isna())) & ((df_hemera['valor_recompra_acumulada'] == 0) | (df_hemera['valor_recompra_acumulada'].isna())) & ((df_hemera['valor_recompra'] == 0) | (df_hemera['valor_recompra'].isna())),
                        condicao4,
                        np.where(
                            (df_hemera['cnpj_cedente'] == '35.914.008/0001-48') & (df_hemera['data_aquisicao'] < pd.to_datetime('2024-10-18')),
                            condicao6,
                            condicao5
                            )
                        )
                    )
                )
            )               


        df_hemera['spread_bruto_percentual'] = df_hemera['spread_bruto_fidc'] / df_hemera['valor_aquisicao']


        df_hemera['data_inicial_funding'] = np.where(
            df_hemera['estoque_valor_presente_inicial'] > 0,
            df_hemera['primeiro_dia_mes'],
            df_hemera['data_aquisicao']
        )

        df_hemera['data_final_funding'] = np.where(
            df_hemera['estoque_valor_presente_final'] > 0,
            df_hemera['data_fechamento'],
            df_hemera['data_baixa']
            )


        df_hemera['produto'] = df_hemera['grupo'].str.upper()


        # Trazendo dados referente a selic
        df_hemera['data_final_funding'] = pd.to_datetime(df_hemera['data_final_funding'], errors='coerce')
        df_hemera['data_inicial_funding'] = pd.to_datetime(df_hemera['data_inicial_funding'], errors='coerce')
        df_selic['DATA'] = pd.to_datetime(df_selic['DATA'], errors='coerce')
        df_hemera = df_hemera.merge(df_selic, how='left', left_on='data_inicial_funding', right_on='DATA', suffixes=('', '_inicial'))
        df_hemera = df_hemera.merge(df_selic, how='left', left_on='data_final_funding', right_on='DATA', suffixes=('', '_final'))


        # Tratando para cálculo
        df_hemera['taxa_selic_inicio'] = np.where(
            df_hemera['produto'] == 'VENDERMAIS',
            df_hemera['FATOR_DIARIO_VENDERMAIS'],
            np.where(
                df_hemera['produto'] == 'CONGLOMERADO',
                df_hemera['FATOR_DIARIO_CONGLOMERADO'],
                df_hemera['FATOR_DIARIO_TRADICIONAL']
            )
        )

        df_hemera['taxa_selic_final'] = np.where(
            df_hemera['produto'] == 'VENDERMAIS',
            df_hemera['FATOR_DIARIO_VENDERMAIS_final'],
            np.where(
                df_hemera['produto'] == 'CONGLOMERADO',
                df_hemera['FATOR_DIARIO_CONGLOMERADO_final'],
                df_hemera['FATOR_DIARIO_TRADICIONAL_final']
            )
        )



        # Calculando Funding
        df_hemera['taxa_selic_final'] = pd.to_numeric(df_hemera['taxa_selic_final'], errors='coerce') / 100
        df_hemera['taxa_selic_inicio'] = pd.to_numeric(df_hemera['taxa_selic_inicio'], errors='coerce') / 100
        df_hemera['taxa_funding'] = (df_hemera['taxa_selic_final'] / df_hemera['taxa_selic_inicio']) - 1
        df_hemera['data_vencimento'] = pd.to_datetime(df_hemera['data_vencimento'], errors='coerce')
        df_hemera['data_fechamento'] = pd.to_datetime(df_hemera['data_fechamento'], errors='coerce')
        df_hemera['valor_funding'] = np.where(
            (df_hemera['data_vencimento'] + pd.Timedelta(days=60)) < df_hemera['data_fechamento'],
            0,
            df_hemera['taxa_funding'] * df_hemera['valor_aquisicao']
        )

        # Incluindo campo valor_funding_simulado (Solicitação Alexia)
        df_hemera['valor_funding_simulado'] = np.where(
            (df_hemera['data_vencimento'] + pd.Timedelta(days=60)) < df_hemera['data_fechamento'],
            0,
            df_hemera['taxa_funding'] * df_hemera['estoque_valor_presente_inicial'])


        df_hemera['spread_liquido_fidc_valor'] = df_hemera['spread_bruto_fidc'] - df_hemera['valor_funding'] - df_hemera['delta_pdd']
        df_hemera['spread_liquido_fidc_percentual'] = (df_hemera['spread_liquido_fidc_valor'] / df_hemera['valor_nominal'])
        df_hemera['descritivo_pdd'] = np.where(
            df_hemera['delta_pdd'] > 0,
            'saida',
            'entrada'
        )

        df_hemera['data_vencimento'] = pd.to_datetime(df_hemera['data_vencimento'])
        df_hemera['prazo_operacao'] = (df_hemera['data_vencimento'] - df_hemera['data_aquisicao']).dt.days
        df_hemera['prazo_medio_ponderado'] = (df_hemera['prazo_operacao'] * df_hemera['valor_nominal'])
        total_peso = df_hemera['valor_nominal'].sum()
        df_hemera['prazo_medio_ponderado'] = df_hemera['prazo_medio_ponderado'] / total_peso

        # Definindo nome Cedente
        df_hemera['cedente'] = np.where(
            df_hemera['nome_cedente'] == 'ALPE INTERMEDIACAO DE NEGOCIOS S.A.',
            df_hemera['nome_cedente_query'],
            df_hemera['nome_cedente']
        )


        # Pegando spread da Alpe inter
        # Converte as colunas envolvidas para float
        df_hemera['valor_aquisicao'] = df_hemera['valor_aquisicao'].astype(float)
        df_hemera['valor_nominal_original'] = df_hemera['valor_nominal'].astype(float)
        df_hemera['valor_descontado_nota_fn'] = df_hemera['valor_descontado_nota_fn'].astype(float)
        # Esse cálculo "df_hemera['valornominaloriginal'] - df_hemera['valor_descontado_nota_fn']" é para saber o valor pago de fato
        df_hemera['aux_spread_alpe_inter'] = df_hemera['valor_aquisicao'] - (df_hemera['valor_nominal_original'] - df_hemera['valor_descontado_nota_fn'])

        df_hemera['spread_alpe_inter'] = np.where(
            (df_hemera['data_aquisicao'] >= df_hemera['primeiro_dia_mes']) & 
            (df_hemera['data_aquisicao'] <= df_hemera['data_fechamento']),
            df_hemera['aux_spread_alpe_inter'],
            0
        )



        df_hemera['multa_e_juros'] = np.where(
            df_hemera['valor_baixa'] == 0,
            0, 
            df_hemera['valor_baixa'] - df_hemera['valor_nominal_original']
        )

        df_hemera['aquisicoes_baixa'] = df_hemera['valor_aquisicao_ok'] - df_hemera['valor_baixa']


        # Atribuindo data
        now = datetime.now(tz=timezone(timedelta(hours=-3)))
        df_hemera['atualizado_em'] = now.strftime('%Y-%m-%d %X')



        # Tirando indice
        df_hemera.reset_index(drop=True, inplace=True)

        # Função para converter as colunas para datetime e ajustar fuso horário
        def converter_para_datetime_e_ajustar_fuso(df, colunas, formato='%Y-%m-%d', fuso_local='America/Sao_Paulo'):
            for coluna in colunas:
                if coluna in df.columns:
                    try:
                        # Converte a coluna para datetime
                        df[coluna] = pd.to_datetime(df[coluna], format=formato, errors='coerce')
                        
                        # Aplica o fuso horário de São Paulo e converte para UTC
                        df[coluna] = df[coluna].dt.tz_localize(fuso_local, ambiguous='NaT', nonexistent='NaT').dt.tz_convert('UTC')
                        
                        # Remove a parte do tempo, mantendo apenas a data
                        df[coluna] = df[coluna].dt.strftime('%Y-%m-%d')

                        # Verifica se há valores nulos após a conversão
                        if df[coluna].isnull().any():
                            print(f"Alguns valores na coluna '{coluna}' não puderam ser convertidos ou ajustados para o fuso horário.")
                    except Exception as e:
                        print(f"Erro ao converter a coluna '{coluna}': {e}")
                else:
                    print(f"A coluna '{coluna}' não está presente no DataFrame.")
            
            return df

        # Colunas que serão convertidas
        colunas_para_converter_datetime = [
            'data_fechamento', 'data_aquisicao', 'data_vencimento', 
            'data_lancamento_recompra', 'data_lancamento_retorno', 
            'data_baixa', 'data_inicial_funding', 'data_final_funding'
        ]

        # Aplicando a conversão e ajuste de fuso
        df_hemera = converter_para_datetime_e_ajustar_fuso(df_hemera, colunas_para_converter_datetime)


        df_hemera = df_hemera.drop(columns=['valor_aquisicao'])


        df_hemera['pdd_vencido'] = np.where(
                (df_hemera['data_vencimento'] < df_hemera['data_fechamento']) &
                df_hemera['delta_pdd'] > 0,
                df_hemera['delta_pdd'],
                0
            )
        
        df_hemera['dias_vencidos_pdd_vencido'] = np.where(
            df_hemera['pdd_vencido'] != 0,
            (pd.to_datetime(df_hemera['data_fechamento']) - pd.to_datetime(df_hemera['data_vencimento'])).dt.days,
            0
        )

        df_hemera['aliquota_pdd_inicial'] = np.where(
            df_hemera['estoque_valor_presente_inicial'] != 0,
            df_hemera['pdd_inicial'] / df_hemera['estoque_valor_presente_inicial'],
            0
        )

        df_hemera['flag_vencido_inicial'] = np.where(
            df_hemera['aliquota_pdd_inicial'] > 0.01,
            1,
            0
        )


        # Condições baseadas na lógica do Excel
        condicoes = [
            (df_hemera['produto'] == "TRADICIONAL"),
            (df_hemera['data_vencimento'] > df_hemera['data_fechamento']) | ((df_hemera['estoque_valor_presente_final'] == 0) & (df_hemera['flag_vencido_inicial'] == 0)),
            (df_hemera['pdd_final'] > df_hemera['pdd_inicial'] * 1.05) & (df_hemera['dias_vencidos_pdd_vencido'] > 20),
            (df_hemera['pdd_final'] < df_hemera['pdd_inicial']) & (df_hemera['flag_vencido_inicial'] == 1)
        ]

        # Resultados correspondentes a cada condição
        resultados = [
            "00 - TRADICIONAL",
            "01 - CARTEIRA EM DIA",
            "02 - AUMENTO VENCIDO",
            "03 - REDUÇÃO VENCIDO"
        ]

        # Resultado padrão caso nenhuma condição seja satisfeita
        resultado_default = "04 - MANUTENÇÃO VALOR PDD (+-5%)"

        # Aplicar as condições no DataFrame
        df_hemera['cluster_pdd'] = np.select(condicoes, resultados, default=resultado_default)



        ## Filtrando apenas colunas necessárias e renomando-as
        df_hemera.rename(columns={
            'valor_aquisicao_ok': 'valor_aquisicao'
        }, inplace=True)

        # Filtrando as colunas desejadas

        df_hemera['anomes_fechamento'] = pd.to_datetime(df_hemera['data_fechamento']).dt.strftime('%Y-%m')

        colunas_desejadas_analitico = ['produto', 'anomes_fechamento', 'data_fechamento', 'cedente', 'cnpj_sacado', 'nome_sacado', 'data_referencia', 'cluster_pdd', # Info básica
                                    'id_titulo', 'numero_titulo', # Info títulos                  
                                    'data_aquisicao', 'data_vencimento', 'data_lancamento_recompra', 
                                    'data_lancamento_retorno', 'data_baixa', 'data_inicial_funding', 'data_final_funding', # Datas
                                    'estoque_valor_presente_inicial', 'estoque_valor_presente_final', 'pdd_inicial', 
                                    'delta_pdd', 'pdd_final', 'valor_retorno', 'valor_recompra', 'valor_recompra_acumulada','valor_baixa','valor_nominal_original', # Valores
                                    'valor_descontado_nota_fn', 'multa_e_juros', 'valor_aquisicao', 'spread_bruto_fidc', 
                                    'spread_bruto_percentual', 'valor_funding', 'valor_funding_simulado', 'spread_liquido_fidc_valor', 
                                    'spread_alpe_inter', 'taxa_selic_inicio', 'taxa_selic_final', 'taxa_funding', # Percentuais
                                    'spread_liquido_fidc_percentual', 'prazo_operacao', 'prazo_medio_ponderado', 'atualizado_em'] # Prazo

        # Aplicando a função para renomear as colunas no DataFrame
        df_analitico = df_hemera[colunas_desejadas_analitico].copy()

        # Padronizando Outputs
        # Datas
        df_analitico['data_referencia'] = pd.to_datetime(df_analitico['data_referencia']).dt.strftime('%Y-%m-%d')
        df_analitico['data_fechamento'] = pd.to_datetime(df_analitico['data_fechamento']).dt.strftime('%Y-%m-%d')
        df_analitico['data_aquisicao'] = pd.to_datetime(df_analitico['data_aquisicao']).dt.strftime('%Y-%m-%d')
        df_analitico['data_vencimento'] = pd.to_datetime(df_analitico['data_vencimento']).dt.strftime('%Y-%m-%d')
        df_analitico['data_lancamento_recompra'] = pd.to_datetime(df_analitico['data_lancamento_recompra']).dt.strftime('%Y-%m-%d')
        df_analitico['data_lancamento_retorno'] = pd.to_datetime(df_analitico['data_lancamento_retorno']).dt.strftime('%Y-%m-%d')
        df_analitico['data_baixa'] = pd.to_datetime(df_analitico['data_baixa']).dt.strftime('%Y-%m-%d')
        df_analitico['data_inicial_funding'] = pd.to_datetime(df_analitico['data_inicial_funding']).dt.strftime('%Y-%m-%d')
        df_analitico['data_final_funding'] = pd.to_datetime(df_analitico['data_final_funding']).dt.strftime('%Y-%m-%d')
        df_analitico['atualizado_em'] = pd.to_datetime(df_analitico['atualizado_em']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Strings
        df_analitico['cluster_pdd'] = df_analitico['cluster_pdd'].astype(str)
        df_analitico['produto'] = df_analitico['produto'].astype(str)
        df_analitico['cedente'] = df_analitico['cedente'].astype(str)
        df_analitico['cnpj_sacado'] = df_analitico['cnpj_sacado'].astype(str).str.zfill(14)  # CNPJ com 14 dígitos
        df_analitico['nome_sacado'] = df_analitico['nome_sacado'].astype(str)
        df_analitico['id_titulo'] = df_analitico['id_titulo'].astype(str).str.zfill(10)  # Completando com zeros à esquerda
        df_analitico['numero_titulo'] = df_analitico['numero_titulo'].astype(str)


        # Extrair o ano e o mês da coluna formatada como string
        df_analitico['year'] = df_analitico['data_fechamento'].str[:4]
        df_analitico['month'] = df_analitico['data_fechamento'].str[5:7]


        # Numéricas
        colunas_valores = ['estoque_valor_presente_inicial', 'estoque_valor_presente_final', 'pdd_inicial', 
                        'delta_pdd', 'pdd_final', 'valor_retorno', 'valor_recompra', 'valor_recompra_acumulada', 'valor_baixa', 
                        'valor_nominal_original', 'valor_descontado_nota_fn', 'multa_e_juros', 
                        'valor_aquisicao', 'spread_bruto_fidc', 'spread_liquido_fidc_valor', 'valor_funding', 'valor_funding_simulado',
                        'prazo_operacao', 'prazo_medio_ponderado']

        df_analitico[colunas_valores] = df_analitico[colunas_valores].apply(pd.to_numeric, errors='coerce').round(2)

        total_colunas = df_analitico[['estoque_valor_presente_inicial', 'estoque_valor_presente_final', 'valor_aquisicao', 'valor_baixa', 'spread_bruto_fidc', 'valor_funding', 'delta_pdd', 'spread_liquido_fidc_valor', 'spread_alpe_inter']].sum()
        total_colunas = total_colunas.apply(lambda x: f"{x:,.2f}")
        print(total_colunas)


        # Armazena o resultado na lista
        lista_dfs.append(df_analitico)


    # ➕ Agrega tudo em um único DataFrame
    df_hemera_final = pd.concat(lista_dfs, ignore_index=True)


    print(f"\nConsulta final consolidada: {df_hemera_final.shape[0]} linhas no total.")


    print('Salvando dados na Refined...')

    storage_options_refined = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "hemera-refined"
    FOLDER_DESTINATION_REFINED = "fechamento/delta"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_hemera_final,
        storage_options=storage_options_refined,
        mode="overwrite"
    )
 
 
    print('Arquivo salvo com sucesso!')