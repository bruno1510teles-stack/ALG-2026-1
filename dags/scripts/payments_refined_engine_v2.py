import pandas as pd
from datetime import date, datetime, timedelta, timezone
import numpy as np

def transform_payments_engine_v2(hpex_trusted):

    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    ## Tratando Hpex
    # Eliminando duplicatas

    # ordenado variaveis
    hpex_trusted_ordenada = hpex_trusted.sort_values(['data_hp', 'data_emissao', 'data_pagamento',  'data_vencimento','numero_parcela'], ascending=True)

    # eliminando duplicatas
    hpex_trusted_ordenada.drop_duplicates(keep = 'last', inplace = True)
    hpex_trusted_distintas = hpex_trusted_ordenada[~hpex_trusted_ordenada.duplicated(['documento', 'numero_titulo', 'fornecedor','numero_parcela'], keep='last')]

    # Removendo valores negativos de boletos
    hpex_trusted_distintas = hpex_trusted_distintas[hpex_trusted_distintas['valor_titulo'] > 0]

    # Criando CNPJ_RAIZ
    hpex_trusted_distintas.loc[hpex_trusted_distintas['tipo_documento'] == 'CNPJ', 'documento_raiz'] = hpex_trusted_distintas.loc[hpex_trusted_distintas['tipo_documento'] == 'CNPJ', 'documento'].str.slice(0, 8)
    hpex_trusted_distintas.loc[hpex_trusted_distintas['tipo_documento'] != 'CNPJ', 'documento_raiz'] = hpex_trusted_distintas.loc[hpex_trusted_distintas['tipo_documento'] != 'CNPJ', 'documento']

    # #### Ajustando coluna de parcelas nulas
    # Substituindo os valores de 0 ou nulo para 1, para seguir a mesma logica na hora de verificar se é parcela única ou não
    # ###### OBS: Para tomar essa decisão foi verificado se isso impactava na precisão da criação das parcelas lógicas (sessão abaixo), como foi observado que não segui-se com essa tratativa
    hpex_trusted_distintas.loc[:,'numero_parcela'] = hpex_trusted_distintas.loc[:,'numero_parcela'].replace(0, 1)
    hpex_trusted_distintas.loc[:,'numero_parcela'] = hpex_trusted_distintas.loc[:,'numero_parcela'].replace(np.nan, 1)

    # ##### REGRA PARCELA POR FORNECEDOR

    # ###### cadubo = número título e documento iguais
    # ###### casal = número título e documento iguais 
    # ###### megaleste = o Core do número título é igual e o documento também (41770-11 -> 41770)
    # ###### cecil = número título e documento iguais 
    # ###### unigres = documento e numero_titulo iguais (quando a parcela vem como 0 a compra foi a vista)
    # ###### ciacor = documento é igual e core também (000000052393A-NF -> 000000052393)
    # ###### arcelor = documento e numero_titulo iguais
    # ###### discor = documento e numero_titulo iguais
    # ###### ciadastintas = documento é igual e core também (000000052393A-NF -> 000000052393)
    # ###### gama = documento é igual e core também (000000052393A-NF -> 000000052393)
    # ###### multypet = documento e data de emissão é igual e numero_titulo, com excessão dos dois últimos que dizem respeito a parcela, são iguais (parcela_1 = 53559602; parcela_2 = 53559603, parcela_3 = 53559604)

    def TratandoParecelaNula(df):
        
        fornecedores_nulos = df['fornecedor'].unique()
        
        fornecedores_esperados = ['cadubo', 'casal', 'ciacor', 'megaleste', 'cecil', 'unigres', 'arcelor', 'discor', 'ciadastintas', 'gama', 'multypet', 'alpe']
        
        #Verifica se os forneceores nulos estão nas regras de tratativa, caso não estejam ele quebra
        if set(fornecedores_nulos) - set(fornecedores_esperados) == set():
            
            parcela_nula = df

            #Tratando numeros titulo que são iguais quando parcelados
            mask1 = parcela_nula['fornecedor'].isin(['cadubo', 'casal', 'cecil', 'arcelor', 'discor', 'unigres'])
            parcela_nula.loc[mask1, 'core_numero_titulo'] = parcela_nula.loc[mask1, 'numero_titulo']

            #Tratando caso MegaLeste (contem core do título)
            mask2 = parcela_nula['fornecedor'] == 'megaleste'
            parcela_nula.loc[mask2, 'core_numero_titulo'] = parcela_nula.loc[mask2, 'numero_titulo'].str.split('-').str[0]

            #Tratando caso Gama e Cia das Tintas (contem core do título)
            mask3 = parcela_nula['fornecedor'].isin(['gama', 'ciadastintas', 'ciacor'])
            parcela_nula.loc[mask3, 'core_numero_titulo'] = parcela_nula.loc[mask3, 'numero_titulo'].str[:-4]

            #Tratando caso Multypet (contem core do título)
            mask4 = parcela_nula['fornecedor'] == 'multypet'
            parcela_nula.loc[mask4, 'core_numero_titulo'] = parcela_nula.loc[mask4, 'numero_titulo'].str[:-2]

            parcela_nula.loc[:,'parcela_logica'] = parcela_nula.groupby(['fornecedor', 'documento', 'core_numero_titulo'])['data_vencimento'].rank(method='first')

            df.loc[:,'parcela_logica'] = parcela_nula.loc[:,'parcela_logica']
            
            return df
        
        else:
            raise print('ERRO: EXISTEM FORNECEDORES QUE NÃO ESTÃO COM AS REGRAS DE TRATATIVA DE PARCELA NULA DE DEFINIDA!!!')

    hpex_trusted_distintas = TratandoParecelaNula(hpex_trusted_distintas)

    # TESTE PARA VERIFICAR A EFICIENCIA DO MODELO DE TRATAMENTO DE PARCELAS LÓGICAS

    # Aplicando o modelo no conjunto de dados do dia 27/10/2023:

    parcela_nula = hpex_trusted_distintas

    #Tratando numeros titulo que são iguais quando parcelados
    mask1 = parcela_nula['fornecedor'].isin(['cadubo', 'casal', 'cecil', 'arcelor', 'discor', 'unigres'])
    parcela_nula.loc[mask1, 'core_numero_titulo'] = parcela_nula.loc[mask1, 'numero_titulo']

    #Tratando caso MegaLeste (contem core do título)
    mask2 = parcela_nula['fornecedor'] == 'megaleste'
    parcela_nula.loc[mask2, 'core_numero_titulo'] = parcela_nula.loc[mask2, 'numero_titulo'].str.split('-').str[0]

    #Tratando caso Gama e Cia das Tintas (contem core do título)
    mask3 = parcela_nula['fornecedor'].isin(['gama', 'ciadastintas', 'ciacor'])
    parcela_nula.loc[mask3, 'core_numero_titulo'] = parcela_nula.loc[mask3, 'numero_titulo'].str[:-4]

    #Tratando caso Multypet (contem core do título)
    mask4 = parcela_nula['fornecedor'] == 'multypet'
    parcela_nula.loc[mask4, 'core_numero_titulo'] = parcela_nula.loc[mask4, 'numero_titulo'].str[:-2]

    parcela_nula.loc[:,'parcela_logica'] = parcela_nula.groupby(['fornecedor', 'documento', 'core_numero_titulo'])['data_vencimento'].rank(method='first')


    # Filtrando aqueles que já vieram com valores não nulos para comparar com o modelo

    df_naoNulos = parcela_nula[(~parcela_nula['numero_parcela'].isnull()) & (parcela_nula['numero_parcela'] > 0)]

    df_tratando = df_naoNulos[~(df_naoNulos['numero_parcela'] == df_naoNulos['parcela_logica'])]

    # Fazendo a contagem dos valores que não batem temos 2080 (df_tratando)
    # E a contagem total de valores não nulos de parecela oficial é de 1560600 (df_naoNulos)
    # Fazendo a matemágica temos que ((2080/1253391) * 100) == 0.16% de erro 
    # E avaliando os erros foi possivel verificar que os valores que não batem, no geral, são excessão da regra de data de vencimento, no qual datas de vencimento maiores as vezes são para parcelas menores, que provavelmente são renegociações de parcelas em atraso.

    # Verificando outliers nas datas 

    colunas_verificar_outliers = ['data_emissao', 'data_vencimento', 'data_pagamento', 'data_hp']
    outliers_geral = pd.DataFrame(columns=hpex_trusted.columns)
    for coluna in colunas_verificar_outliers:
        Q1 = hpex_trusted[coluna].quantile(0.25)
        Q3 = hpex_trusted[coluna].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 3 * IQR
        upper_bound = Q3 + 3 * IQR

        outliers = hpex_trusted[(hpex_trusted[coluna] < lower_bound) | (hpex_trusted[coluna] > upper_bound)] 
        outliers['origem'] = coluna
        
        outliers_geral = pd.concat([outliers_geral, outliers], ignore_index=True)
        
        outliers_geral

    # Eliminando valores em que o valor do título é negativo (impacta nas medidas e provelmente é devolução)

    hpex_trusted_distintas = hpex_trusted_distintas[hpex_trusted_distintas['valor_titulo'] >= 0]

    # Criando função de Vop Acumulado pela Raiz
    def VopAcumulado(df, meses):    

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
            
        # Soma vop
        
        soma_vop = hpex_vop_acumulado.groupby('documento_raiz')['valor_titulo'].sum()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'VOP{meses}M']

        return df_saida

    def VopAVista(df, meses):
    
        # Mantem apenas os valores com parcela única
        df = df[~df.duplicated(['fornecedor', 'documento', 'core_numero_titulo'], keep = False)]    
        
        # Mantem apenas os valores com prazo de até 10 dias entre emissão e vencimento
        df = df[(df['data_vencimento'] - df['data_emissao']) <= '10 days']

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
            
        # Soma vop
        
        soma_vop = hpex_vop_acumulado.groupby('documento_raiz')['valor_titulo'].sum()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'VOPAVista{meses}M']

        return df_saida

    def VopAPrazo(df, meses):
    
        # Mantem apenas os valores com parcela única
        df = df[~df.duplicated(['fornecedor', 'documento', 'core_numero_titulo'], keep = False)]    
        
        # Mantem apenas os valores com prazo de até 10 dias entre emissão e vencimento
        df = df[(df['data_vencimento'] - df['data_emissao']) > '10 days']

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
    
        # Soma vop
        
        soma_vop = hpex_vop_acumulado.groupby('documento_raiz')['valor_titulo'].sum()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'VOPAPrazo{meses}M']

        return df_saida

    def VopAPrazoComParcelamento(df, meses):
    
        # Mantem apenas os valores parcelados
        df = df[df.duplicated(['fornecedor', 'documento', 'core_numero_titulo'], keep = False)]    
        
        # Mantem apenas os valores com prazo maior que 10 dias entre emissão e vencimento
        df = df[(df['data_vencimento'] - df['data_emissao']) > '10 days']

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
    
        # Soma vop
        
        soma_vop = hpex_vop_acumulado.groupby('documento_raiz')['valor_titulo'].sum()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'VOPAPrazoParcelado{meses}M']

        return df_saida

    def VopMediaMensal(df, meses):    

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
    
        # Soma vop
        
        soma_vop = hpex_vop_acumulado.groupby('documento_raiz')['valor_titulo'].sum() / meses
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'MediaVOP{meses}M']

        return df_saida

    def VopMaxMensal(df, meses):   
        
        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
    
        # Soma vop

        hpex_vop_acumulado['mes'] = hpex_vop_acumulado['data_emissao'].dt.month
        soma_vop = hpex_vop_acumulado.groupby(['mes', 'documento_raiz'])['valor_titulo'].sum().groupby('documento_raiz').max()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'MaxVOP{meses}M']

        return df_saida

    def MesMaxVop(df, meses = 12):   
        
        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]

        # Soma vop

        hpex_vop_acumulado['mes'] = hpex_vop_acumulado['data_emissao'].dt.month
        soma_vop = hpex_vop_acumulado.groupby(['mes', 'documento_raiz'])['valor_titulo'].sum().groupby(['documento_raiz']).idxmax()
        soma_vop = soma_vop.str[0]

        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'MesMaxVop{meses}M']

        return df_saida

    def QtdTitulos(df, meses):    

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
    
        # Soma vop
        # O Ticket Médio é de acordo com as notas fiscais e não de acordo com os títulos (que podem ser referentes a parcelas), por isso do agrupamento anterior
        
        soma_vop = hpex_vop_acumulado.groupby(['core_numero_titulo', 'documento_raiz', 'fornecedor'])['valor_titulo'].sum().groupby('documento_raiz').count()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'QtdTitulos{meses}M']

        return df_saida

    def TicketMedio(df, meses):    

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
    
        # Soma vop
        # O Ticket Médio é de acordo com as notas fiscais e não de acordo com os títulos (que podem ser referentes a parcelas), por isso do agrupamento anterior
        
        soma_vop = hpex_vop_acumulado.groupby(['core_numero_titulo', 'documento_raiz', 'fornecedor'])['valor_titulo'].sum().groupby('documento_raiz').mean()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'TicketMedio{meses}M']

        return df_saida

    def MediaDifDiasFaturamento(df, meses):    

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
    
        # Soma vop
        
        # Pega apenas as primeiras parcela (ou seja apenas as que são a vista, ou apenas 1 vez as parceladas)

        hpex_notafiscal_ordenada = hpex_vop_acumulado[hpex_vop_acumulado['parcela_logica'] == 1].sort_values(['fornecedor', 'documento_raiz', 'data_emissao'], ascending = True)
        hpex_notafiscal_ordenada['prox_data_emissao'] = hpex_notafiscal_ordenada.groupby(['fornecedor','documento_raiz'])['data_emissao'].shift(-1)
        hpex_notafiscal_ordenada['diferenca_dias'] = (hpex_notafiscal_ordenada['prox_data_emissao'] - hpex_notafiscal_ordenada['data_emissao']).dt.days
        soma_vop = hpex_notafiscal_ordenada.groupby('documento_raiz')['diferenca_dias'].mean()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'MediaDifDiasFaturamento{meses}M']

        return df_saida

    def PrazoMedio(df, meses):    

        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
    
        # Soma vop

        hpex_vop_acumulado['dias'] = hpex_vop_acumulado['data_vencimento'] - hpex_vop_acumulado['data_emissao']
        soma_vop = hpex_vop_acumulado.groupby('documento_raiz')['dias'].mean().dt.days
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'PrazoMedio{meses}M']

        return df_saida

    def Ever(df, dias):
        
        # Filtra pela quantidade de dias desejada
        # Temos alguns títulos que foram enviados com a data de pagamento anterior a data de emissão, como são poucos (2330 no dia 30/10/2023) assumiremos eles como 0 de diferença de dias, ou seja, foram pagos no prazo.
        
        filter_pagamento_nao_nulo = ~df['data_pagamento'].isna()
        hpex_pagos = df[filter_pagamento_nao_nulo]
        filter_ever = (hpex_pagos['data_pagamento'] - hpex_pagos['data_vencimento']).dt.days >= dias
        hpex_ever = hpex_pagos[filter_ever]    
        
        soma_vop = hpex_ever.groupby('documento_raiz')['valor_titulo'].count()
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'Ever{dias}']

        return df_saida

    def Over(df, dias):
        
        # Filtra pela quantidade de dias desejada
        # Temos alguns títulos que foram enviados com a data de pagamento anterior a data de emissão, como são poucos (2330 no dia 30/10/2023) assumiremos eles como 0 de diferença de dias, ou seja, foram pagos no prazo.
        
        filter_pagamento_nulo = df['data_pagamento'].isna()
        hpex_pagos = df[filter_pagamento_nulo]
        filter_over = (hpex_pagos['data_hp'] - hpex_pagos['data_vencimento']).dt.days >= dias
        hpex_ever = hpex_pagos[filter_over]    

        soma_vop = hpex_ever.groupby('documento_raiz')['valor_titulo'].count()
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'Over{dias}']

        return df_saida

    def QtdDiasMaxPagamentoAtrasado(df, meses):
        
        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
        
        #Calcula a quantidade máxima de dias que um pagamento ficou em atraso
        
        filtroPagamentoNulo = (~hpex_vop_acumulado['data_pagamento'].isna())
        hpex_vop_acumulado.loc[filtroPagamentoNulo, 'dias_atraso'] = hpex_vop_acumulado.loc[filtroPagamentoNulo, 'data_pagamento'] - hpex_vop_acumulado.loc[filtroPagamentoNulo, 'data_vencimento']
        
        soma_vop = hpex_vop_acumulado[filtroPagamentoNulo].groupby('documento_raiz')['dias_atraso'].max()
        
        df_saida = pd.DataFrame(soma_vop)
        df_saida.columns = [f'QtdeDiasMaxPagamentoAtrasado{meses}M']
        
        return df_saida

    def PercentPagoEmDia(df, meses):
        
        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
        
        # Calcula o percentual pago em dia
        
        filtroPagos = (~hpex_vop_acumulado['data_pagamento'].isna())
        df_pagos = hpex_vop_acumulado[filtroPagos]
        
        # Pagos em até 5 dias após a data de vencimento são considerados pagos em dia
        df_pagos['IsPagoEmDia'] = (df_pagos['data_pagamento'] <= (df_pagos['data_vencimento'] + pd.offsets.Day(5)))

        PercentPagoEmDia = (df_pagos[df_pagos['IsPagoEmDia']].groupby('documento_raiz').size() / df_pagos.groupby('documento_raiz').size()).fillna(0)
        
        df_saida = pd.DataFrame(PercentPagoEmDia)
        df_saida.columns = [f'PercentualPagoEmDia{meses}M']
        
        return df_saida
    
    def PercentMedAlavancagemPeriodo(df, meses):
        
        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
        
        #Compila os valores por data de emissao e vencimento
        emissao = hpex_vop_acumulado[['documento_raiz', 'data_emissao', 'valor_titulo']]
        vencimento = hpex_vop_acumulado[['documento_raiz', 'data_vencimento', 'valor_titulo']]
        
        #Deixa as colnas com o nome certo para posterior concat
        emissao.columns = ['documento_raiz', 'data', 'valor_titulo_fat']    
        vencimento.columns = ['documento_raiz', 'data', 'valor_titulo_venc']
        
        df = pd.concat([emissao, vencimento])
        
        #Soma os valores que são do mesmo dia e do mesmo documento e posteriormente faz um acumulativo pelas datas
        df_acumulado = df.groupby(['documento_raiz', 'data']).sum().groupby(['documento_raiz']).cumsum().reset_index()
        
        #Realiza o calculo de porcentagem
        df_acumulado['Alavancagem'] = (df_acumulado['valor_titulo_venc']/df_acumulado['valor_titulo_fat'])
        
        #Traz a média de alavancagem diaria do EC
        AlavancagemPeriodo = df_acumulado.groupby('documento_raiz')['Alavancagem'].mean()
        
        df_saida = pd.DataFrame(AlavancagemPeriodo)
        df_saida.columns = [f'PercentualMedioDeAlavancagemPeriodo{meses}M']
        
        return df_saida

    def PercentMedAlavancagemFinal(df, meses):
        
        # Filtra pela quantidade de meses desejada

        #    O offsets.MonthBegin e o offsets.MonthEnd se comportam de forma diferente caso o dia seja o primeiro ou último dia do mês e esses filtros abaixo servem para esses casos
        DateInitEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthBegin(0))
        DateEndEqual = (df['data_hp'] == df['data_hp'] + pd.offsets.MonthEnd(0))

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data inicial do mês
        df.loc[DateInitEqual, 'data_inicial'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateInitEqual, 'data_final'] = df.loc[DateInitEqual, 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP não for igual a data inicial ou final do mês
        df.loc[~(DateInitEqual | DateEndEqual), 'data_inicial'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthBegin(-meses-1)
        df.loc[~(DateInitEqual | DateEndEqual), 'data_final'] = df.loc[~(DateInitEqual | DateEndEqual), 'data_hp'] + pd.offsets.MonthEnd(-1)

        #    Aplica o tratamento correto para os casos em que a data da HP for igual a data final do mês
        df.loc[DateEndEqual, 'data_inicial'] = df.loc[DateEndEqual, 'data_hp'] + pd.offsets.MonthBegin(-meses)
        df.loc[DateEndEqual, 'data_final'] = df.loc[DateEndEqual, 'data_hp']

        hpex_vop_acumulado = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]
        
        
        FiltroVencido = hpex_vop_acumulado['data_vencimento'] < hpex_vop_acumulado['data_hp']
        AlavancagemFinal = (hpex_vop_acumulado.loc[FiltroVencido,:].groupby('documento_raiz')['valor_titulo'].sum() / hpex_vop_acumulado.groupby('documento_raiz')['valor_titulo'].sum()).fillna(0)
            
        df_saida = pd.DataFrame(AlavancagemFinal)
        df_saida.columns = [f'PercentualMedioDeAlavancagemFinal{meses}M']
        
        
        return df_saida
        
    # ### --- COMPILANDO VARIÁVEIS ---

    boletos_refined = pd.DataFrame(hpex_trusted_distintas['documento_raiz'].unique(), columns = ['documento_raiz'])

    ## CRIANDO DF COM AS COLUNAS
    VOP_3M = VopAcumulado(hpex_trusted_distintas, 3)
    VOP_A_VISTA3M = VopAVista(hpex_trusted_distintas, 3)
    VOP_A_PRAZO_3M = VopAPrazo(hpex_trusted_distintas, 3)
    VOP_A_PRAZO_PARCELADO_3M = VopAPrazoComParcelamento(hpex_trusted_distintas, 3)
    MEDIA_VOP_3M = VopMediaMensal(hpex_trusted_distintas, 3)
    MÁXIMO_VOP_3M = VopMaxMensal(hpex_trusted_distintas, 3)
    QTD_TÍTULOS_3M = QtdTitulos(hpex_trusted_distintas, 3)
    TICKET_MÉDIO_3M = TicketMedio(hpex_trusted_distintas, 3)
    MÉDIA_DIF_DIAS_DE_FATURAMENTO_3M = MediaDifDiasFaturamento(hpex_trusted_distintas, 3)
    PRAZO_MÉDIO_3M = PrazoMedio(hpex_trusted_distintas, 3)
    VOP_6M = VopAcumulado(hpex_trusted_distintas, 6)
    VOP_A_VISTA_6M = VopAVista(hpex_trusted_distintas, 6)
    VOP_A_PRAZO_6M = VopAPrazo(hpex_trusted_distintas, 6)
    VOP_A_PRAZO_PARCELADO_6M = VopAPrazoComParcelamento(hpex_trusted_distintas, 6)
    MEDIA_VOP_6M = VopMediaMensal(hpex_trusted_distintas, 6)
    MÁXIMO_VOP_6M = VopMaxMensal(hpex_trusted_distintas, 6)
    QTD_TÍTULOS_6M = QtdTitulos(hpex_trusted_distintas, 6)
    TICKET_MÉDIO_6M = TicketMedio(hpex_trusted_distintas, 6)
    MÉDIA_DIF_DIAS_DE_FATURAMENTO_6M = MediaDifDiasFaturamento(hpex_trusted_distintas, 6)
    PRAZO_MÉDIO_6M = PrazoMedio(hpex_trusted_distintas, 6)
    VOP_12M = VopAcumulado(hpex_trusted_distintas, 12)
    VOP_A_VISTA_12M = VopAVista(hpex_trusted_distintas, 12)
    VOP_A_PRAZO_12M = VopAPrazo(hpex_trusted_distintas, 12)
    VOP_A_PRAZO_PARCELADO_12M = VopAPrazoComParcelamento(hpex_trusted_distintas, 12)
    MEDIA_VOP_12M = VopMediaMensal(hpex_trusted_distintas, 12)
    MÁXIMO_VOP_12M = VopMaxMensal(hpex_trusted_distintas, 12)
    MES_MAX_VOP_12M = MesMaxVop(hpex_trusted_distintas)
    QTD_TÍTULOS_12M = QtdTitulos(hpex_trusted_distintas, 12)
    TICKET_MÉDIO_12M = TicketMedio(hpex_trusted_distintas, 12)
    MÉDIA_DIF_DIAS_DE_FATURAMENTO_12M = MediaDifDiasFaturamento(hpex_trusted_distintas, 12)
    PRAZO_MÉDIO_12M = PrazoMedio(hpex_trusted_distintas, 12)
    EVER_10 = Ever(hpex_trusted_distintas, 10)
    EVER_30 = Ever(hpex_trusted_distintas, 30)
    EVER_60 = Ever(hpex_trusted_distintas, 60)
    EVER_90 = Ever(hpex_trusted_distintas, 90)
    OVER_10 = Over(hpex_trusted_distintas, 10)
    OVER_30 = Over(hpex_trusted_distintas, 30)
    OVER_60 = Over(hpex_trusted_distintas, 60)
    OVER_90 = Over(hpex_trusted_distintas, 90)
    QTDE_DIAS_MAXIMO_PAGAMENTO_ATRASADO_3M = QtdDiasMaxPagamentoAtrasado(hpex_trusted_distintas, 3)
    QTDE_DIAS_MAXIMO_PAGAMENTO_ATRASADO_12M = QtdDiasMaxPagamentoAtrasado(hpex_trusted_distintas, 12)
    PERCENTUAL_PAGO_EM_DIA_3M = PercentPagoEmDia(hpex_trusted_distintas, 3)
    PERCENTUAL_PAGO_EM_DIA_12M = PercentPagoEmDia(hpex_trusted_distintas, 12)
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_DO_PERÍODO_3M = PercentMedAlavancagemPeriodo(hpex_trusted_distintas, 3)
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_DO_PERÍODO_6M = PercentMedAlavancagemPeriodo(hpex_trusted_distintas ,6)
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_DO_PERÍODO_12M = PercentMedAlavancagemPeriodo(hpex_trusted_distintas, 12)
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_FINAL_3M = PercentMedAlavancagemFinal(hpex_trusted_distintas, 3)
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_FINAL_6M = PercentMedAlavancagemFinal(hpex_trusted_distintas, 6)
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_FINAL_12M = PercentMedAlavancagemFinal(hpex_trusted_distintas, 12)

    medidas = [VOP_3M,
    VOP_A_VISTA3M,
    VOP_A_PRAZO_3M,
    VOP_A_PRAZO_PARCELADO_3M,
    MEDIA_VOP_3M,
    MÁXIMO_VOP_3M,
    QTD_TÍTULOS_3M,
    TICKET_MÉDIO_3M,
    MÉDIA_DIF_DIAS_DE_FATURAMENTO_3M,
    PRAZO_MÉDIO_3M,
    VOP_6M,
    VOP_A_VISTA_6M,
    VOP_A_PRAZO_6M,
    VOP_A_PRAZO_PARCELADO_6M,
    MEDIA_VOP_6M,
    MÁXIMO_VOP_6M,
    QTD_TÍTULOS_6M,
    TICKET_MÉDIO_6M,
    MÉDIA_DIF_DIAS_DE_FATURAMENTO_6M,
    PRAZO_MÉDIO_6M,
    VOP_12M,
    VOP_A_VISTA_12M,
    VOP_A_PRAZO_12M,
    VOP_A_PRAZO_PARCELADO_12M,
    MEDIA_VOP_12M,
    MÁXIMO_VOP_12M,
    MES_MAX_VOP_12M,
    QTD_TÍTULOS_12M,
    TICKET_MÉDIO_12M,
    MÉDIA_DIF_DIAS_DE_FATURAMENTO_12M,
    PRAZO_MÉDIO_12M,
    EVER_10,
    EVER_30,
    EVER_60,
    EVER_90,
    OVER_10,
    OVER_30,
    OVER_60,
    OVER_90,
    PERCENTUAL_PAGO_EM_DIA_3M,
    PERCENTUAL_PAGO_EM_DIA_12M,
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_DO_PERÍODO_3M,
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_DO_PERÍODO_6M,
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_DO_PERÍODO_12M,
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_FINAL_3M,
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_FINAL_6M,
    PERCENTUAL_MÉDIO_DE_ALAVANCAGEM_FINAL_12M]

    for medida in medidas:
        boletos_refined = boletos_refined.join(medida, on='documento_raiz', how='left')

    # ## PT2 - MEDIDAS DE MEDIDAS

    #def TendenciaPrazoMedio(df):
        
        # Analisa se houve Aumento, Manutenção ou Queda no Prazo Médio de pagamento
    #    filtroQueda = df['PrazoMedio12M'] > df['PrazoMedio3M']
    #    filtroAlta = df['PrazoMedio3M'] > df['PrazoMedio12M']
    #    filtroManteve = df['PrazoMedio12M'] == df['PrazoMedio3M']
    #    df.loc[filtroQueda, 'TendenciaPrazoMedio'] = 'QUEDA'
    #    df.loc[filtroAlta, 'TendenciaPrazoMedio'] = 'ALTA'
    #    df.loc[filtroManteve, 'TendenciaPrazoMedio'] = 'MANTEVE'
        
    #    return df

    def FaixaPrazo(df):
        #Analisa em que faixa de prazo médio 12m o EC está
        
        FiltroAte30 = df['PrazoMedio12M'] <= 30
        Filtro31a60 = (df['PrazoMedio12M'] >= 31) & (df['PrazoMedio12M'] <= 60)
        Filtro61a90 = (df['PrazoMedio12M'] >= 61) & (df['PrazoMedio12M'] <= 90)
        FiltroAcima90 = df['PrazoMedio12M'] > 90
        
        df.loc[FiltroAte30, 'FaixaPrazo'] = 'ATÉ 30'
        df.loc[Filtro31a60, 'FaixaPrazo'] = '31 A 60'
        df.loc[Filtro61a90, 'FaixaPrazo'] = '61 A 90'
        df.loc[FiltroAcima90, 'FaixaPrazo'] = 'ACIMA DE 90'
        
        return df

    def FaixaAlavancagemPeriodo(df):
        #Analisa em que faixa de alavancagem do período 12m o EC está
        
        FiltroAte30 = df['PercentualMedioDeAlavancagemPeriodo12M'] <= 0.25
        Filtro31a60 = (df['PercentualMedioDeAlavancagemPeriodo12M'] > 0.25) & (df['PercentualMedioDeAlavancagemPeriodo12M'] <= 0.50)
        Filtro61a90 = (df['PercentualMedioDeAlavancagemPeriodo12M'] > 0.50) & (df['PercentualMedioDeAlavancagemPeriodo12M'] <= 0.75)
        FiltroAcima90 = df['PercentualMedioDeAlavancagemPeriodo12M'] > 0.75
        
        df.loc[FiltroAte30, 'FaixaAlavancagemPeriodo12M'] = 'ATÉ 25%'
        df.loc[Filtro31a60, 'FaixaAlavancagemPeriodo12M'] = '25% A 50%'
        df.loc[Filtro61a90, 'FaixaAlavancagemPeriodo12M'] = '50% A 75%'
        df.loc[FiltroAcima90, 'FaixaAlavancagemPeriodo12M'] = 'ACIMA DE 75%'

    def FaixaAlavancagemFinal(df):
        #Analisa em que faixa de alavancagem final 12m o EC está
        
        FiltroAte30 = df['PercentualMedioDeAlavancagemFinal12M'] <= 0.25
        Filtro31a60 = (df['PercentualMedioDeAlavancagemFinal12M'] > 0.25) & (df['PercentualMedioDeAlavancagemFinal12M'] <= 0.50)
        Filtro61a90 = (df['PercentualMedioDeAlavancagemFinal12M'] > 0.50) & (df['PercentualMedioDeAlavancagemFinal12M'] <= 0.75)
        FiltroAcima90 = df['PercentualMedioDeAlavancagemFinal12M'] > 0.75
        
        df.loc[FiltroAte30, 'FaixaAlavancagemFinal12M'] = 'ATÉ 25%'
        df.loc[Filtro31a60, 'FaixaAlavancagemFinal12M'] = '25% A 50%'
        df.loc[Filtro61a90, 'FaixaAlavancagemFinal12M'] = '50% A 75%'
        df.loc[FiltroAcima90, 'FaixaAlavancagemFinal12M'] = 'ACIMA DE 75%'

    def FaixaMediaDifDiasFat(df):
        #Analisa em que faixas com de diferença de dias de faturamentos entre as notas o EC está
        
        FiltroAte29 = df['MediaDifDiasFaturamento12M'] <= 29
        Filtro30a60 = (df['MediaDifDiasFaturamento12M'] > 30) & (df['MediaDifDiasFaturamento12M'] <= 60)
        FiltroAcima60 = df['MediaDifDiasFaturamento12M'] > 60
        
        df.loc[FiltroAte29, 'FaixaMediaDifDiasFaturamento12M'] = '0 - 29'
        df.loc[Filtro30a60, 'FaixaMediaDifDiasFaturamento12M'] = '30 - 60'
        df.loc[FiltroAcima60, 'FaixaMediaDifDiasFaturamento12M'] = '>60'

    def IsPreSafra(df):
        df['IsPreSafra'] = False
        #Pega o mês atual e caso seja igual o Mes com maior VOP ou 2 meses anteriores ele atribue True
        MesAtual = date.today().month

        for i in range(0, 3):
            # Corrige o valor para o intervalo correto de meses (como os meses estão armazenados como número o mês 1 e 2 dariam problema na hora da subtração e com essa matemágica esse problema é corrigido)
            mes_corrigido = (df['MesMaxVop12M'] - i - 1) % 12 + 1
            
            # Verifica se a condição é verdadeira para cada linha individualmente
            mask = (mes_corrigido == MesAtual) | (df['IsPreSafra'] == True)
            df['IsPreSafra'] = df['IsPreSafra'] | mask


    FaixaPrazo(boletos_refined)
    FaixaAlavancagemPeriodo(boletos_refined)
    FaixaAlavancagemFinal(boletos_refined)
    FaixaMediaDifDiasFat(boletos_refined)
    IsPreSafra(boletos_refined)

    colunas_string = ['documento_raiz']

    colunas_float = ['VOP3M', 'VOPAVista3M', 'VOPAPrazo3M', 'VOPAPrazoParcelado3M', 'MediaVOP3M', 'MaxVOP3M', 'TicketMedio3M', 'MediaDifDiasFaturamento3M',
                'PrazoMedio3M', 'VOP6M', 'VOPAVista6M', 'VOPAPrazo6M', 'VOPAPrazoParcelado6M', 'MediaVOP6M', 'MaxVOP6M', 'TicketMedio6M',
                'MediaDifDiasFaturamento6M', 'PrazoMedio6M', 'VOP12M', 'VOPAVista12M', 'VOPAPrazo12M', 'VOPAPrazoParcelado12M', 'MediaVOP12M',
                'MaxVOP12M', 'TicketMedio12M', 'MediaDifDiasFaturamento12M', 'PrazoMedio12M', 'PercentualPagoEmDia3M', 'PercentualPagoEmDia12M', 'PercentualMedioDeAlavancagemPeriodo3M',
                'PercentualMedioDeAlavancagemPeriodo6M', 'PercentualMedioDeAlavancagemPeriodo12M', 'PercentualMedioDeAlavancagemFinal3M', 
                'PercentualMedioDeAlavancagemFinal6M', 'PercentualMedioDeAlavancagemFinal12M']

    colunas_category = ['FaixaPrazo', 'FaixaAlavancagemPeriodo12M', 'FaixaAlavancagemFinal12M', 'FaixaMediaDifDiasFaturamento12M']

    colunas_binarias = ['IsPreSafra']


    colunas_int = ['QtdTitulos3M', 'QtdTitulos6M', 'MesMaxVop12M', 'QtdTitulos12M', 'Ever10', 
                'Ever30', 'Ever60', 'Ever90', 'Over10', 'Over30', 'Over60', 'Over90']


    # VERIFICANDO SE TODAS AS COLUNAS SERÃO TRATADAS. RESULTADO TEM QUE SER 0
    len(boletos_refined.columns) - len(colunas_string + colunas_category + colunas_float + colunas_int)

    # CONVERTENDO VALORES
    # Como existem valores nulos em int eles não vão ser convertidos para int e ficam como float

    #boletos_refined[colunas_int] = boletos_refined[colunas_int].astype('int')
    boletos_refined[colunas_string] = boletos_refined[colunas_string].astype('str')
    boletos_refined[colunas_float + colunas_int] = boletos_refined[colunas_float + colunas_int].astype('float')
    # boletos_refined[colunas_category] = boletos_refined[colunas_category].astype('category') # REMOVENDO PORUQE NÃO É COMPATÍVEL COM DELTALAKE
    boletos_refined[colunas_binarias] = boletos_refined[colunas_binarias].astype('bool')


    # boletos_refined.to_parquet('data/refined/hpex_refined.parquet', index = False)
    # boletos_refined.to_excel('data/refined/hpex_refined.xlsx', index = False)

    #def TendenciaAlavancagemPeriodo(df):
        
        # Analisa se houve Aumento, Manutenção ou Queda na alavancagem por periodo
        
    #    filtroQueda = df['PercentualMedioDeAlavancagemPeriodo12M'] > df['PercentualMedioDeAlavancagemPeriodo3M']
    #    filtroAlta = df['PercentualMedioDeAlavancagemPeriodo3M'] > df['PercentualMedioDeAlavancagemPeriodo12M']
    #    filtroManteve = df['PercentualMedioDeAlavancagemPeriodo12M'] == df['PercentualMedioDeAlavancagemPeriodo3M']
    #    df.loc[filtroQueda, 'TendenciaAlavancagemPeriodo'] = 'QUEDA'
    #    df.loc[filtroAlta, 'TendenciaAlavancagemPeriodo'] = 'ALTA'
    #    df.loc[filtroManteve, 'TendenciaAlavancagemPeriodo'] = 'MANTEVE'

    #def TendenciaAlavancagemFinal(df):
        
        # Analisa se houve Aumento, Manutenção ou Queda na alavancagem por periodo
        
    #    filtroQueda = df['PercentualMedioDeAlavancagemFinal12M'] > df['PercentualMedioDeAlavancagemFinal3M']
    #    filtroAlta = df['PercentualMedioDeAlavancagemFinal3M'] > df['PercentualMedioDeAlavancagemFinal12M']
    #    filtroManteve = df['PercentualMedioDeAlavancagemFinal12M'] == df['PercentualMedioDeAlavancagemFinal3M']
    #    df.loc[filtroQueda, 'TendenciaAlavancagemFinal'] = 'QUEDA'
    #    df.loc[filtroAlta, 'TendenciaAlavancagemFinal'] = 'ALTA'
    #    df.loc[filtroManteve, 'TendenciaAlavancagemFinal'] = 'MANTEVE'

    # adicionar momento do processamento
    boletos_refined['atualizado em'] = now

    boletos_refined['versão motor'] = 'v2'

    return boletos_refined


