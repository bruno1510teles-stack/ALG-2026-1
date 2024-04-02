# Carregando libs
import pandas as pd
import pyarrow as pa
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable
from scripts.query_trino_payments import query_trino

# Calculando Variáveis
def PrazoMedio(df, meses=None):    

    # Filtra pela quantidade de meses desejada se nessecário
    if meses is None:
        hpex_vop_acumulado = df
    
    else:    
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
    soma_vop = hpex_vop_acumulado.groupby(['documento_raiz', 'fornecedor'])['dias'].mean().dt.days
    df_saida = pd.DataFrame(soma_vop)
    df_saida = df_saida.reset_index()
    df_saida.columns = ['documento_raiz', 'fornecedor',f'prazo_medio_{meses}_meses']

    return df_saida

def MediaDifDiasFaturamento(df, meses=None):    

    # Filtra pela quantidade de meses desejada se nessecário
    if meses is None:
        hpex_vop_acumulado = df
    
    else:    
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
    # Filtrando somente a primeira parcela
    hpex_vop_acumulado = hpex_vop_acumulado.groupby(['documento_raiz', 'fornecedor', 'numero_titulo']).agg({
        'data_emissao': 'min'
    }).reset_index()
    hpex_vop_acumulado.columns = ['documento_raiz', 'fornecedor', 'numero_titulo', 'min_data_emissao']
    hpex_vop_acumulado.sort_values(by=['documento_raiz', 'fornecedor', 'min_data_emissao', 'numero_titulo'], inplace=True)
    hpex_vop_acumulado['diferenca'] = hpex_vop_acumulado.groupby(['fornecedor', 'documento_raiz'])['min_data_emissao'].diff()
    hpex_vop_acumulado = hpex_vop_acumulado.groupby(['documento_raiz', 'fornecedor']).agg({
        'diferenca': 'mean'
    }).reset_index()
    hpex_vop_acumulado.columns = ['documento_raiz', 'fornecedor', 'media_dif_dias_geral']
    hpex_vop_acumulado['media_dif_dias_geral'].fillna(pd.Timedelta(seconds=0), inplace=True)
    hpex_vop_acumulado.sort_values(by=['media_dif_dias_geral'], inplace=True)
    
    # Converter a coluna 'MEDIA_DIFERENÇA_DIAS_GERAL' de Timedelta para inteiros (dias)
    hpex_vop_acumulado['media_dif_dias_geral'] = hpex_vop_acumulado['media_dif_dias_geral'].dt.days

    
    df_saida = hpex_vop_acumulado
    df_saida = df_saida[['documento_raiz',	'fornecedor',	'media_dif_dias_geral']]
    df_saida.columns = ['documento_raiz', 'fornecedor',f'media_dif_dias_faturamento{meses}_meses']
    
    return df_saida
    
def PercentMedAlavancagemPeriodo(df, meses=None):    

    # Filtra pela quantidade de meses desejada se nessecário
    if meses is None:
        hpex_vop_acumulado = df
    
    else:    
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
    emissao = hpex_vop_acumulado[['documento_raiz', 'fornecedor', 'data_emissao', 'valor_titulo']]
    vencimento = hpex_vop_acumulado[['documento_raiz', 'fornecedor', 'data_vencimento', 'valor_titulo']]
    
    #Deixa as colnas com o nome certo para posterior concat
    emissao.columns = ['documento_raiz', 'fornecedor', 'data', 'valor_titulo_fat']    
    vencimento.columns = ['documento_raiz', 'fornecedor', 'data', 'valor_titulo_venc']
    
    df = pd.concat([emissao, vencimento])
    
    #Soma os valores que são do mesmo dia e do mesmo documento e posteriormente faz um acumulativo pelas datas
    df_acumulado = df.groupby(['documento_raiz', 'fornecedor', 'data']).sum().groupby(['documento_raiz', 'fornecedor']).cumsum().reset_index()
    
    #Realiza o calculo de porcentagem
    df_acumulado['Alavancagem'] = (df_acumulado['valor_titulo_venc']/df_acumulado['valor_titulo_fat'])
    
    #Traz a média de alavancagem diaria do EC
    AlavancagemPeriodo = df_acumulado.groupby(['documento_raiz', 'fornecedor'])['Alavancagem'].mean()
    
    df_saida = pd.DataFrame(AlavancagemPeriodo)
    df_saida = df_saida.reset_index()
    df_saida.columns = ['documento_raiz', 'fornecedor',f'percentual_medio_de_alavancagem_periodo{meses}_meses']
    
    return df_saida

def PercentMedAlavancagemFinal(df, meses=None):    

    # Filtra pela quantidade de meses desejada se nessecário
    if meses is None:
        hpex_vop_acumulado = df
    
    else:    
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
    AlavancagemFinal = (hpex_vop_acumulado.loc[FiltroVencido,:].groupby(['documento_raiz', 'fornecedor'])['valor_titulo'].sum() / hpex_vop_acumulado.groupby(['documento_raiz', 'fornecedor'])['valor_titulo'].sum()).fillna(0)
        
    df_saida = pd.DataFrame(AlavancagemFinal)
    df_saida = df_saida.reset_index()
    df_saida.columns = ['documento_raiz', 'fornecedor',f'percentual_medio_de_alavancagem_final{meses}_meses']
    
    
    return df_saida   

def QtdDiasMaxPagamentoAtrasado(df, meses=None):    

    # Filtra pela quantidade de meses desejada se nessecário
    if meses is None:
        hpex_vop_acumulado = df
    
    else:    
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
    
    soma_vop = hpex_vop_acumulado[filtroPagamentoNulo].groupby(['documento_raiz', 'fornecedor'])['dias_atraso'].max().dt.days
    
    df_saida = pd.DataFrame(soma_vop)
    df_saida = df_saida.reset_index()
    df_saida.columns = ['documento_raiz', 'fornecedor',f'qtde_dias_max_pagamento_atrasado{meses}_meses']
    
    return df_saida

def PercentPagoEmDia(df, meses=None):    

    # Filtra pela quantidade de meses desejada se nessecário
    if meses is None:
        hpex_vop_acumulado = df
    
    else:    
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
    df_pagos['IsPagoEmDia'] = df_pagos['IsPagoEmDia'] = (df_pagos['data_pagamento'] <= (df_pagos['data_vencimento'] + pd.Timedelta(days=5)))

    PercentPagoEmDia = (df_pagos[df_pagos['IsPagoEmDia']].groupby(['documento_raiz', 'fornecedor']).size() / df_pagos.groupby(['documento_raiz', 'fornecedor']).size()).fillna(0)
    
    df_saida = pd.DataFrame(PercentPagoEmDia)
    df_saida = df_saida.reset_index()
    df_saida.columns = ['documento_raiz', 'fornecedor',f'percentual_pago_em_dia{meses}_meses']
    
    return df_saida 

def Ever(df, dias):
    
    # Filtra pela quantidade de dias desejada
    # Temos alguns títulos que foram enviados com a data de pagamento anterior a data de emissão, como são poucos (2330 no dia 30/10/2023) assumiremos eles como 0 de diferença de dias, ou seja, foram pagos no prazo.
    
    filter_pagamento_nao_nulo = ~df['data_pagamento'].isna()
    hpex_pagos = df[filter_pagamento_nao_nulo]
    filter_ever = (hpex_pagos['data_pagamento'] - hpex_pagos['data_vencimento']).dt.days >= dias
    hpex_ever = hpex_pagos[filter_ever]    
    
    soma_vop = hpex_ever.groupby(['documento_raiz', 'fornecedor'])['valor_titulo'].count()
    df_saida = pd.DataFrame(soma_vop)
    df_saida = df_saida.reset_index()
    df_saida.columns = ['documento_raiz', 'fornecedor', f'ever_{dias}']

    return df_saida

def Over(df, dias):
    
    # Filtra pela quantidade de dias desejada
    # Temos alguns títulos que foram enviados com a data de pagamento anterior a data de emissão, como são poucos (2330 no dia 30/10/2023) assumiremos eles como 0 de diferença de dias, ou seja, foram pagos no prazo.
    
    filter_pagamento_nulo = df['data_pagamento'].isna()
    hpex_pagos = df[filter_pagamento_nulo]
    filter_over = (hpex_pagos['data_hp'] - hpex_pagos['data_vencimento']).dt.days >= dias
    hpex_ever = hpex_pagos[filter_over]    

    soma_vop = hpex_ever.groupby(['documento_raiz', 'fornecedor'])['valor_titulo'].count()
    df_saida = pd.DataFrame(soma_vop)
    df_saida = df_saida.reset_index()
    df_saida.columns = ['documento_raiz', 'fornecedor',f'over_{dias}']

    return df_saida

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
    
    soma_vop = hpex_vop_acumulado.groupby(['documento_raiz', 'fornecedor'])['valor_titulo'].sum()
    
    df_saida = pd.DataFrame(soma_vop)
    df_saida = df_saida.reset_index()
    df_saida.columns = ['documento_raiz', 'fornecedor',f'vop_{meses}_meses']

    return df_saida

def PercentualCompraRecorrente(df, meses=None):    

    # Filtra pela quantidade de meses desejada se nessecário
    if meses is None:
        PercentualCompraRecorrente = df
    
    else:    
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

        PercentualCompraRecorrente = df[(df['data_emissao'] >= df['data_inicial']) & (df['data_emissao'] <= df['data_final'])]

    # PercentualCompraRecorrente

    PercentualCompraRecorrente['data_emissao'] = pd.to_datetime(PercentualCompraRecorrente['data_emissao'])
    PercentualCompraRecorrente['data_vencimento'] = pd.to_datetime(PercentualCompraRecorrente['data_vencimento'])
    PercentualCompraRecorrente['dias'] = (PercentualCompraRecorrente['data_vencimento'] - PercentualCompraRecorrente['data_emissao']).dt.days
    PercentualCompraRecorrente['safra'] = PercentualCompraRecorrente['data_emissao'].dt.strftime('%Y-%m-01')

    # Criando Aux
    aux_PercentualCompraRecorrente = PercentualCompraRecorrente
    aux_PercentualCompraRecorrente = aux_PercentualCompraRecorrente[['fornecedor', 'safra']].drop_duplicates()
    aux_PercentualCompraRecorrente = aux_PercentualCompraRecorrente.groupby(['fornecedor']).agg({
    'safra': 'count'
    }).reset_index()
    aux_PercentualCompraRecorrente.columns = ['fornecedor', 'qtde_de_safras']
    PercentualCompraRecorrente = PercentualCompraRecorrente.merge(aux_PercentualCompraRecorrente, on=['fornecedor'], how = 'inner')
    PercentualCompraRecorrente['QTDE'] = 1
    PercentualCompraRecorrente = PercentualCompraRecorrente[['documento_raiz', 'fornecedor', 'safra', 'qtde_de_safras', 'QTDE']].drop_duplicates()
    PercentualCompraRecorrente = PercentualCompraRecorrente.groupby(['documento_raiz', 'fornecedor']).agg({
    'QTDE':'sum',
    'qtde_de_safras' : 'max'
    }).reset_index()
    PercentualCompraRecorrente.columns = ['documento_raiz', 'fornecedor', 'qtde_compras_geral', 'qtde_meses_total_safra']
    PercentualCompraRecorrente['percentual_compra_safra_geral'] = PercentualCompraRecorrente['qtde_compras_geral'] / PercentualCompraRecorrente['qtde_meses_total_safra']
    df_saida = PercentualCompraRecorrente[['documento_raiz', 'fornecedor', 'percentual_compra_safra_geral']]
    df_saida.columns = ['documento_raiz', 'fornecedor',f'percentual_compra_safra_geral{meses}_meses']
    return df_saida

# Criando conexão
def transform_data_to_refined(files_list, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_TRUSTED = "payments"
    TRUSTED_FOLDER =  "boletos/"
    BUCKET_SOURCE_REFINED = "payments"
    REFINED_FOLDER = "motor/book_de_variaveis/"

    df_payments = pd.DataFrame()

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_trusted'],
        access_key = access_params['aws_access_key_id_trusted'],
        secret_key = access_params['aws_secret_access_key_trusted'],
    )
    
    dfs = []
    # Juntando arquivos da HP
    for file_name in files_list:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_TRUSTED, object_name=file_name)
        df_hpex_raw_temp = pd.read_parquet(BytesIO(file.data))
        dfs.append(df_hpex_raw_temp)
    # Consolidando    
    base = pd.concat(dfs, ignore_index=True)

    base = base[base['fonte'] == 'HP_EXTERNA']
    base = base[base['tipo_documento'] == 'CNPJ']
    
    base['documento_raiz'] = base['documento'].str[:8]
    ids_query = str(base['documento_raiz'].unique().tolist()).replace('[', '(').replace(']', ')') 
    
    query = f""" WITH CTE AS (
    SELECT 
        substring(documento, 1, 8) documento_raiz,
        razao_social,
        numero_titulo,
        data_emissao,
        data_vencimento,
        data_pagamento,
        valor_titulo,
        data_hp,
        numero_parcela,
        fornecedor,
        fonte,
        atualizado_em,
        tipo_documento,
        year,
        month,
        day,
        ROW_NUMBER() OVER (PARTITION BY documento, numero_titulo, data_emissao, data_vencimento, fonte, fornecedor ORDER BY year DESC, month DESC, day DESC) AS rn
    FROM miniotrusted.payments.boletos 
    WHERE substring(documento, 1, 8) IN {ids_query} AND fonte = 'HP_EXTERNA' and tipo_documento = 'CNPJ'
        )
        SELECT 
            *
        FROM CTE
        WHERE rn = 1"""
    
    print(f'quantidade de CNPJs a serem atualziados: {len(ids_query)}')
    print(f"query: {query}")
    
    base = query_trino(query, 
                                access_params['trino_endpoint'],
                                access_params['trino_port'],
                                access_params['trino_user'],
                                access_params['trino_password'])
    base = base.drop('rn', axis = 1)

    prazo_medio_geral = PrazoMedio(base)
    print('prazo_medio_geral: executado!')
    
    prazo_medio_3_meses = PrazoMedio(base, 3)
    print('prazo_medio_3_meses: executado!')
    
    alavancagem_data_analise = PercentMedAlavancagemFinal(base)
    print('alavancagem_data_analise: executado!')
    
    alavancagem_media_historica = PercentMedAlavancagemPeriodo(base)
    print('alavancagem_media_historica: executado!')
    
    media_diferenca_dias_pedidos = MediaDifDiasFaturamento(base)
    print('media_diferenca_dias_pedidos: executado!')
    
    media_diferenca_dias_pedidos_3_meses = MediaDifDiasFaturamento(base, 3)
    print('media_diferenca_dias_pedidos_3_meses: executado!')
    
    maior_atraso_em_dias = QtdDiasMaxPagamentoAtrasado(base)
    print('maior_atraso_em_dias: executado!')
    
    maior_atraso_em_dias_3_meses = QtdDiasMaxPagamentoAtrasado(base, 3)
    print('maior_atraso_em_dias_3_meses: executado!')
    
    percentual_pago_em_dia = PercentPagoEmDia(base)
    print('percentual_pago_em_dia: executado!')
    
    percentual_pago_em_dia_3_meses = PercentPagoEmDia(base, 3)
    print('percentual_pago_em_dia_3_meses: executado!')
    
    over_5 = Over(base, 5)
    print('over_5: executado!')
    
    ever_10 = Ever(base, 10)
    print('ever_10: executado!')
    
    vop_6_meses = VopAcumulado(base,6)
    print('vop_6_meses: executado!')

    percentual_compra_recorrente_geral = PercentualCompraRecorrente(base)
    print('percentual_compra_recorrente_geral: executado!')

    percentual_compra_recorrente_3_meses = PercentualCompraRecorrente(base, 3)
    print('percentual_compra_recorrente_3_meses: executado!')

    #CONCATENANDO MEDIDAS
    dfs_inter = [
        prazo_medio_3_meses,
        alavancagem_data_analise,
        alavancagem_media_historica,
        media_diferenca_dias_pedidos,
        media_diferenca_dias_pedidos_3_meses,
        maior_atraso_em_dias,
        maior_atraso_em_dias_3_meses,
        percentual_pago_em_dia,
        percentual_pago_em_dia_3_meses,
        over_5,
        ever_10,
        vop_6_meses,
        percentual_compra_recorrente_geral,
        percentual_compra_recorrente_3_meses]

    df_final = prazo_medio_geral
    
    print('Concatenou as medidas')
    
    for df_inter in dfs_inter:
        df_final = pd.merge(df_final, df_inter, on=['documento_raiz', 'fornecedor'], how='outer')

    print('Agrupou as medidas')

    #AJUSTANDO NOME DAS COLUNAS
    colunas = ['documento_raiz', 
    'fornecedor',
    'prazo_medio_geral',
    'prazo_medio_3_meses',
    'alavancagem_data_analise',
    'alavancagem_media_historica',
    'media_diferenca_dias_pedidos',
    'media_diferenca_dias_pedidos_3_meses',
    'maior_atraso_em_dias',
    'maior_atraso_em_dias_3_meses',
    'percentual_pago_em_dia',
    'percentual_pago_em_dia_3_meses',
    'over_5',
    'ever_10',
    'vop_6_meses',
    'percentual_compra_recorrente_geral',
    'percentual_compra_recorrente_3_meses']
    
    df_final.columns = colunas

    print('Renomeou as colunas')
    
    colunas_float = [
        'prazo_medio_geral',
        'prazo_medio_3_meses',
        'alavancagem_data_analise',
        'alavancagem_media_historica',
        'media_diferenca_dias_pedidos',
        'media_diferenca_dias_pedidos_3_meses',
        'maior_atraso_em_dias',
        'maior_atraso_em_dias_3_meses',
        'percentual_pago_em_dia',
        'percentual_pago_em_dia_3_meses',
        'over_5',
        'ever_10',
        'vop_6_meses',
        'percentual_compra_recorrente_geral',
        'percentual_compra_recorrente_3_meses'
    ]
    df_final[colunas_float] = df_final[colunas_float].astype('float')

    print('Aplicou float nas medidas')
    
    #Definindo data de tratamento do arquivo
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    
    df_final['year'] = now.year
    df_final['month'] = now.month
    df_final['day'] = now.day
        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    
    # O pandas cria esse index, este codigo serve para remover caso ele crie
    df_final = pa.Table.from_pandas(df_final, preserve_index=False)

    write_deltalake(f"s3a://{BUCKET_SOURCE_REFINED}/{REFINED_FOLDER}", 
                    df_final, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )