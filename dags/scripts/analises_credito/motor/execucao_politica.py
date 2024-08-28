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


def execucao_politica(access_params=None):
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/auxiliar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/out'

    # variaveis
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso_horario)
    ano = agora.strftime('%Y')
    mes = agora.strftime('%m')
    dia = agora.strftime('%d')
    hora = agora.strftime('%H')


    # # Conectando na refined
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key=access_params['aws_access_key_id_refined'],
        secret_key=access_params['aws_secret_access_key_refined'],
    )
        


    # importando resposta modelo (score 1)
    dado_texto = {'cnpj_raiz':str}

    #saida_modelo = pd.read_csv(R'C:\Users\joao.leite\OneDrive - Yandeh\Desktop\Arvore Automatizada\AUXILIAR\LANDING_MODELO.csv', delimiter=';', dtype=dado_texto)
    file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/LANDING_MODELO.csv')
    saida_modelo = pd.read_csv(BytesIO(file.data), dtype=dado_texto, sep = ';')

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


    # # Configurando conexão Trino
    # conn = connect(
    #     host='trino.alpe.com.br',
    #     port='443',
    #     user='trinodados',
    #     auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
    #     http_scheme="https",
    # )

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

    query = (f"""
            with 
        total_restritivos_pj as (
    select 
        org.id,
        date(split_part(org.data_hora_consulta, ' ', 1)) AS data_consulta,
        org.cnpj_raiz,
        sum(coalesce(remp.valor_total, 0)) as "TOTAL RESTRITIVOS"
    from 
        deltalaketrusted.serasa.organizacoes org
    left join deltalaketrusted.serasa.resumo_restritivo remp
        on
        remp.id = org.id
        and remp.titular_pendencia = CONCAT('0',
        org.cnpj_raiz)
    group by 
        org.id,
        org.data_hora_consulta,
        org.cnpj_raiz
    )
    ,
        qtde_cheque_pj as (
    select 
        org.id,
        org.cnpj_raiz,
        sum(coalesce(remp.quantidade_ocorrencia, 0)) as "QTD CHEQUE"
    from 
        deltalaketrusted.serasa.organizacoes org
    left join deltalaketrusted.serasa.resumo_restritivo remp
        on
        remp.id = org.id
        and remp.titular_pendencia = CONCAT('0', org.cnpj_raiz)
    where
        remp.grupo_ocorrencia = 'CHEQUE'
    group by 
        org.id,
        org.cnpj_raiz
    )
    ,
        qtde_cheque_pf as (
    select 
        rsocio.id,
        sum(coalesce(rsocio.quantidade_ocorrencia, 0)) as "CHEQUE PF"
    from 
        deltalaketrusted.serasa.socios socios
    left join deltalaketrusted.serasa.resumo_restritivo rsocio
        on
        rsocio.id = socios.id
        and rsocio.titular_pendencia = socios.documento_socio 
    where
        rsocio.grupo_ocorrencia = 'CHEQUE'
    group by 
        rsocio.id
    )
    ,
        total_restritivos_socio as (
    select 
        rsocio.id, sum(coalesce(rsocio.valor_total, 0)) as "RESTRITIVOS PF"
    from 
        deltalaketrusted.serasa.socios socios
    left join deltalaketrusted.serasa.resumo_restritivo rsocio
        on
        rsocio.id = socios.id
        and rsocio.titular_pendencia = socios.documento_socio 
    group by 
        rsocio.id
    )
    ,
        score_pj as (
    select 
        sc.id, sc.valor_score as "Score Positivo PJ"
    from
        deltalaketrusted.serasa.score sc
    ),
        consulta_mais_recente AS (
    select
        org.cnpj_raiz,
        MAX(date(split_part(org.data_hora_consulta, ' ', 1))) AS consulta_mais_recente
    from 
        deltalaketrusted.serasa.organizacoes org 
    group by
        org.cnpj_raiz
    )
    select 
        trp.id,
        trp.data_consulta,
        trp.cnpj_raiz,
        score."Score Positivo PJ",
        trp."TOTAL RESTRITIVOS",
        coalesce(cpj."QTD CHEQUE", 0) as "QTD CHEQUE",
        coalesce(cpf."CHEQUE PF", 0) as "CHEQUE PF",
        coalesce(trs."RESTRITIVOS PF", 0) as "RESTRITIVOS PF"
    from 
        total_restritivos_pj trp
    left join 
        qtde_cheque_pj cpj
        on trp.id = cpj.id
    left join 
        qtde_cheque_pf cpf
        on trp.id = cpf.id
    left join 
        total_restritivos_socio trs 
        on trp.id = trs.id
    left join 
        score_pj score
        on trp.id = score.id
    inner join 
        consulta_mais_recente cmr
        on trp.cnpj_raiz = cmr.cnpj_raiz and trp.data_consulta = cmr.consulta_mais_recente
    where 
        trp.cnpj_raiz in {ids_query}

        """)

    print(query)

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

    # # # # # # # # dados_texto = {'CPF do Principal Socio':'str', 'CNPJ':'str', 'Capital Social':'float'}

    # # # # # # # # # importando consulta BVS
    # # # # # # # # file = client.get_object(
    # # # # # # # #     bucket_name=BUCKET_SOURCE_REFINED, 
    # # # # # # # #     object_name=f'{FOLDER_SOURCE_REFINED}/{ano}/{mes}/{dia}/BVS.csv'
    # # # # # # # # )


    # # # # # # # # #file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/BVS.csv')
    # # # # # # # # bvs_pj = pd.read_csv(BytesIO(file.data), sep=';',dtype = dados_texto)

    # # # # # # # # # Ajustando a formatação do CNPJ para 14 digitos
    # # # # # # # # bvs_pj['CNPJ'] = bvs_pj['CNPJ'].astype(str).str.zfill(14)
    # # # # # # # # bvs_pj['cnpj_raiz'] = bvs_pj['CNPJ'].astype(str).str.slice(0, 8)

    # # # # # # # # # Ajustando a formatação do CPF para 11 digitos
    # # # # # # # # bvs_pj['CPF do Principal Socio'] = bvs_pj['CPF do Principal Socio'].str.zfill(11)


    # # # # # # # # # importando a base SPC PJ
    # # # # # # # # file = client.get_object(
    # # # # # # # #     bucket_name=BUCKET_SOURCE_REFINED, 
    # # # # # # # #     object_name=f'{FOLDER_SOURCE_REFINED}/{ano}/{mes}/{dia}/SPC_PJ.xlsx'
    # # # # # # # # )
    # # # # # # # # #file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/SPC_PJ.xlsx')
    # # # # # # # # spc_pj = pd.read_excel(BytesIO(file.data))

    # # # # # # # # # Ajustando a formatação do CNPJ para 14 digitos
    # # # # # # # # spc_pj['CNPJ'] = spc_pj['CNPJ'].astype(str).str.zfill(14)
    # # # # # # # # spc_pj['cnpj_raiz'] = spc_pj['CNPJ'].astype(str).str.slice(0, 8)

    # # # # # # # # # importando a base SPC PF
    # # # # # # # # dados_texto_pf = {'CPF':'str', 'CNPJ':'str'}

    # # # # # # # # file = client.get_object(
    # # # # # # # #     bucket_name=BUCKET_SOURCE_REFINED, 
    # # # # # # # #     object_name=f'{FOLDER_SOURCE_REFINED}/{ano}/{mes}/{dia}/SPC_PF.xlsx'
    # # # # # # # # )

    # # # # # # # # #file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/SPC_PF.xlsx')
    # # # # # # # # spc_pf = pd.read_excel(BytesIO(file.data), dtype=dados_texto_pf)

    # # # # # # # # # formatando o CPF
    # # # # # # # # spc_pf['CPF'] = spc_pf['CPF'].astype(str).str.zfill(11)
    # # # # # # # # spc_pf['cnpj_raiz'] = spc_pf['CNPJ'].astype(str).str.slice(0, 8)

    # # # # # # # # spc_pf = spc_pf.drop(columns=['CPF.1'])

    # # # # # # # # # Criando um novo df com as principais colunas pj e pf

    # # # # # # # # # unificando os dados pj MODELO e BVS
    # # # # # # # # base_modelo_bvs = pd.merge(seguem_analise_com_hp,
    # # # # # # # #     bvs_pj[['cnpj_raiz','CNPJ','Razao Social','CPF do Principal Socio','Data de Fundacao','Faixa Faturamento Presumido Positivo','Capital Social','Score Positivo PJ','Indicativo de Restritivo']],
    # # # # # # # #     left_on='cnpj_raiz', right_on='cnpj_raiz', how='left')
    # # # # # # # # base_modelo_bvs = base_modelo_bvs.drop(columns = ['CNPJ'])

    # # # # # # # # # unificando os dados pj MODELO + BVS com SPC
    # # # # # # # # base_modelo_bvs_spc = pd.merge(base_modelo_bvs,
    # # # # # # # #     spc_pj[['cnpj_raiz','CNPJ','TOTAL RESTRITIVOS','QTD RESTRITIVOS','QTD CHEQUE']],
    # # # # # # # #     left_on='cnpj_raiz', right_on='cnpj_raiz', how='left')

    # # # # # # # # #Trazendo os dados PF
    # # # # # # # # spc_pf_reduzida = spc_pf[['CNPJ','CPF','RESTRITIVOS','CHEQUE']]
    # # # # # # # # spc_pf_reduzida['cnpj_raiz'] = spc_pf_reduzida['CNPJ'].str.slice(0,8)


    # # # # # # # # base_modelo_bureau = pd.merge(base_modelo_bvs_spc, spc_pf_reduzida,
    # # # # # # # #     left_on = 'cnpj_raiz',
    # # # # # # # #     right_on ='cnpj_raiz',
    # # # # # # # #     how='left')

    # # # # # # # # base_modelo_bureau = base_modelo_bureau.drop(columns = ['CNPJ_x','CNPJ_y'])
    # # # # # # # # base_modelo_bureau = base_modelo_bureau.rename(columns={'RESTRITIVOS':'RESTRITIVOS PF'})
    # # # # # # # # base_modelo_bureau = base_modelo_bureau.rename(columns={'CHEQUE':'CHEQUE PF'})


    # # # # # # # # print(f'Base modelo + bvs \n',base_modelo_bvs['CLASSIFICACAO'].value_counts())
    # # # # # # # # print(f'Base modelo + bvs + spc \n',base_modelo_bvs_spc['CLASSIFICACAO'].value_counts())
    # # # # # # # # print(f'Base modelo + bvs \n',base_modelo_bureau['CLASSIFICACAO'].value_counts())

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
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] > 100000000) :
                return 'A - C6'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['TOTAL RESTRITIVOS'] > 500000):
                return 'A - C5'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            1000 < linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] < 646):
                return 'A - C4'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            1000 < linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] >= 646): 
                return 'A - C3'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] < 646): 
                return 'A - C2'
        elif(linha['CLASSIFICACAO'] == 'C' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] >= 646): 
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
            linha['Score Positivo PJ'] != 2 and 
            linha['Capital Social'] > 100000000):
                return 'A - B6'    
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['TOTAL RESTRITIVOS'] > 500000):
                return 'A - B5'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            1000 < linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] < 646):
                return 'A - B4'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            1000 < linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] >= 646):
                return 'A - B3'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] < 646): 
                return 'A - B2'
        elif(linha['CLASSIFICACAO'] == 'B' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] >= 646): 
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
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['Capital Social'] > 100000000) :
                return 'A - A10'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['TOTAL RESTRITIVOS'] > 500000):
                return 'A - A9'
        elif(linha['CLASSIFICACAO'] == 'A' and    
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            1000 < linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] < 646):
                return 'A - A8'
        elif(linha['CLASSIFICACAO'] == 'A' and    
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            1000 < linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] >= 646):
                return 'A - A7'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] < 646):
                return 'A - A6'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] >= 646 and 
            linha['Score Positivo PJ'] <= 711):
                return 'A - A5'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] > 710 and
            linha['RESTRITIVOS PF'] > 500):
                return 'A - A4'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] > 710 #and
            #pd.isnull(linha['CPF do Principal Socio']
                      ):
                return 'A - A3'    
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] > 710 and
            linha['RESTRITIVOS PF'] <= 500 and
            linha['idade_socio'] < 2) :
                return 'A - A2'
        elif(linha['CLASSIFICACAO'] == 'A' and
            linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and      
            linha['Score Positivo PJ'] > 710 and
            linha['RESTRITIVOS PF'] <= 500 and
            linha['idade_socio'] >= 2) :
                return 'A - A1'  
        return "Verificar"

    seguem_analise_com_hp['DECISAO_POLITICA'] = seguem_analise_com_hp.apply(politica_com_hp, axis=1)

    seguem_analise_com_hp['resposta_motor'] = None

    # REPROVADO
    #base_modelo_bureau.loc[base_modelo_bureau['DECISAO_POLITICA'].isin(['A - E1','A - C7', 'A - C5', 'A - C4','A - C2' 'A - B7', 'A - B5', 'A - B4', 'A - A11','A - A9', 'A - A8']), 'resposta_motor'] = 'REPROVADO'
    
    # MESA
    #base_modelo_bureau.loc[base_modelo_bureau['DECISAO_POLITICA'].isin(['A - A2', 'A - A3', 'A - A4', 'A - A5', 'A - A6', 'A - A7','A - A10', 'A - B1', 'A - B2','A - B3','A - B6', 'A - C1','A - C3','A - C6','A - D1']), 'resposta_motor'] = 'MESA'
    
    # APROVADO
    #base_modelo_bureau.loc[base_modelo_bureau['DECISAO_POLITICA'].isin(['A - A1']), 'resposta_motor'] = 'APROVADO'

    # Separando as linhas que não possuem HP para rodar a politica

    segue_analise_sem_hp = saida_modelo[saida_modelo['possui_hp'] == 'NAO']

    segue_analise_sem_hp = segue_analise_sem_hp.merge(base_retorno_serasa, on = ['cnpj_raiz'], how = 'left')

    # Criando um novo df com as principais colunas pj e pf somente de bureau, para a politica sem HP

    # # # # # # # # # # unificando base sem HP com BVS
    # # # # # # # # # base_bvs_pj = pd.merge(segue_analise_sem_hp,
    # # # # # # # # #     bvs_pj[['cnpj_raiz','CNPJ','Razao Social','CPF do Principal Socio','Data de Fundacao','Faixa Faturamento Presumido Positivo','Capital Social','Score Positivo PJ','Indicativo de Restritivo']],
    # # # # # # # # #     left_on='cnpj_raiz', right_on='cnpj_raiz', how='left')

    # # # # # # # # # base_bvs_pj.head()

    # # # # # # # # # base_bvs_spc_pj = pd.merge(base_bvs_pj,
    # # # # # # # # #     spc_pj[['cnpj_raiz','TOTAL RESTRITIVOS','QTD RESTRITIVOS','QTD CHEQUE']],
    # # # # # # # # #     left_on='cnpj_raiz', right_on='cnpj_raiz', how='left')

    # # # # # # # # # #Trazendo os dados PF
    # # # # # # # # # base_sem_hp = pd.merge(base_bvs_spc_pj,spc_pf_reduzida,
    # # # # # # # # #     left_on='cnpj_raiz',
    # # # # # # # # #     right_on='cnpj_raiz',
    # # # # # # # # #     how='left')

    # # # # # # # # # base_sem_hp = base_sem_hp.drop(columns=['CNPJ_y'])
    # # # # # # # # # base_sem_hp = base_sem_hp.rename(columns={'RESTRITIVOS':'RESTRITIVOS PF'})
    # # # # # # # # # base_sem_hp = base_sem_hp.rename(columns={'CHEQUE':'CHEQUE PF'})
    # # # # # # # # # base_sem_hp = base_sem_hp.rename(columns={'CNPJ_x':'CNPJ'})

    # Função com a árvore de decisão sem HP
    def politica_sem_hp(linha):
        if pd.isnull(linha[['Score Positivo PJ','TOTAL RESTRITIVOS','QTD CHEQUE','RESTRITIVOS PF','CHEQUE PF']]).values.any():
            return 'B - 12'
        elif linha['QTD CHEQUE'] > 0: 
            return 'B - 11'
        elif(linha['QTD CHEQUE'] == 0 and 
            linha['Score Positivo PJ'] == 2):
                return 'B - 10'
        elif(linha['QTD CHEQUE'] == 0 and 
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] > 100000000):
                return 'B - 10'
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            linha['TOTAL RESTRITIVOS'] > 500000):
                return 'B - 9'
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            1000 < linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] < 646):
                return 'B - 8'
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and
            1000 < linha['TOTAL RESTRITIVOS'] <= 500000 and
            linha['Score Positivo PJ'] >= 646): 
                return 'B - 7'
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and 
            linha['Capital Social'] <= 100000000 and    
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] < 646):
                return 'B - 6'
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and    
            linha['TOTAL RESTRITIVOS'] <= 1000 and
            linha['Score Positivo PJ'] >= 646 and
            linha['Score Positivo PJ'] <= 900):
                return "B - 5"
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 900 #and
            #pd.isnull(linha['CPF do Principal Socio'])
            ):
                return "B - 4"
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 900 and
            pd.isnull(linha['idade_socio'])):
                return "B - 4"
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 900 and
            linha['RESTRITIVOS PF'] > 500):
                return "B - 3"
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 900 and   
            linha['RESTRITIVOS PF'] <= 500 and
            linha['idade_socio'] < 2):
                return "B - 2"
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 900 and   
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
    resposta_motor.loc[resposta_motor['DECISAO_POLITICA'].isin(['A - E1','A - C7', 'A - C5', 'A - C4','A - C2', 'A - B7', 'A - B5', 'A - B4', 'A - A11','A - A9', 'A - A8', 'B - 11', 'B - 9', 'B - 8']), 'resposta_motor'] = 'REPROVADO'
    
    # MESA
    resposta_motor.loc[resposta_motor['DECISAO_POLITICA'].isin(['A - A2', 'A - A3', 'A - A4', 'A - A5', 'A - A6', 'A - A7','A - A10', 'A - B1', 'A - B2','A - B3','A - B6', 'A - C1','A - C3','A - C6','A - D1', 'B - 10', 'B - 7', 'B - 6', 'B - 5', 'B - 4', 'B - 3', 'B - 2','B - 12', 'A - A12', 'A - B8', 'A - C8']), 'resposta_motor'] = 'MESA'
    
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

    #DEFININDO O PARECER DO MOTOR, PARA OS CASOS NEGADOS
    resposta_motor.loc[resposta_motor['resposta_motor'] == 'REPROVADO', 'parecer'] = 'Motor - Recusado, dados analisados fora da politica atual'
    resposta_motor.loc[resposta_motor['resposta_motor'] == 'MESA', 'parecer'] = 'Motor - Direcionar para avaliação da mesa de crédito'


    resposta_motor_resumida = resposta_motor[['issue_jira', 'resposta_motor', 'cnpj_ec', 'parecer', 'ramificacao_motor']].rename(columns={
    'cnpj_ec': 'cnpj_ec',
    'resposta_motor': 'resolucao',
    'ramificacao_motor': 'ramificacao'
    })

    resposta_motor_resumida['path_arquivos_minio'] = ''
    resposta_motor_resumida['valor_aprovado'] = 0

    resposta_motor_resumida = resposta_motor_resumida[['issue_jira', 'resolucao', 'cnpj_ec', 'valor_aprovado', 'parecer', 'ramificacao', 'path_arquivos_minio']]

    print("Quantidade de CNPJs por ramificação:")
    print(resposta_motor_resumida.groupby('ramificacao')['cnpj_ec'].size())

    print("Quantidade de CNPJs por parecer:")
    print(resposta_motor_resumida.groupby('parecer')['cnpj_ec'].size())

    print(resposta_motor_resumida)

    # Tratando para Salvar Arquivos

    def tratando_coluna(value):
        if not isinstance(value, str):
            value = str(value)  # Converte para string, se não for
        
        # Remove qualquer caractere que não seja alfanumérico
        return re.sub(r'\W+', '', value)

# Itera sobre cada combinação de 'issue_jira' e 'CNPJ' no DataFrame
    for _, row in resposta_motor.iterrows():
        issue_jira = row['issue_jira']
        cnpj = row['cnpj_ec']
        
        # Sanitize os valores de 'issue_jira' e 'CNPJ'
        issue_jira_tratado = tratando_coluna(issue_jira)
        cnpj_tratado = tratando_coluna(cnpj)
        
        # Gera o nome base do arquivo combinando 'issue_jira' e 'CNPJ'
        file_base_name = f'{issue_jira_tratado}_{cnpj_tratado}'

        # Filtra o DataFrame resumido e detalhado para o CNPJ específico
        df_resumido = resposta_motor_resumida[resposta_motor_resumida['cnpj_ec'] == cnpj]
        df_detalhado = resposta_motor[resposta_motor['cnpj_ec'] == cnpj]

        # Adiciona mensagens de log para depuração
        print(f"Processando CNPJ: {cnpj_tratado}")
        print(f"Resumido DF: {df_resumido.shape}")
        print(f"Detalhado DF: {df_detalhado.shape}")
        
        # Salva a análise resumida
        file_out_resumido = f'RESPOSTA_MOTOR_RESUMIDA_{file_base_name}.csv'
        csv_bytes_resumido = df_resumido.to_csv(index=False, sep=';').encode('utf-8')
        csv_buffer_resumido = BytesIO(csv_bytes_resumido)
        client.put_object(
            f'{BUCKET_SOURCE_REFINED}',
            f'{FOLDER_DESTINATION_REFINED}/{ano}/{mes}/{dia}/{hora}/resumida/{file_out_resumido}',
            data=csv_buffer_resumido,
            length=len(csv_bytes_resumido)
        )
        
        # Salva a análise detalhada
        file_out_detalhado = f'RESPOSTA_MOTOR_DETALHADA_{file_base_name}.csv'
        csv_bytes_detalhado = df_detalhado.to_csv(index=False, sep=';').encode('utf-8')
        csv_buffer_detalhado = BytesIO(csv_bytes_detalhado)
        client.put_object(
            f'{BUCKET_SOURCE_REFINED}',
            f'{FOLDER_DESTINATION_REFINED}/{ano}/{mes}/{dia}/{hora}/detalhada/{file_out_detalhado}',
            data=csv_buffer_detalhado,
            length=len(csv_bytes_detalhado)
        )

        return df_resumido.to_dict(orient='records')