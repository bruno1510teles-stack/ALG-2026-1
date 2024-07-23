# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from io import BytesIO


def execucao_politica(access_params=None):
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/auxiliar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/out'


    # Conectando na refined
    client = Minio(
        'api-refined.alpe.com.br',
        access_key = '0FKu1vkOJbq0K4C0qRuF',
        secret_key = 'PIqXSinLX2q9XTvGsVrw5Z5jzyuBl7ng7hIq62oA',
    )
        

    # importando resposta modelo (score 1)
    dado_texto = {'cnpj_raiz':str}

    #saida_modelo = pd.read_csv(R'C:\Users\joao.leite\OneDrive - Yandeh\Desktop\Arvore Automatizada\AUXILIAR\LANDING_MODELO.csv', delimiter=';', dtype=dado_texto)
    file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/LANDING_MODELO.csv')
    saida_modelo = pd.read_csv(BytesIO(file.data), dtype=dado_texto, sep = ';')

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

    dados_texto = {'CPF do Principal Socio':'str', 'CNPJ':'str'}

    # importando consulta BVS
    file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/BVS.csv')
    bvs_pj = pd.read_csv(BytesIO(file.data), sep=';',dtype = dados_texto)

    # Ajustando a formatação do CNPJ para 14 digitos
    bvs_pj['CNPJ'] = bvs_pj['CNPJ'].astype(str).str.zfill(14)
    bvs_pj['cnpj_raiz'] = bvs_pj['CNPJ'].astype(str).str.slice(0, 8)

    # Ajustando a formatação do CPF para 11 digitos
    bvs_pj['CPF do Principal Socio'] = bvs_pj['CPF do Principal Socio'].str.zfill(11)


    # importando a base SPC PJ

    file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/SPC_PJ.csv')
    spc_pj = pd.read_csv(BytesIO(file.data), sep = ';')

    spc_pj['TOTAL RESTRITIVOS'] = spc_pj['TOTAL RESTRITIVOS'].str.replace(',','.').astype(float)

    # Ajustando a formatação do CNPJ para 14 digitos
    spc_pj['CNPJ'] = spc_pj['CNPJ'].astype(str).str.zfill(14)
    spc_pj['cnpj_raiz'] = spc_pj['CNPJ'].astype(str).str.slice(0, 8)

    # importando a base SPC PF
    dados_texto_pf = {'CPF':'str', 'CNPJ':'str'}

    file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/SPC_PF.csv')
    spc_pf = pd.read_csv(BytesIO(file.data), dtype = dados_texto_pf, sep = ';')

    # formatando o CPF
    spc_pf['CPF'] = spc_pf['CPF'].astype(str).str.zfill(11)
    spc_pf['cnpj_raiz'] = spc_pf['CNPJ'].astype(str).str.slice(0, 8)

    spc_pf = spc_pf.drop(columns=['CPF.1'])

    # Criando um novo df com as principais colunas pj e pf

    # unificando os dados pj MODELO e BVS
    base_modelo_bvs = pd.merge(seguem_analise_com_hp,
        bvs_pj[['cnpj_raiz','CNPJ','Razao Social','CPF do Principal Socio','Data de Fundacao','Faixa Faturamento Presumido Positivo','Capital Social','Score Positivo PJ','Indicativo de Restritivo']],
        left_on='cnpj_raiz', right_on='cnpj_raiz', how='left')
    base_modelo_bvs = base_modelo_bvs.drop(columns = ['CNPJ'])

    # unificando os dados pj MODELO + BVS com SPC
    base_modelo_bvs_spc = pd.merge(base_modelo_bvs,
        spc_pj[['cnpj_raiz','CNPJ','TOTAL RESTRITIVOS','QTD RESTRITIVOS','QTD CHEQUE']],
        left_on='cnpj_raiz', right_on='cnpj_raiz', how='left')

    #Trazendo os dados PF
    spc_pf_reduzida = spc_pf[['CNPJ','CPF','RESTRITIVOS','CHEQUE']]
    spc_pf_reduzida['cnpj_raiz'] = spc_pf_reduzida['CNPJ'].str.slice(0,8)


    base_modelo_bureau = pd.merge(base_modelo_bvs_spc, spc_pf_reduzida,
        left_on = 'cnpj_raiz',
        right_on ='cnpj_raiz',
        how='left')

    base_modelo_bureau = base_modelo_bureau.drop(columns = ['CNPJ_x','CNPJ_y'])
    base_modelo_bureau = base_modelo_bureau.rename(columns={'RESTRITIVOS':'RESTRITIVOS PF'})
    base_modelo_bureau = base_modelo_bureau.rename(columns={'CHEQUE':'CHEQUE PF'})


    print(f'Base modelo + bvs \n',base_modelo_bvs['CLASSIFICACAO'].value_counts())
    print(f'Base modelo + bvs + spc \n',base_modelo_bvs_spc['CLASSIFICACAO'].value_counts())
    print(f'Base modelo + bvs \n',base_modelo_bureau['CLASSIFICACAO'].value_counts())

    # Função com a árvore de decisão
    def politica_com_hp(linha):
        if(linha['CLASSIFICACAO'] == 'C' and
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
            646 < linha['Score Positivo PJ'] <= 711):
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
            linha['Score Positivo PJ'] > 710 and
            pd.isnull(linha['CPF do Principal Socio'])):
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

    base_modelo_bureau['DECISAO_POLITICA'] = base_modelo_bureau.apply(politica_com_hp, axis=1)

    base_modelo_bureau['resposta_motor'] = None

    # REPROVADO
    base_modelo_bureau.loc[base_modelo_bureau['DECISAO_POLITICA'].isin(['A - E1','A - C7', 'A - C5', 'A - C4','A - C2' 'A - B7', 'A - B5', 'A - B4', 'A - A11','A - A9', 'A - A8']), 'resposta_motor'] = 'REPROVADO'
    
    # MESA
    base_modelo_bureau.loc[base_modelo_bureau['DECISAO_POLITICA'].isin(['A - A2', 'A - A3', 'A - A4', 'A - A5', 'A - A6', 'A - A7','A - A10', 'A - B1', 'A - B2','A - B3','A - B6', 'A - C1','A - C3','A - C6','A - D1']), 'resposta_motor'] = 'MESA'
    
    # APROVADO
    base_modelo_bureau.loc[base_modelo_bureau['DECISAO_POLITICA'].isin(['A - A1']), 'resposta_motor'] = 'APROVADO'

    # Separando as linhas que não possuem HP para rodar a politica

    segue_analise_sem_hp = saida_modelo[saida_modelo['possui_hp'] == 'NAO']

    # Criando um novo df com as principais colunas pj e pf somente de bureau, para a politica sem HP

    # unificando base sem HP com BVS
    base_bvs_pj = pd.merge(segue_analise_sem_hp,
        bvs_pj[['cnpj_raiz','CNPJ','Razao Social','CPF do Principal Socio','Data de Fundacao','Faixa Faturamento Presumido Positivo','Capital Social','Score Positivo PJ','Indicativo de Restritivo']],
        left_on='cnpj_raiz', right_on='cnpj_raiz', how='left')
    #base_bvs_pj = base_bvs_pj.drop(columns = ['cnpj_raiz'])

    print(base_bvs_pj['cnpj_raiz'].count())

    base_bvs_pj.head()

    base_bvs_spc_pj = pd.merge(base_bvs_pj,
        spc_pj[['cnpj_raiz','TOTAL RESTRITIVOS','QTD RESTRITIVOS','QTD CHEQUE']],
        left_on='cnpj_raiz', right_on='cnpj_raiz', how='left')

    #Trazendo os dados PF
    base_sem_hp = pd.merge(base_bvs_spc_pj,spc_pf_reduzida,
        left_on='cnpj_raiz',
        right_on='cnpj_raiz',
        how='left')

    base_sem_hp = base_sem_hp.drop(columns=['CNPJ_y'])
    base_sem_hp = base_sem_hp.rename(columns={'RESTRITIVOS':'RESTRITIVOS PF'})
    base_sem_hp = base_sem_hp.rename(columns={'CHEQUE':'CHEQUE PF'})
    base_sem_hp = base_sem_hp.rename(columns={'CNPJ_x':'CNPJ'})

    # Função com a árvore de decisão sem HP
    def politica_sem_hp(linha):
        if linha['QTD CHEQUE'] > 0: 
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
            646 <= linha['Score Positivo PJ'] <= 900):
                return "B - 5"
        elif(linha['QTD CHEQUE'] == 0 and
            linha['Score Positivo PJ'] != 2 and
            linha['Capital Social'] <= 100000000 and     
            linha['TOTAL RESTRITIVOS'] <= 1000 and        
            linha['Score Positivo PJ'] > 900 and
            pd.isnull(linha['CPF do Principal Socio'])):
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

    base_sem_hp['DECISAO_POLITICA'] = base_sem_hp.apply(politica_sem_hp, axis=1)

    base_sem_hp['resposta_motor'] = None
    
    # REPROVADO
    base_sem_hp.loc[base_sem_hp['DECISAO_POLITICA'].isin(['B - 11', 'B - 9', 'B - 8']), 'resposta_motor'] = 'REPROVADO'
    
    # MESA
    base_sem_hp.loc[base_sem_hp['DECISAO_POLITICA'].isin(['B - 10', 'B - 7', 'B - 6', 'B - 5', 'B - 4', 'B - 3', 'B - 2']), 'resposta_motor'] = 'MESA'
    
    # APROVADO
    base_sem_hp.loc[base_sem_hp['DECISAO_POLITICA'].isin(['B - 1']), 'resposta_motor'] = 'APROVADO'

    cnpjs_politica_c = list(base_modelo_bureau['cnpj_raiz'])
    cnpjs_politica_s = list(base_sem_hp['cnpj_raiz'])

    cnpjs_politica = cnpjs_politica_c + cnpjs_politica_s

    filtro = ~saida_modelo['cnpj_raiz'].isin(cnpjs_politica)

    sem_politica = saida_modelo[filtro]

    resposta_motor = pd.concat([sem_politica, base_modelo_bureau, base_sem_hp], axis=0)

    filtro_na = resposta_motor['resposta_motor'].isnull()
    resposta_motor.loc[filtro_na, 'resposta_motor'] = resposta_motor.loc[filtro_na, 'resposta_modelo']

    filtro_na = resposta_motor['resposta_motor'].isnull()
    resposta_motor.loc[filtro_na, 'resposta_motor'] = resposta_motor.loc[filtro_na, 'resposta']

    resposta_motor['ramificacao_motor'] = resposta_motor['DECISAO_POLITICA']

    filtro_na = resposta_motor['DECISAO_POLITICA'].isnull()
    resposta_motor.loc[filtro_na, 'ramificacao_motor'] = resposta_motor.loc[filtro_na, 'ramificacao_pre_filtro']

    resposta_motor_resumida = resposta_motor[['cnpj_raiz', 'documento_sem_formatacao', 'ramificacao_motor', 'resposta_motor']]

    #GRAVANDO
    # Nome do arquivo CSV de output que subirá para a execução da política
    file_out = f'RESPOSTA_MOTOR_RESUMIDA.csv'

    csv_bytes = resposta_motor_resumida.to_csv(index=False, sep=';').encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    client.put_object(f'{BUCKET_SOURCE_REFINED}',
                        f'{FOLDER_DESTINATION_REFINED}/{file_out}',
                            data=csv_buffer,
                            length=len(csv_bytes))

    #GRAVANDO
    # Nome do arquivo CSV de output que subirá para a execução da política
    file_out = f'RESPOSTA_MOTOR_DETALHADA.csv'

    csv_bytes = resposta_motor.to_csv(index=False, sep=';').encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    client.put_object(f'{BUCKET_SOURCE_REFINED}',
                        f'{FOLDER_DESTINATION_REFINED}/{file_out}',
                            data=csv_buffer,
                            length=len(csv_bytes))
