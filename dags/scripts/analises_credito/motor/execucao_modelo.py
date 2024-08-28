# Carregando libs
import pandas as pd
import joblib
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from io import BytesIO
import os
import base64, requests, sys, json

def execucao_modelo(access_params=None):

    # VARIAVEIS DE DOS ARQUIVOS
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/auxiliar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/auxiliar'

    # Conectando na refined
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key=access_params['aws_access_key_id_refined'],
        secret_key=access_params['aws_secret_access_key_refined'],
    )

    dtype = {'cnpj_raiz':str,
        'documento_sem_formatacao':str,
        'razao_social':str,
        'cod_cnae':str,
        'cod_natureza_juridica':str
    }

    # BAIXANDO ARQUIVO A SER ANALISADO
    file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/LANDING_PRE_FILTRO.csv')
    base_pre_filtro = pd.read_csv(BytesIO(file.data), dtype=dtype, sep = ';')
    
    # Separando os casos que seguem analise
    segue_analise_prefiltro = base_pre_filtro[base_pre_filtro['resposta'] == 'SEGUE']

    print(segue_analise_prefiltro)

    # Criando uma lista com os CNPJ's do df cnpj_segue_analise, para consulta de dados de HP no lake 
    cnpj_segue_analise = segue_analise_prefiltro['cnpj_raiz']

    ids_query = ', '.join(f"'{cnpj_raiz}'" for cnpj_raiz in cnpj_segue_analise)
    ids_query = f"({ids_query})"

    print(ids_query)
    
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    # Cria um cursor e executa a query
    cur = conn.cursor()
    query = (f"""
        select
            documento_raiz cnpj_raiz,
            fornecedor,
            prazo_medio_geral,
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
            percentual_compra_recorrente_3_meses
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY documento_raiz, fornecedor ORDER BY dataprocessamento DESC) AS row_num
            FROM miniorefined.payments.book_variaveis
        ) t
        WHERE row_num = 1 and documento_raiz in {ids_query}
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
    df = pd.DataFrame(rows, columns=columns)
    
    print('rodou a query')

    
    df = df.merge(base_pre_filtro[['cnpj_raiz', 'idade', 'codigo_porte_empresa']], on='cnpj_raiz', how='left')

    print('puxando modelo')
    # PUXANDO ARQUIVO COM O MODELO
    model_file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name='analise_credito/auxiliar/modelo_score_1_arcelor.pkl')
    model_file = model_file.read()
    model_file = BytesIO(model_file)
    
    print('carregando modelo')
    model = joblib.load(model_file)
    print('modelo carregado')

    # Fazer uma cópia do DataFrame original
    base_final = df

    # Remova a coluna 'CNPJ' do DataFrame de entrada
    nova_base = df.drop(columns=['cnpj_raiz','fornecedor','over_5','ever_10','vop_6_meses'], axis=1)
      

    #ALTERANDO NOME DAS COLUNAS PARA SEREM DE ACORDO COM O MODELO
    colunas = {'prazo_medio_geral':'PRAZO_MEDIO_GERAL',
            'prazo_medio_3_meses':'PRAZO_MEDIO_3M',
            'alavancagem_data_analise':'ALAVANCAGEM_DATA_ANALISE',
            'alavancagem_media_historica':'ALAVANCAGEM_MEDIA_HISTORICA',
            'media_diferenca_dias_pedidos':'MEDIA_DIFERENCA_DIAS_PEDIDOS',
            'media_diferenca_dias_pedidos_3_meses':'MEDIA_DIFERENCA_DIAS_PEDIDOS_3M',
            'maior_atraso_em_dias':'MAIOR_ATRASO_EM_DIAS',
            'maior_atraso_em_dias_3_meses':'MAIOR_ATRASO_EM_DIAS_3M',
            'percentual_pago_em_dia':'PERCENTUAL_PAGO_EM_DIA',
            'percentual_pago_em_dia_3_meses':'PERCENTUAL_PAGO_EM_DIA_3M',
            'percentual_compra_recorrente_geral':'PCTO_COMPRA_SAFRA_GERAL',
            'percentual_compra_recorrente_3_meses': 'PCTO_COMPRA_SAFRA_GERAL_2SEM',
            'idade':'IDADE',
            'codigo_porte_empresa':'COD_PORTE_EMPRESA'
            }
 

    nova_base = nova_base.rename(columns=colunas)

    dtype = {'PRAZO_MEDIO_GERAL':float,
            'PRAZO_MEDIO_3M':float,
            'ALAVANCAGEM_DATA_ANALISE':float,
            'ALAVANCAGEM_MEDIA_HISTORICA':float,
            'MEDIA_DIFERENCA_DIAS_PEDIDOS':float,
            'MEDIA_DIFERENCA_DIAS_PEDIDOS_3M':float,
            'MAIOR_ATRASO_EM_DIAS':float,
            'MAIOR_ATRASO_EM_DIAS_3M':float,
            'PERCENTUAL_PAGO_EM_DIA':float,
            'PERCENTUAL_PAGO_EM_DIA_3M':float,
            'PCTO_COMPRA_SAFRA_GERAL':float,
            'PCTO_COMPRA_SAFRA_GERAL_2SEM': float,
            'IDADE':float,
            'COD_PORTE_EMPRESA':float
            }
    nova_base = nova_base.astype(dtype)

    # Faça as previsões com base nos dados de entrada
    score = model.predict_proba(nova_base)[:, 1]


    # Adicione as colunas de scores ao DataFrame base_final
    base_final['Score_Model_Geral'] = score
    # Criar uma função para categorizar os valores (faixa)
    def categorizar_faixa(score):
        if score <= 0.098267:
            return 'A'
        elif score <= 0.287503:
            return 'B'
        elif score <= 0.602975:
            return 'C'
        elif score <= 0.808356:
            return 'D'
        else:
            return 'E'

    # Aplicar a função para criar a classificação
    base_final['CLASSIFICACAO'] = base_final['Score_Model_Geral'].apply(categorizar_faixa)


    saida_modelo = base_pre_filtro.merge(base_final, on='cnpj_raiz', how='left')
    
    print(saida_modelo)
 

    #GRAVANDO
    # Nome do arquivo CSV de output que subirá para a execução da política
    file_out = f'LANDING_MODELO.csv'

    csv_bytes = saida_modelo.to_csv(index=False, sep=';').encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    client.put_object(f'{BUCKET_SOURCE_REFINED}',
                        f'{FOLDER_DESTINATION_REFINED}/{file_out}',
                            data=csv_buffer,
                            length=len(csv_bytes))
