# Carregando libs
import pandas as pd
import joblib
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication


def execucao_modelo(access_params=None):

    # VARIAVEIS DE DOS ARQUIVOS
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/auxiliar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/auxiliar'

    # Conectando na refined
    client = Minio(
        'api-refined.alpe.com.br',
        access_key = '0FKu1vkOJbq0K4C0qRuF',
        secret_key = 'PIqXSinLX2q9XTvGsVrw5Z5jzyuBl7ng7hIq62oA',
    )

    # BAIXANDO ARQUIVO A SER ANALISADO
    file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/LANDING_PRE_FILTRO.csv')
    base_pre_filtro = pd.read_csv(BytesIO(file.data), dtype=str, sep = ';')

    # Separando os casos que seguem analise
    segue_analise_prefiltro = base_pre_filtro[base_pre_filtro['resposta'] == 'SEGUE']

    # Criando uma lista com os CNPJ's do df cnpj_segue_analise, para consulta de dados de HP no lake 
    cnpj_segue_analise = segue_analise_prefiltro['cnpj_raiz']

    ids_query = ', '.join(f"'{cnpj_raiz}'" for cnpj_raiz in cnpj_segue_analise)
    ids_query = f"({ids_query})"

    # Configura a conexão com o Trino
    conn = connect(
        host="trino.alpe.com.br",
        port=443,
        user="trinodados",
        auth=BasicAuthentication("trinodados", "hosgzPvuhyXkP<j}RyT+"),
        http_scheme="https",
    )

    # Cria um cursor e executa a query
    cur = conn.cursor()
    query = (f"""
    select 
        bv.*
    from 
        miniorefined.payments.book_de_variaveis bv
    join (
        select 
            documento_raiz ,
            MAX(year) AS max_year,
            MAX(month) AS max_month,
            MAX(day) AS max_day
        from 
            miniorefined.payments.book_de_variaveis
        where 
            documento_raiz IN {ids_query}
        group by 
            documento_raiz
    ) max_dates on bv.documento_raiz = max_dates.documento_raiz 
        and bv.year = max_dates.max_year
        and bv.month = max_dates.max_month
        and bv.day = max_dates.max_day
    where 
        bv.documento_raiz in {ids_query}
        """)

    cur.execute(query)

    # Obtém os resultados
    rows = cur.fetchall()

    # Fecha o cursor e a conexão
    cur.close()
    conn.close()

    # Para pegar o nome das colunas, você pode usar cur.description
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)

    # PUXANDO ARQUIVO COM O MODELO
    model_file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name='analise_credito/auxiliar/modelo_score_1_arcelor.pkl')
    model_file = model_file.read()
    model_file = BytesIO(model_file)
    model = joblib.load(model_file)

    # Fazer uma cópia do DataFrame original
    base_final = df

    # Remova a coluna 'CNPJ' do DataFrame de entrada
    nova_base = df.drop(columns=['documento_raiz','fornecedor','over_5','ever_10','vop_6_meses','year', 'month','day'], axis=1)

    # Criando colunas temporarias que deverão ser analisadas  (REMOVER QUANDO CONSEGUIRMOS ESSAS COLUNAS)
    nova_base['PCTO_COMPRA_SAFRA_GERAL'] = None
    nova_base['PCTO_COMPRA_SAFRA_GERAL_2SEM'] = None
    nova_base['IDADE'] = None
    nova_base['COD_PORTE_EMPRESA'] = None

    nova_base[['PCTO_COMPRA_SAFRA_GERAL','PCTO_COMPRA_SAFRA_GERAL_2SEM','IDADE','COD_PORTE_EMPRESA']] = nova_base[['PCTO_COMPRA_SAFRA_GERAL','PCTO_COMPRA_SAFRA_GERAL_2SEM','IDADE','COD_PORTE_EMPRESA']].astype(float)

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
            'percentual_pago_em_dia_3_meses':'PERCENTUAL_PAGO_EM_DIA_3M'
            }

    nova_base = nova_base.rename(columns=colunas)

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

    lista_df = [base_pre_filtro,base_final]
    saida_modelo = pd.concat(lista_df, axis=1)

    #GRAVANDO
    # Nome do arquivo CSV de output que subirá para a execução da política
    file_out = f'LANDING_MODELO.csv'

    csv_bytes = saida_modelo.to_csv(index=False, sep=';').encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    client.put_object(f'{BUCKET_SOURCE_REFINED}',
                        f'{FOLDER_DESTINATION_REFINED}/{file_out}',
                            data=csv_buffer,
                            length=len(csv_bytes))
