# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np


def tratamento_faturamento_externo(access_params=None, **kwargs):

    print('Parte 1')

    # Estou extraindo a base da trusted direto do Trino, nao do MinIO

    # Coletando dados da camada Trusted
    # Conectando com o banco

    
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )
    
    '''
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )
    '''

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)

    query_base_fat_externo_trusted = f"""
                                        select *
                                        from deltalaketrusted.payments.faturamento_externo_arcelor
                                    """

    df = execute_query(conn, query_base_fat_externo_trusted)

    print(f"Quantidade de linhas no DataFrame 'dados': {df.shape[0]}")


    print(f"Quantidade de linhas no DataFrame 'dados' nulas:")
    df.isnull().sum()

    print('Parte 2')

    # Criando Indicadores:

    colunas_202 = df.columns[df.columns.str.startswith('202')]

    # Função para obter os últimos 12 meses para calculo dos indicadores
    def obter_ultimos_12_meses():
        # Data atual
        data_atual = datetime.now()
        meses = [(data_atual - timedelta(days=30 * i)).strftime('%Y%m') for i in range(1,13)]
        return meses

    ultimos_12_meses = obter_ultimos_12_meses()
    colunas_ultimos_12_meses = df.filter(items=ultimos_12_meses).columns


    # ->> VOP 2023
    df_VOP_2023 = df[df.filter(like='2023').columns].sum(axis=1)

    # ->> VOP 2024
    df_VOP_2024 = df[df.filter(like='2024').columns].sum(axis=1)

    # ->> VOP 2025
    df_VOP_2025 = df[df.filter(like='2025').columns].sum(axis=1)

    # ->> VOP 2026
    df_VOP_2026 = df[df.filter(like='2026').columns].sum(axis=1)

    # ->> VOP TOTAL
    df_VOP_TOTAL = df[colunas_202].sum(axis=1)

    # ->> VOP ULTIMO 12 MESES
    df_VOP_ULT_12_MESES = df[colunas_ultimos_12_meses].sum(axis=1)


    print('Parte 3')


    # ->> Maximo VOP 2023
    df_MAX_VOP_2023 = df[df.filter(like='2023').columns].max(axis=1)

    # ->> Maximo VOP 2024
    df_MAX_VOP_2024 = df[df.filter(like='2024').columns].max(axis=1)

    # ->> Maximo VOP 2025
    df_MAX_VOP_2025 = df[df.filter(like='2025').columns].max(axis=1)

    # ->> Maximo VOP 2026
    df_MAX_VOP_2026 = df[df.filter(like='2026').columns].max(axis=1)

    # ->> Maximo VOP TOTAL
    df_MAX_VOP_total = df[colunas_202].max(axis=1)

    # ->> Maximo VOP ULTIMO 12 MESES
    df_MAX_VOP_ULT_12_MESES = df[colunas_ultimos_12_meses].max(axis=1)


    print('Parte 3')


    # Calculando Media e Mediana (Considerando apenas os meses com VOP > 0, levando em consideracao que os meses zerados foram erro de preenchimento)

    df_filtrado = df[colunas_202].replace(0, np.nan)

    # --> MEDIANA TOTAL
    df_MEDIANA_TOTAL = df_filtrado.median(axis=1).astype(float)

    # MEDIANA ULTIMO 12 MESES AQUI
    df_MEDIANA_ULT_12_MESES = df_filtrado[colunas_ultimos_12_meses].median(axis=1).astype(float)


    print('Parte 4')


    # ->> MEDIA VOP 2023
    df_MEAN_VOP_2023 = df_filtrado[df_filtrado.filter(like='2023').columns].mean(axis=1)

    # ->> MEDIA VOP 2024
    df_MEAN_VOP_2024 = df_filtrado[df_filtrado.filter(like='2024').columns].mean(axis=1)

    # ->> MEDIA VOP 2025
    df_MEAN_VOP_2025 = df_filtrado[df_filtrado.filter(like='2025').columns].mean(axis=1)

    # ->> MEDIA VOP 2026
    df_MEAN_VOP_2026 = df_filtrado[df_filtrado.filter(like='2026').columns].mean(axis=1)

    # ->> MEDIA VOP TOTAL
    df_MEAN_VOP_total = df_filtrado[df_filtrado.filter(like='202').columns].mean(axis=1)

    # ->> MEDIA VOP ULTIMO 12 MESES
    df_MEAN_VOP_ULT_12_MESES = df_filtrado[colunas_ultimos_12_meses].mean(axis=1)


    # ->> CONTADOR DE FATURAMENTOS INFORMADOS
    df_FAT_INFORM_PERIODO = (df[colunas_202] > 0).sum(axis=1)

    # ->> MEDIA VOP ULTIMO 12 MESES
    df_FAT_INFORM_12_MESES = (df[colunas_ultimos_12_meses] > 0).sum(axis=1)


    print('Parte 5')


    # Inserindo no DF principal

    df['vop_2023'] = round(df_VOP_2023, 2)                             
    df['vop_2024'] = round(df_VOP_2024, 2)
    df['vop_2025'] = round(df_VOP_2025, 2)
    df['vop_2026'] = round(df_VOP_2026, 2)
    df['vop_total'] = round(df_VOP_TOTAL, 2)
    df['vop_ult_12_meses'] = round(df_VOP_ULT_12_MESES, 2)


    df['max_vop_2023'] = round(df_MAX_VOP_2023, 2)
    df['max_vop_2024'] = round(df_MAX_VOP_2024, 2)
    df['max_vop_2025'] = round(df_MAX_VOP_2025, 2)
    df['max_vop_2026'] = round(df_MAX_VOP_2026, 2)
    df['max_vop_total'] = round(df_MAX_VOP_total, 2)
    df['max_vop_ult_12_meses'] = round(df_MAX_VOP_ULT_12_MESES, 2)


    df['media_vop_2023'] = round(df_MEAN_VOP_2023, 2)
    df['media_vop_2024'] = round(df_MEAN_VOP_2024, 2)
    df['media_vop_2025'] = round(df_MEAN_VOP_2025, 2)
    df['media_vop_2026'] = round(df_MEAN_VOP_2026, 2)
    df['media_vop_total'] = round(df_MEAN_VOP_total, 2)
    df['media_vop_ult_12_meses'] = round(df_MEAN_VOP_ULT_12_MESES, 2)

    df['mediana_total'] = round(df_MEDIANA_TOTAL, 2)
    df['mediana_ult_12_meses'] = round(df_MEDIANA_ULT_12_MESES, 2)

    df['qtd_fat_inform_periodo'] = df_FAT_INFORM_PERIODO
    df['qtd_fat_inform_12_meses'] = df_FAT_INFORM_12_MESES


    print('Parte 6')


    df_final = df.filter(items=['raiz_cnpj', 'unidade_consolidada', 'razao_social', 'atualizado_em', 'year', 'month', 'day', 'vop_2023', 'vop_2024', 'vop_2025',
                        'vop_2026', 'vop_total', 'vop_ult_12_meses', 'max_vop_2023', 'max_vop_2024', 'max_vop_2025', 'max_vop_2026',
                        'max_vop_total', 'max_vop_ult_12_meses', 'media_vop_2023', 'media_vop_2024', 'media_vop_2025', 'media_vop_2026',
                        'media_vop_total', 'media_vop_ult_12_meses', 'mediana_total', 'mediana_ult_12_meses', 'qtd_fat_inform_periodo', 'qtd_fat_inform_12_meses'])


    df_final = df_final.fillna(0)

    df_final['razao_social'] = df_final['razao_social'].astype(str)

    print('Exportando base para Refined...')

    df_final = df_final.reset_index(drop=True)


    # Exportando dados para a camada Refined
    # # Conectando na Refined
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = 'payments'
    FOLDER_DESTINATION_REFINED = 'faturamento_externo/arcelor'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        df_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )