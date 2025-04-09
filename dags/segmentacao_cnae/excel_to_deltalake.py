# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication

def transforma_excel_deltalake_cnae (access_params=None, **kwargs):

    minio_raw = Minio(
    "api-raw.alpe.com.br",
    access_key = 'B7q0avvSIpSdyGPXWnEC',
    secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
    )

        # Connection validation
    try:
        # Try to list the buckets
        
        buckets = minio_raw.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")

    
     # Gerando nome do arquivo para importacao
    BUCKET_SOURCE_RAW = "arquivos-python"
    FOLDER_DESTINATION_RAW = 'depara_cnae'
    file_name = 'Enriquecimento Segmento.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'



    # Conexão com o banco de dados
    conn = connect(
        host='trino.alpe.com.br',
        port=443,
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


    query_segmento = """ select 
                                cnpj_sacado as "CNPJ SACADO",
                                sum(case when nome_cedente = 'ARCELORMITTAL BRASIL S/A' or nome_cedente = 'BELGO BEKAERT ARAMES LTDA' 
                                    or nome_cedente = 'ARCELORMITTAL GONVARRI BRASIL PRODUTOS SIDERURGICOS S/A'  then 1 else 0 end) as casos_matcon,
                                sum(case when nome_cedente = 'CASAL COMERCIO E SERVICOS LTDA' or nome_cedente = 'CASA DO ADUBO S.A' 
                                    or nome_cedente = 'ASUS INDÚSTRIA DE MÁQUINAS AGRÍCOLAS LTDA' or nome_cedente = 'AGRICHEM DO BRASIL S.A'  then 1 else 0 end) as casos_agro,
                                sum(case when nome_cedente not in ( 'CASAL COMERCIO E SERVICOS LTDA', 'CASA DO ADUBO S.A', 'ASUS INDÚSTRIA DE MÁQUINAS AGRÍCOLAS LTDA',
                                                                    'AGRICHEM DO BRASIL S.A', 'ARCELORMITTAL BRASIL S/A', 'BELGO BEKAERT ARAMES LTDA',
                                                                    'ARCELORMITTAL GONVARRI BRASIL PRODUTOS SIDERURGICOS S/A') then 1 else 0 end) as casos_outros
                            from deltalaketrusted.payments.boletos_internos
                            group by cnpj_sacado
                    """

    if conn is not None:
        segmento = execute_query(conn, query_segmento)
        print("Dados carregados com sucesso.")
    else:
        print("A consulta não foi executada porque a conexão com o Trino falhou.")


    df = pd.merge(df, segmento, on = 'CNPJ SACADO', how = 'left')


    df['Raíz CNPJ'] = df['Raíz CNPJ'].astype(str)
    df['CNPJ Completo'] = df['CNPJ Completo'].astype(str)
    df['CNAE'] = df['CNAE'].astype(str).str.replace('.0', '', regex=False)

    # Tratando CNPJ
    df['Raíz CNPJ'] = df['Raíz CNPJ'].astype(str).str.zfill(8)
    df['CNPJ Completo'] = df['CNPJ Completo'].astype(str).str.zfill(14)


    # Criando segmento e sub_segmento
    df['segmento'] = df.apply(
        lambda row: 'MATCON' if row['casos_matcon'] > 0 else 
                    ('AGRO' if row['casos_agro'] > 0 else 
                    ('OUTROS' if row['casos_outros'] > 0 else 'MAPEAR NO SCRIPT')), 
        axis=1
    )


    df['sub_segmento'] = df.apply(
        lambda row: 'COMÉRCIO VAREJISTA' if row['segmento'] == 'MATCON' and row['Divisão Final'] in ['COMÉRCIO VAREJISTA', 'COMÉRCIO E REPARAÇÃO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS'] else
                    ('COMÉRCIO ATACADISTA' if row['segmento'] == 'MATCON' and row['Divisão Final'] == 'COMÉRCIO POR ATACADO, EXCETO VEÍCULOS AUTOMOTORES E MOTOCICLETAS' else
                    ('INCORPORAÇÃO' if row['segmento'] == 'MATCON' and row['Seção Final'] == 'CONSTRUÇÃO' else row['segmento'])),
        axis=1
    )



    df.rename(columns={'Raíz CNPJ': 'raiz_cnpj', 'CNPJ Completo': 'cnpj_completo', 'CNPJ SACADO': 'cnpj',
                   'NOME SACADO': 'nome_sacado', 'CNAE': 'cnae', 'SEÇÃO': 'secao',
                   'DIVISÃO': 'divisao', 'Seção Final': 'secao_final', 'Divisão Final': 'divisao_final'}, inplace=True)
    

    df = df.drop(columns=['casos_matcon', 'casos_agro', 'casos_outros'])


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day

    # Exportando dados para a camada Raw
    
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "cnae/depara-cnae/delta"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}", 
        df, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )