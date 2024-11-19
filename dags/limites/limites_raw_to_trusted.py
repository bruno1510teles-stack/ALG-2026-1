import deltalake
import pandas as pd
import minio as Minio
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from airflow.utils.log.logging_mixin import LoggingMixin


def limites_raw_to_trusted(access_params=None, **kwargs):

    # Coletando dados
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_raw'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_raw'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_raw']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Defina o caminho do arquivo Delta
    delta_path = "s3://limites/"

    # Leia os dados da tabela Delta (última versão)
    delta_table = DeltaTable(delta_path, storage_options=storage_options)

    # Converta para um DataFrame Pandas
    df = delta_table.to_pandas()

    # Filtrando colunas
    colunas_desejadas = ['cnpj_sacado', 'limite_atribuido', 'limite_utilizado', 'limite_disponivel', 'cedente']

    # Filtrando no DataFrame
    df = df[colunas_desejadas]

    # Tratando cnpj_sacado
    df['cnpj_sacado'] = df['cnpj_sacado'].astype(str)
    df['cnpj_sacado'] = df['cnpj_sacado'].str.zfill(14)

    # Puxando base de UF SACADO
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
    
    # Extraindo os CNPJs do DataFrame 'df' e convertendo-os para uma lista
    cnpjs = df['cnpj_sacado'].unique().tolist()

    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str = ', '.join([f"'{cnpj}'" for cnpj in cnpjs])

    query_sacado = f"""
        select 
            est.documento_sem_formatacao as cnpj_sacado, est.nome_fantasia as nome_sacado, est.uf, mun.descricao as nome_cidade
        from 
            deltalaketrusted.receita_federal.estabelecimentos est
            left join deltalaketrusted.receita_federal.municipios mun on est.municipio = mun.codigo 
        where 
            est.documento_sem_formatacao IN ({cnpjs_str})
    """
    sacado = execute_query(conn, query_sacado)
    print(f"Quantidade de linhas no DataFrame 'sacado': {sacado.shape[0]}")

    # Cruzando Bases
    df_final = pd.merge(df, sacado, on='cnpj_sacado', how='left')
    df_final.reset_index(drop=True, inplace=True)


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    # Exiba o DataFrame
    print(df_final)

    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options_trusted = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_TRUSTED = "limites"
        FOLDER_DESTINATION_TRUSTED = "limite"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
            df_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options_trusted,
            mode="overwrite"
        )
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")