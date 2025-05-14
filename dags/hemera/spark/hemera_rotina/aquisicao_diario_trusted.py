# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
import os
import tempfile
import msoffcrypto
import re
import numpy as np
from trino.dbapi import connect
from trino.auth import BasicAuthentication


def aquisicao_diario_trusted (access_params=None, **kwargs):

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
    

    # Configurações
    BUCKET_NAME = "hemera-csv"
    PREFIX = "aquisicao/"
    ALL_DATA = []

    # Define a data de hoje no fuso horário UTC−3
    data_hoje = (datetime.now(timezone.utc) - timedelta(hours=3)).date()
    print(f"📅 Lendo arquivos CSV modificados em: {data_hoje} (UTC−3)")

    # Lista e lê os arquivos CSV no bucket
    objects = minio_raw.list_objects(BUCKET_NAME, prefix=PREFIX, recursive=True)

    for obj in objects:
        file_key = obj.object_name

        # Ajusta a data de modificação para UTC−3
        modified_date = (obj.last_modified - timedelta(hours=3)).date()

        # Filtro por data
        if modified_date != data_hoje:
            print(f"⏭️ Ignorando {file_key} (modificado em: {modified_date})")
            continue

        print(f"📥 Lendo CSV: {file_key}")

        try:
            response = minio_raw.get_object(BUCKET_NAME, file_key)
            csv_data = BytesIO(response.read())

            df = pd.read_csv(csv_data, dtype=str, low_memory=False)
            df["source_file"] = file_key
            ALL_DATA.append(df)

        except Exception as e:
            print(f"⚠️ Erro ao ler {file_key}: {e}")

    # Empilha todos os DataFrames
    if ALL_DATA:
        final_df = pd.concat(ALL_DATA, ignore_index=True)
        print("✅ Dados empilhados com sucesso!")
        print(f"📂 Total de arquivos carregados: {len(ALL_DATA)}")
    else:
        print("⚠️ Nenhum dado foi carregado.")

    print("📄 Arquivos carregados:")
    for df in ALL_DATA:
        print(f"   - {df['source_file'].iloc[0]}")

    

    # Tratando base

    print('Tratando base para inserção na Trusted...')

    def converter_data_excel(data):
        # Verifica se a entrada é um número (formato Excel)
        if isinstance(data, (int, float)):
            # Converte do formato Excel para a data correta
            return pd.to_datetime(data, origin='1899-12-30', unit='D')
        else:
            # Retorna a data original, caso já esteja em formato datetime
            return pd.to_datetime(data, errors='coerce')
        

    final_df['DataVencimento'] = final_df['DataVencimento'].apply(converter_data_excel)
    final_df['DataEmissao'] = final_df['DataEmissao'].apply(converter_data_excel)
    final_df['DataAquisicao'] = final_df['DataAquisicao'].apply(converter_data_excel)

    if 'TIT_DATA_AQUISICAO' in final_df.columns:
        final_df['TIT_DATA_AQUISICAO'] = final_df['TIT_DATA_AQUISICAO'].apply(converter_data_excel)
    else:
        print("⚠️ Coluna 'TIT_DATA_AQUISICAO' não encontrada. Criando coluna com valores nulos.")
        final_df['TIT_DATA_AQUISICAO'] = np.nan


    # Extrai o padrão de data do nome do arquivo e cria a coluna data_arquivo
    final_df["data_arquivo"] = final_df["source_file"].apply(
        lambda x: re.search(r"\d{2}\.\d{2}\.\d{2}", x).group() if re.search(r"\d{2}\.\d{2}\.\d{2}", x) else None
    )

    # Converte para datetime
    final_df["data_arquivo"] = pd.to_datetime(final_df["data_arquivo"], format="%d.%m.%y", errors="coerce")


    # Regra de negócio de produto
    final_df["produto"] = final_df["CedenteCnpjCpf"].map({
        "28.494.032/0001-00": "VENDERMAIS",
        "35.914.008/0001-48": "BLIPS"
    }).fillna("TRADICIONAL")

    # Garantir que 'IdTituloVx' tenha exatamente 10 caracteres, completando com zeros à esquerda se necessário
    final_df['IdTituloVx'] = final_df['IdTituloVx'].astype(str).str.zfill(10)

    # Formatar para ter 2 casas decimais
    final_df['ValorAquisicao'] = pd.to_numeric(final_df['ValorAquisicao'], errors='coerce').round(2)
    final_df['ValorNominal'] = pd.to_numeric(final_df['ValorNominal'], errors='coerce').round(2)
    final_df['CMC7'] = pd.to_numeric(final_df['CMC7'], errors='coerce').round(2)


    # Renomear a coluna 'ultima_data' para 'data_fechamento'
    data_fechamento = final_df['data_arquivo'] + pd.offsets.MonthEnd(0)
    final_df['data_fechamento'] = data_fechamento


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    final_df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    final_df['year'] = final_df['data_fechamento'].apply(lambda x: x.year if pd.notnull(x) else None)
    final_df['month'] = final_df['data_fechamento'].apply(lambda x: x.month if pd.notnull(x) else None)


    ## Filtrando apenas colunas necessárias e renomando-as
    final_df.rename(columns={
        "CedenteCnpjCpf":"cnpj_cedente",     
        "CedenteNome":"nome_cedente",      
        "SacadoCnpjCpf":"cnpj_sacado",     
        "SacadoNome":"nome_sacado",     
        "IdTituloVx":"id_titulo",     
        "TipoAtivo":"tipo_titulo",     
        "DataEmissao":"data_emissao",     
        "DataAquisicao":"data_aquisicao",     
        "DataVencimento":"data_vencimento",     
        "NumeroBoletoBanco":"numero_boleto_banco",     
        "NumeroTitulo":"numero_titulo",     
        "CampoChave":"campo_chave",
        "CMC7":"cmc7",      
        "ValorAquisicao":"valor_aquisicao",
        "ValorNominal":"valor_nominal",
        "TIT_DATA_AQUISICAO":"tit_data_aquisicao",            
        "Coobrigacao":"coobrigacao",
        "OriginadorCpfCnpj":"cpf_cnpj_originador",
        "Taxa":"taxa",
        "Cnae":"cnae"
    }, inplace=True)

    colunas_finais = ['data_fechamento', 'data_arquivo', 'cnpj_cedente', 'nome_cedente', 'cnpj_sacado', 'nome_sacado', 'id_titulo', 'tipo_titulo', 'data_emissao',
                    'data_aquisicao', 'data_vencimento', 'numero_boleto_banco', 'numero_titulo', 'campo_chave', 'cmc7', 'valor_aquisicao', 'valor_nominal', 'tit_data_aquisicao',
                    'coobrigacao', 'cpf_cnpj_originador', 'taxa', 'cnae', 'produto', 'atualizado_em', 'year', 'month']

    final_df = final_df[colunas_finais]



    # Coletando dados da camada Trusted para remoção das linhas que serão atualizadas
    # Conectando com o banco
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

    query_aquisicao = f"""
                        select *
                        from deltalaketrusted.hemera_trusted.aquisicao_consolidado
                    """

    aquisicao_atual = execute_query(conn, query_aquisicao)



    # Datas para desconsiderarmos da base da Trusted
    datas_para_remover = final_df['data_arquivo'].unique()

    print('Datas consideradas para atualização:')
    print(datas_para_remover)



    # Normalizando campos que vamos fazer a validação
    final_df['data_arquivo'] = pd.to_datetime(final_df['data_arquivo']).dt.tz_localize(None).dt.floor('S')
    aquisicao_atual['data_arquivo'] = pd.to_datetime(aquisicao_atual['data_arquivo']).dt.tz_localize(None).dt.floor('S')

    print('Linhas antes da remoção:')
    print(len(aquisicao_atual))

    # Remover do DataFrame as linhas com essas datas
    aquisicao_atual = aquisicao_atual[~aquisicao_atual['data_arquivo'].isin(datas_para_remover)]

    print('Linhas após a remoção:')
    print(len(aquisicao_atual))


    # Incluindo dados novos e gerando tabela final
    aquisicao_final = pd.concat([aquisicao_atual, final_df], ignore_index=True)

    print('Linhas após a inserção de novas linhas:')
    print(len(aquisicao_final))

    # Voltando o campo de validação para o mesmo tipo de dados
    aquisicao_final['data_arquivo'] = pd.to_datetime(aquisicao_final['data_arquivo']).dt.floor('D')
    aquisicao_final['data_arquivo'] = aquisicao_final['data_arquivo'].dt.tz_localize('UTC', nonexistent='NaT', ambiguous='NaT')

    print(aquisicao_final)

    print('Salvando dados na Trusted...')

    storage_options_trusted = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "hemera-trusted"
    FOLDER_DESTINATION_TRUSTED = "aquisicao/delta"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        aquisicao_final,
        storage_options=storage_options_trusted,
        mode="overwrite"
    )

    print('Arquivo salvo com sucesso!')