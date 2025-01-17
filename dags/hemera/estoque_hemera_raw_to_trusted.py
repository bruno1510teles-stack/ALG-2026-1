# Importando Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from deltalake import write_deltalake, DeltaTable
import os
from concurrent.futures import ThreadPoolExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timezone, timedelta
import re

def hemera_raw_to_trusted(access_params=None, **kwargs):

    client = Minio(
                access_params['endpoint_url_raw'],
                access_key= access_params['aws_access_key_id_raw'],
                secret_key=access_params['aws_secret_access_key_raw'],
                secure=True
            )

    # Definindo bucket e caminho do arquivo
    BUCKET_SOURCE_RAW = "hemera"
    FOLDER_DESTINATION_RAW = 'estoque/year=2024/month=01'

    # Listando os arquivos no diretório
    objects = list(client.list_objects(BUCKET_SOURCE_RAW, prefix=FOLDER_DESTINATION_RAW, recursive=True))

    # Função para ler o arquivo do MinIO e retornar um DataFrame
    def read_file_from_minio(obj):
        file_path = obj.object_name
        print(f"Lendo arquivo: {file_path}")
        
        # Lendo o arquivo do MinIO
        response = client.get_object(BUCKET_SOURCE_RAW, file_path)
        file_data = BytesIO(response.read())
        
        # Assumindo que os arquivos são Excel
        df = pd.read_excel(file_data)
        
        # Extraindo o mês do caminho do arquivo
        match = re.search(r'/month=(\d{2})/', file_path)
        if match:
            mes = match.group(1)
            # Criando a coluna 'data_ref'
            df['data_ref'] = f"2024-{mes}"
        
        return df
    
    # Usar ThreadPoolExecutor para ler arquivos em paralelo
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(read_file_from_minio, objects))

    # Concatenar todos os DataFrames
    df_consolidado = pd.concat(dfs, ignore_index=True)


    # Filtrando colunas
    colunas_desejadas = ['data_ref','Situacao', 'CedenteTipoInscricao', 'CedenteCnpjCpf', 'CedenteNome',
        'NotaPDD', 'SacadoTipoInscricao', 'SacadoCnpjCpf', 'SacadoNome',
        'IdTituloVx', 'TipoAtivo', 'DataEmissao', 'DataAquisicao',
        'DataVencimento', 'NumeroBoletoBanco', 'NumeroTitulo', 'CampoChave',
        'CMC7', 'ValorAquisicao', 'ValorNominalOriginal', 'ValorNominalAtual',
        'ValorPresente', 'PDDNota', 'PDDVencido', 'DataPosicao',
        'DataProrrogacao', 'DataOcorrenciaProrrogacao', 'Coobrigacao',
        'OriginadorCpfCnpj', 'EmpresaConveniadaCnpj', 'SubTipoAtivo', 'Cnae']

    # Filtrando no DataFrame
    df_consolidado = df_consolidado[colunas_desejadas]

    # Tratando base
    #Formatando NumeroTitulo
    df_consolidado['NumeroTitulo'] = df_consolidado['NumeroTitulo'].astype(str).str.zfill(10)

    # Remover espaços em branco nas colunas de texto
    df_consolidado['CedenteNome'] = df_consolidado['CedenteNome'].str.strip()
    df_consolidado['SacadoNome'] = df_consolidado['SacadoNome'].str.strip()
    df_consolidado['NumeroTitulo'] = df_consolidado['NumeroTitulo'].astype(str).str.lstrip('0')
    # Função para converter os números do formato Excel para datas
    def converter_data_excel(data):
        # Verifica se a entrada é um número (formato Excel)
        if isinstance(data, (int, float)):
            # Converte do formato Excel para a data correta
            return pd.to_datetime(data, origin='1899-12-30', unit='D')
        else:
            # Retorna a data original, caso já esteja em formato datetime
            return pd.to_datetime(data, errors='coerce')

    # Aplicar a função à coluna 'DataPosicao'
    df_consolidado['DataPosicao'] = df_consolidado['DataPosicao'].apply(converter_data_excel)
    df_consolidado['DataAquisicao'] = df_consolidado['DataAquisicao'].apply(converter_data_excel)
    df_consolidado['DataVencimento'] = df_consolidado['DataVencimento'].apply(converter_data_excel)
    df_consolidado['pddtotal'] = df_consolidado['PDDNota'] + df_consolidado['PDDVencido']



    # Agrupar por colunas identificadoras do título
    df_agrupado = df_consolidado.groupby(
        ['data_ref','IdTituloVx', 'SacadoCnpjCpf', 'SacadoNome', 'CedenteCnpjCpf', 'CedenteNome'], 
        as_index=False
    ).agg({
        'DataAquisicao': 'min',           # Data mínima de aquisição
        'DataVencimento': 'min',          # Data mínima de vencimento
        'ValorNominalOriginal': 'min',    # Valor nominal original mínimo
        'ValorAquisicao': 'min',           # Valor de aquisição mínimo
        'NumeroTitulo': 'min'
    })

    # Ordenar o DataFrame pela 'DataPosicao' para garantir que os primeiros e últimos valores sejam corretamente identificados
    df_consolidado = df_consolidado.sort_values(by='DataPosicao')

    # Agora vamos agrupar por 'IdTituloVx' (título) para pegar o primeiro e último valor com base na 'DataPosicao'
    df_novos_valores = df_consolidado.groupby(['data_ref','IdTituloVx']).agg(
        estoque_valor_presente_inicial=('ValorPresente', 'first'),  # Pega o primeiro valor com base na DataPosicao
        estoque_valor_presente_final=('ValorPresente', 'last'),      # Pega o último valor com base na DataPosicao
        primeira_data=('DataPosicao', 'first'),  # Pega o primeiro valor com base na DataPosicao
        ultima_data=('DataPosicao', 'last'),
        pdd_inicial=('pddtotal', 'first'),  # Pega o primeiro valor com base na DataPosicao
        pdd_final=('pddtotal', 'last'),   
    ).reset_index()

    # Unir as novas colunas com o DataFrame agrupado original
    df_final = pd.merge(df_agrupado, df_novos_valores, on=['IdTituloVx', 'data_ref'], how='left')
    df_final.reset_index(drop=True, inplace=True)

    data_abertura = df_consolidado['DataPosicao'].min()
    data_fechamento = df_consolidado['DataPosicao'].max()

    # Formatando colunas de estoque

    def formatar_valores_com_base_em_datas(df, coluna_valor, coluna_data, data_referencia, valor_padrao=0):
        return df.apply(lambda row: valor_padrao if row[coluna_data] != data_referencia else row[coluna_valor], axis=1)

    df_final['estoque_valor_presente_inicial'] = formatar_valores_com_base_em_datas(
        df_final, 'estoque_valor_presente_inicial', 'primeira_data', data_abertura)

    df_final['estoque_valor_presente_final'] = formatar_valores_com_base_em_datas(
        df_final, 'estoque_valor_presente_final', 'ultima_data', data_fechamento)

    df_final['pdd_inicial'] = formatar_valores_com_base_em_datas(
        df_final, 'pdd_inicial', 'primeira_data', data_abertura)

    df_final['pdd_final'] = formatar_valores_com_base_em_datas(
        df_final, 'pdd_final', 'ultima_data', data_fechamento)
    
    # Excluir a coluna 'primeira_data'
    df_final = df_final.drop(columns=['primeira_data'])

    # Renomear a coluna 'ultima_data' para 'data_fechamento'
    df_final = df_final.rename(columns={'ultima_data': 'data_fechamento'})
    maior_data = df_final['data_fechamento'].max()
    df_final['data_fechamento'] = maior_data
    df_final['data_fechamento'] = df_final['data_fechamento'] + pd.offsets.MonthEnd(0)

    # Reorganizar as colunas para que 'data_fechamento' seja a primeira
    colunas = ['data_fechamento'] + [col for col in df_final.columns if col != 'data_fechamento']
    df_final = df_final[colunas]

    # Converter a coluna de datas para strings formatadas
    # Tratando Dados
    def converter_para_datetime(df, colunas, formato='%Y-%m-%d'):
        for coluna in colunas:
            try:
                # Tenta converter a coluna para datetime, ignorando erros
                df[coluna] = pd.to_datetime(df[coluna], format=formato, errors='coerce')
                
                # Verifica se a coluna foi convertida corretamente
                if df[coluna].isnull().any():
                    print(f"Alguns valores na coluna '{coluna}' não puderam ser convertidos para datetime.")
                
                # Acessa apenas a parte da data (sem a hora) se for um datetime válido
                df[coluna] = df[coluna].dt.date
            except Exception as e:
                print(f"Erro ao converter a coluna '{coluna}': {e}")
        
        return df
    
    colunas_para_converter_datetime = ['DataAquisicao', 'DataVencimento', 'data_fechamento']
    df_final = converter_para_datetime(df_final, colunas_para_converter_datetime)


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'] = df_final['data_fechamento'].apply(lambda x: x.year if pd.notnull(x) else None)
    df_final['month'] = df_final['data_fechamento'].apply(lambda x: x.month if pd.notnull(x) else None)

    # Conferindo totais
    total_colunas = df_final[['ValorNominalOriginal', 'ValorAquisicao', 'estoque_valor_presente_inicial', 'estoque_valor_presente_final', 'pdd_inicial', 'pdd_final']].sum()
    total_colunas = total_colunas.apply(lambda x: f"{x:,.2f}")
    print(total_colunas)


    ## Renomeando colunas
    df_final.rename(columns={
        'SacadoCnpjCpf': 'cnpj_sacado',
        'SacadoNome': 'nome_sacado',
        'CedenteCnpjCpf': 'cnpj_cedente',
        'CedenteNome': 'nome_cedente', 
        'IdTituloVx': 'id_titulo',
        'NumeroTitulo': 'numero_titulo',
        'DataAquisicao': 'data_aquisicao',
        'DataVencimento': 'data_vencimento',
        'ValorNominalOriginal': 'valor_nominal_original',
        'ValorAquisicao': 'valor_aquisicao'
    }, inplace=True)

    # Remover 'numero_titulo' da posição original
    colunas = df_final.columns.tolist()
    colunas.remove('numero_titulo')

    # Inserir 'numero_titulo' na terceira posição (índice 2)
    colunas.insert(2, 'numero_titulo')

    # Reorganizar o DataFrame com as colunas na nova ordem
    df_final = df_final[colunas]


    # Padronizando Outputs
    df_final['data_fechamento'] = pd.to_datetime(df_final['data_fechamento']).dt.strftime('%Y-%m-%d')
    df_final['data_aquisicao'] = pd.to_datetime(df_final['data_aquisicao']).dt.strftime('%Y-%m-%d')
    df_final['data_vencimento'] = pd.to_datetime(df_final['data_vencimento']).dt.strftime('%Y-%m-%d')
    df_final['id_titulo'] = df_final['id_titulo'].astype(str).str.zfill(10)
    df_final['numero_titulo'] = df_final['numero_titulo'].astype(str).str.zfill(10)
    df_final['valor_aquisicao'] = pd.to_numeric(df_final['valor_aquisicao'], errors='coerce').round(2)
    df_final['valor_nominal_original'] = pd.to_numeric(df_final['valor_nominal_original'], errors='coerce').round(2)
    df_final['estoque_valor_presente_inicial'] = pd.to_numeric(df_final['estoque_valor_presente_inicial'], errors='coerce').round(2)
    df_final['estoque_valor_presente_final'] = pd.to_numeric(df_final['estoque_valor_presente_final'], errors='coerce').round(2)
    df_final['pdd_inicial'] = pd.to_numeric(df_final['pdd_inicial'], errors='coerce').round(2)
    df_final['pdd_final'] = pd.to_numeric(df_final['pdd_final'], errors='coerce').round(2)
    df_final['year'] = df_final['year'].astype(str)
    df_final['month'] = df_final['month'].astype(str)
    df_final['atualizado_em'] = pd.to_datetime(df_final['atualizado_em']).dt.strftime('%Y-%m-%d %H:%M:%S')





    # Exportando dados para a camada Trusted
    print('Salvando Arquivo')   
    storage_options_trusted = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "hemera"
    FOLDER_DESTINATION_TRUSTED = "estoque"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        df_final, 
        partition_by=["year", "month"],
        storage_options=storage_options_trusted,
        mode="append"
    )

    print('Arquivo salvo com sucesso!')