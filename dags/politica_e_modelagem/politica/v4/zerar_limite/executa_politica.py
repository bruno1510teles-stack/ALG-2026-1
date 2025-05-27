# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from io import BytesIO
import os, re, pytz
from confluent_kafka import Producer
import json
import requests
import base64
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import numpy as np
import logging



def execucao_politica(access_params=None,  **kwargs):

    print('Capturando base da task anterior...')

    # Pegando DF tarefa anterior
    # Recupera o objeto ti (task instance) via kwargs
    ti = kwargs['ti']
    saida_modelo_dict = ti.xcom_pull(task_ids='captura_proposta')
    saida_modelo = pd.DataFrame(saida_modelo_dict)

    #Ajustando a formatação do CNPJ para 14 digitos
    saida_modelo['CNPJ'] = saida_modelo['CNPJ'].astype(str).str.zfill(14)
    saida_modelo['cnpj_sacado_raiz'] = saida_modelo['CNPJ'].astype(str).str[:8]

    # Criando Parecer Personalizado

    # Extraindo os CNPJs do DataFrame 'limites' e convertendo-os para uma lista
    cnpjs = saida_modelo['cnpj_sacado_raiz'].unique().tolist()
    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str = ', '.join([f"'{cnpj}'" for cnpj in cnpjs])

    print(saida_modelo)

    print('Conectando no banco de dados e processando queries...')

    # Conectando com o Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    # Função para execução da query
    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    
    # Query Situação Receita
    query_receita =  f""" 
                    select distinct 
                        cnpj_raiz as cnpj_sacado_raiz,
                        situacao_cadastral
                    from deltalaketrusted.receita_federal.estabelecimentos
                    where is_matriz = true
                    and cnpj_raiz in ({cnpjs_str})
                    """

    receita = execute_query(conn, query_receita)


    # Query PEP e RJ
    query_pep_rj =  f""" 
                    select 
                        cnpj_raiz as cnpj_sacado_raiz,
                        max(situacao_especial) as situacao_especial,
                        max(tem_pep) as tem_pep
                    from deltalakerefined.motor.pre_filtro
                    where situacao_especial = 'RECUPERACAO JUDICIAL'
                    and cnpj_raiz in ({cnpjs_str})
                    or tem_pep = true
                    group by cnpj_raiz
                    """

    pep_rj = execute_query(conn, query_pep_rj)

    # Cruzando os DFs
    saida_modelo = pd.merge(saida_modelo, receita, on='cnpj_sacado_raiz', how='left')

    saida_modelo = pd.merge(saida_modelo, pep_rj, on='cnpj_sacado_raiz', how='left')

    print('Criando parecer de acordo com a situação do CNPJ...')

    # Função para criar o parecer da proposta
    def definir_motivo(row):
        if row['situacao_cadastral'] != 'ATIVA':
            return row['situacao_cadastral']
        elif row['situacao_especial'] == 'RECUPERACAO JUDICIAL':
            return row['situacao_especial']
        elif row['tem_pep'] == True:
            return 'PEP'
        else:
            return 'NAO ENCONTRADO'

    # Criando a nova coluna 'motivo'
    saida_modelo['parecer'] = saida_modelo.apply(definir_motivo, axis=1)

    # Buscando os nomes dos socios PEP
    # Query Socios
    query_socios =  f""" 
                    select 
                        "nome/razao_social" as nome_socio_receita,
                        cnpj_raiz as cnpj_sacado_raiz,
                        documento_socio as documento
                    from deltalaketrusted.receita_federal.socios
                    where cnpj_raiz in ({cnpjs_str})
                    """

    socios = execute_query(conn, query_socios)

    # Query Socios PEP
    query_socios_pep =  f""" 
                        select distinct
                            nome as nome_socio,
                            REPLACE(REPLACE(documento, '.', ''), '-', '') as documento,
                            'PEP' as flag_pep
                        from deltalaketrusted.pessoas_e_organizacoes.pep
                        """

    socios_pep = execute_query(conn, query_socios_pep)

    socios_pep_final = pd.merge(socios, socios_pep, on='documento', how='left')

    socios_pep_final['flag_pep'] = socios_pep_final['flag_pep'].fillna(' ')

    print(socios_pep_final)

    # Definindo apenas um socio PEP para ser referenciado no parecer (MAX do VARCHAR)
    df_pep = saida_modelo[saida_modelo['parecer'] == 'PEP']

    df_pep_merge = pd.merge(df_pep, socios_pep_final[socios_pep_final['flag_pep'] != ' '], on='cnpj_sacado_raiz', how='left')

    df_pep_merge = df_pep_merge.groupby('cnpj_sacado_raiz')['nome_socio'].max().reset_index()


    # Cruzando com a base de propostas
    saida_modelo = pd.merge(saida_modelo, df_pep_merge, on='cnpj_sacado_raiz', how='left')

    saida_modelo['nome_socio'] = saida_modelo['nome_socio'].fillna(' ')


    # Dropando colunas desnecessarias e deixando o parecer pronto
    saida_modelo.loc[saida_modelo['parecer'] == 'PEP', 'parecer'] = saida_modelo['parecer'].astype(str) + " - " + saida_modelo['nome_socio'].astype(str)

    saida_modelo = saida_modelo.drop(['situacao_cadastral', 'situacao_especial', 'tem_pep', 'nome_socio'], axis=1)

    print('DataFrame de saida:')

    print(saida_modelo)

    resposta_motor = saida_modelo

    resposta_motor['resposta_motor'] = 'REPROVADO'    
    
    resposta_motor['ramificacao_motor'] = 'REPROVADO'

    resposta_motor = resposta_motor.rename(columns={'CNPJ':'cnpj_ec'})

    #DEFININDO O PARECER DO MOTOR
    resposta_motor.loc[resposta_motor['resposta_motor'] == 'REPROVADO', 'parecer'] = 'Motor - Recusado | ' + resposta_motor['parecer'].astype(str)

    resposta_motor.head()

    # Gerando Path
    def gerando_path(row):
           current_date = datetime.now().strftime('%Y-%m-%d')
           return f"{row['cnpj_sacado_raiz']}/{current_date}-{row['pgid']}-{row['issue_jira']}"
    # Aplicando o Path
    resposta_motor['path_arquivos_minio'] = resposta_motor.apply(gerando_path, axis = 1)
    resposta_motor['url'] = 'https://minio-datalake.alpe.com.br/raw/browser/analise-credito/' + resposta_motor['path_arquivos_minio'].astype(str) + '/'


    resposta_motor_resumida = resposta_motor[['issue_jira', 'resposta_motor', 'cnpj_ec', 'parecer', 'ramificacao_motor', 'url']].rename(columns={
    'cnpj_ec': 'cnpj_ec',
    'resposta_motor': 'resolucao',
    'ramificacao_motor': 'ramificacao',
    'url' : 'path_arquivos_minio'
    })

    resposta_motor_resumida['valor_aprovado'] = 0

    #depois do path
    resposta_motor_resumida = resposta_motor_resumida[['issue_jira', 'resolucao', 'cnpj_ec', 'valor_aprovado', 'parecer', 'ramificacao', 'path_arquivos_minio']]

    print("Quantidade de CNPJs por ramificação:")
    print(resposta_motor_resumida.groupby(['issue_jira','cnpj_ec','ramificacao'])['cnpj_ec'].size())

    print("Quantidade de CNPJs por parecer:")
    print(resposta_motor_resumida.groupby(['issue_jira','cnpj_ec','parecer'])['cnpj_ec'].size())


    resposta_motor_resumida.head()


# Itera sobre cada combinação de 'issue_jira' e 'CNPJ' no DataFrame
    for _, row in resposta_motor.iterrows():
        issue_jira = row['issue_jira']
        cnpj = row['cnpj_ec']
             
        # Gera o nome base do arquivo combinando 'issue_jira' e 'CNPJ'
        file_base_name = 'Resposta_Motor'

        # Filtra o DataFrame resumido e detalhado para o CNPJ específico
        df_detalhado = resposta_motor[resposta_motor['cnpj_ec'] == cnpj]

        # Adiciona mensagens de log para depuração
        print(f"Processando CNPJ: {cnpj}")
        print(f"Detalhado DF: {df_detalhado.shape}")

        # Gera o caminho de saída usando a coluna 'path'
        file_out_detalhado = f'{row["path_arquivos_minio"]}/{file_base_name}.csv'
              
        # Salva a análise detalhada
        csv_bytes_detalhado = df_detalhado.to_csv(index=False, sep=';').encode('utf-8')
        csv_buffer_detalhado = BytesIO(csv_bytes_detalhado)

        # Salva o arquivo no bucket MinIO usando o caminho gerado
        # # Conectando na refined
        minio_raw = Minio(
            access_params['endpoint_url_raw'],
            access_key=access_params['aws_access_key_id_raw'],
            secret_key=access_params['aws_secret_access_key_raw'],
        )

        BUCKET_SOURCE_RAW = "analise-credito"

        minio_raw.put_object(
            BUCKET_SOURCE_RAW,
            file_out_detalhado,
            data=csv_buffer_detalhado,
            length=len(csv_bytes_detalhado)
    )


    return resposta_motor_resumida.to_dict(orient='records')