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


def execucao_politica(access_params=None,  **kwargs):
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/auxiliar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/out'

    # Pegando DF tarefa anterior
    # Recupera o objeto ti (task instance) via kwargs
    ti = kwargs['ti']
    saida_modelo_dict = ti.xcom_pull(task_ids='captura_proposta')
    saida_modelo = pd.DataFrame(saida_modelo_dict)

    #Ajustando a formatação do CNPJ para 14 digitos
    saida_modelo['cnpj_raiz'] = saida_modelo['CNPJ'].astype(str).str.zfill(8)

    resposta_motor = saida_modelo

    resposta_motor['resposta_motor'] = 'MESA'    
    
    resposta_motor['ramificacao_motor'] = 'MESA'

    resposta_motor = resposta_motor.rename(columns={'documento_sem_formatacao':'cnpj_ec'})

    #DEFININDO O PARECER DO MOTOR
    resposta_motor.loc[resposta_motor['resposta_motor'] == 'MESA', 'parecer'] = 'Motor - Direcionar para avaliação da mesa de crédito'


    # Gerando Path
    def gerando_path(row):
           current_date = datetime.now().strftime('%Y-%m-%d')
           return f"{row['cnpj_raiz']}/{current_date}-{row['pgid']}-{row['issue_jira']}"
    # Aplicando o Path
    resposta_motor['path_arquivos_minio'] = resposta_motor.apply(gerando_path, axis = 1)
    resposta_motor['url'] = 'https://minio-datalake.alpe.com.br/raw/browser/analise-credito/' + resposta_motor['path_arquivos_minio'] + '/'


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