# Carregando libs
import pandas as pd
import pyarrow as pa
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable
import psutil
from scripts.query_trino_payments import query_trino



# Criando conexão
def pre_filtro(access_params):

    print(f"começou função {psutil.virtual_memory()._asdict()}")
    
    # Variaveis Conexão
    BUCKET_SOURCE_REFINED = "payments"
    REFINED_FOLDER = "motor/pre_filtro/"

    print(f'declarando client {psutil.virtual_memory()._asdict()}')
    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key = access_params['aws_access_key_id_refined'],
        secret_key = access_params['aws_secret_access_key_refined'],
    )
    
    print(f'Rodando query {psutil.virtual_memory()._asdict()}')
    
    query = f""" with 

    tem_pep as (select 
        distinct
        cnpj_raiz,
        true as tem_pep
    from miniotrusted.pessoas_e_organizacoes.pep pep
    join miniotrusted.receita_federal.socios socios on replace(replace(documento, '.', ''), '-', '') = socios.documento_socio and pep.nome = socios."nome/razao_social"
    where socios.data_ref = (select max(data_ref) data_ref from miniotrusted.receita_federal.socios)
    and pep.data_ref = (select max(data_ref) data_ref from miniotrusted.pessoas_e_organizacoes.pep)
    )

    select 
        est.cnpj_raiz cnpj_raiz,
        est.documento_sem_formatacao documento_sem_formatacao,
        est.cnae_principal cod_cnae,
        emp.natureza_juridica cod_natureza_juridica,
        (date_diff('day', date(est.data_inicio_atividade), date(now())) / 365.00) AS idade,
        est.situacao_cadastral situacao_cadastral,
        sim.is_mei is_mei,
        coalesce(tem_pep, false) tem_pep,
        est.data_ref data_ref_receita
        
    from miniotrusted.receita_federal.estabelecimentos est 
    left join miniotrusted.receita_federal.empresas emp on emp.cnpj_raiz = est.cnpj_raiz and est.data_ref = emp.data_ref
    left join miniotrusted.receita_federal.simples sim on sim.cnpj_raiz = est.cnpj_raiz and est.data_ref = sim.data_ref
    left join miniotrusted.receita_federal.naturezas_juridicas natjur on natjur.codigo = emp.natureza_juridica and est.data_ref = natjur.data_ref
    left join miniotrusted.receita_federal.cnaes on cnaes.codigo = est.cnae_principal and est.data_ref = cnaes.data_ref
    left join tem_pep on tem_pep.cnpj_raiz = est.cnpj_raiz

    where est.data_ref = (select max(data_ref) data_ref from miniotrusted.receita_federal.estabelecimentos)

"""
    

    print(f"query: {query} {psutil.virtual_memory()._asdict()}")
    
    df = query_trino(query, 
                                access_params['trino_endpoint'],
                                access_params['trino_port'],
                                access_params['trino_user'],
                                access_params['trino_password'])

    print(f'query carregada {psutil.virtual_memory()._asdict()}')
    
    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    print(f'acrescentando coluna de data{psutil.virtual_memory()._asdict()}')
    
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')

    df['year'] = now.year
    df['month'] = now.month
    df['day'] = now.day    
    
    print(f'tornando false colunas booleanas nulas {psutil.virtual_memory()._asdict()}')
    colunas_bool = ['is_mei', 'tem_pep']
    for coluna in colunas_bool:
        df.loc[df[coluna].isna(), coluna] = False
    
    dict_types = {'cnpj_raiz': str,
              'documento_sem_formatacao': str,
              'cod_cnae': str, 
              'cod_natureza_juridica': str,
              'idade': float,
              'situacao_cadastral': str,
              'is_mei': bool,
              'tem_pep': bool,
              'data_ref_receita': str,
              'atualizado_em': str,
              'year': int,
              'month': int,
              'day':int}  
    
    print(f'definindo tipo das colunas {psutil.virtual_memory()._asdict()}')
    
    df = df.astype(dict_types)

        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    schema = pa.schema([
            ('cnpj_raiz', pa.string()),
              ('documento_sem_formatacao', pa.string()),
              ('cod_cnae', pa.string()), 
              ('cod_natureza_juridica', pa.string()),
              ('idade', pa.float64),
              ('situacao_cadastral', pa.string()),
              ('is_mei', pa.bool_),
              ('tem_pep', pa.bool_),
              ('data_ref_receita', pa.string()),
              ('atualizado_em', pa.string()),
              ('year', pa.int32()),
              ('month', pa.int32()),
              ('day', pa.int32())
        ])
        
    print(f'convertendo em pyarrow {psutil.virtual_memory()._asdict()}')    
    # O pandas cria esse index, este codigo serve para remover caso ele crie
    df = pa.Table.from_pandas(df, preserve_index=False, schema=schema)  
    
    print(f'escrevendo na tabela {psutil.virtual_memory()._asdict()}')
    write_deltalake(f"s3a://{BUCKET_SOURCE_REFINED}/{REFINED_FOLDER}", 
                    df, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )