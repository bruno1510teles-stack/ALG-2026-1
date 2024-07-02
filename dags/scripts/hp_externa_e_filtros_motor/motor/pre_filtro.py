# Carregando libs
import pandas as pd
import pyarrow as pa
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable
from scripts.query_trino_payments import query_trino



# Criando conexão
def pre_filtro():

    print('começou função')
    
    # Variaveis Conexão
    BUCKET_SOURCE_REFINED = "payments"
    REFINED_FOLDER = "motor/pre_filtro/"

    print('declarando client')
    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key = access_params['aws_access_key_id_refined'],
        secret_key = access_params['aws_secret_access_key_refined'],
    )
    
    print('Rodando query')
    
    query = f""" with venc as (SELECT 
        DISTINCT substring(s.numero_cnpj_sacado, 1, 8) cnpj_raiz,
            True AS inad_alpe 
    FROM 
        postgres.ccred_schema_prd_default.boleto_titulo bt
        LEFT JOIN postgres.ccred_schema_prd_default.boleto_titulo_endosso bte ON bt.id = bte.boleto_titulo_id_endossado 
        LEFT JOIN postgres.ccred_schema_prd_default.boleto_titulo bt2 ON bte.boleto_titulo_id = bt2.id AND bt2.codigo_empresa = 3
        LEFT JOIN postgres.ccred_schema_prd_default.sacado s ON bt.codigo_sacado = s.codigo_sacado 
    WHERE 
        bt.titulo_pagamento
        AND bt.excluido != true
        AND bt.codigo_estagio_titulo IN (5, 6)
        AND bt.data_efetivacao IS NOT NULL
        AND bt.codigo_cedente NOT IN (12, 188, 6910, 14099, 40585, 99241, 101880, 13974, 14688, 105372)
        AND bt.status_titulo = 'VENCIDO'),
        
    lim as (SELECT 
            DISTINCT cnpj_raiz, True AS possui_limite 
        FROM 
            postgres.ccred_schema_prd_default.vw_limite_sacado_v3 
        WHERE 
            cedente_principal = true 
            AND sumarizado = true
            AND limite_atribuido >= 0
    ),

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
        est.cnpj_raiz,
        est.documento_sem_formatacao,
        est.cnae_principal,
        cnaes.descricao,
        emp.natureza_juridica,
        natjur.descricao,
        est.data_inicio_atividade,
        (date_diff('day', date(est.data_inicio_atividade), date(now())) / 365.00) AS idade,
        est.situacao_cadastral,
        coalesce(venc.inad_alpe, false) inad_alpe,
        coalesce(lim.possui_limite, false) possui_limite,
        sim.is_mei,
        coalesce(tem_pep, false) tem_pep,
        est.data_ref data_ref_receita
        
    from miniotrusted.receita_federal.estabelecimentos est 
    left join miniotrusted.receita_federal.empresas emp on emp.cnpj_raiz = est.cnpj_raiz and est.data_ref = emp.data_ref
    left join miniotrusted.receita_federal.simples sim on sim.cnpj_raiz = est.cnpj_raiz and est.data_ref = sim.data_ref
    left join miniotrusted.receita_federal.naturezas_juridicas natjur on natjur.codigo = emp.natureza_juridica and est.data_ref = natjur.data_ref
    left join miniotrusted.receita_federal.cnaes on cnaes.codigo = est.cnae_principal and est.data_ref = cnaes.data_ref
    left join lim on lim.cnpj_raiz = est.cnpj_raiz 
    left join venc on venc.cnpj_raiz = est.cnpj_raiz
    left join tem_pep on tem_pep.cnpj_raiz = est.cnpj_raiz

    where est.data_ref = (select max(data_ref) data_ref from miniotrusted.receita_federal.estabelecimentos)

"""
    

    print(f"query: {query}")
    
    df = query_trino(query, 
                                access_params['trino_endpoint'],
                                access_params['trino_port'],
                                access_params['trino_user'],
                                access_params['trino_password'])

    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')

    df['year'] = now.year
    df['month'] = now.month
    df['day'] = now.day    
    
    colunas_bool = ['is_mei', 'tem_pep']
    for coluna in colunas_bool:
        df.loc[df[coluna].isna(), coluna] = False
    
    dict_types = {'cnpj_raiz': str,
              'documento_sem_formatacao': str,
              'cod_cnae': str, 
              'cnae': str,
              'cod_natureza_juridica': str,
              'natureza_juridica': str,
              'data_inicio_atividade': str,
              'idade': float,
              'situacao_cadastral': str,
              'is_mei': bool,
              'tem_pep': bool,
              'data_ref_receita': str,
              'atualizado_em': str,
              'year': int,
              'month': int,
              'day':int}  
    
    df = df.astype(dict_types)

        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    
    # O pandas cria esse index, este codigo serve para remover caso ele crie
    df = pa.Table.from_pandas(df, preserve_index=False)
    
    write_deltalake(f"s3a://{BUCKET_SOURCE_REFINED}/{REFINED_FOLDER}", 
                    df, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )