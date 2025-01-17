# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import os
from airflow.models import Variable
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from decimal import Decimal, ROUND_DOWN

def boletos_raw_to_trusted(access_params=None,  **kwargs):

    ### Coletando dados da camada Raw
    # Conectando com o banco
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
    

    # Base Boletos CCRED
    query_boleto = f"""
        select 
		distinct bt.numero_sequencial_titulo,
		bt.codigo_filial,
		bt.codigo_empresa,
		bt.numero_titulo,
		coalesce(bt.codigo_cedente_endossante, bt.codigo_cedente) as codigo_cedente,
		coalesce(bt.cedente_endossante_id, bt.cedente_id) as cedente_id,
		bt.codigo_sacado,
		bt.sacado_id,
		bt.status_titulo, 
		bt.status_liquidez,
		bt.codigo_situacao_titulo,
		to_char(date(bt.data_emissao), 'yyyy-mm-dd') as data_emissao,
		to_char(coalesce(date(bt2.data_efetivacao), date(bt.data_efetivacao)), 'yyyy-mm-dd') as data_efetivacao,
		to_char(date(bt.data_vencimento), 'yyyy-mm-dd') as data_vencimento,
		to_char(date(bt.data_baixa), 'yyyy-mm-dd') as data_baixa,
        to_char(date(bt.data_original_vencimento), 'yyyy-mm-dd') as data_original_vencimento,
		bt.valor_face,
		bt.valor_titulo,
		bt.valor_baixado,
		bt.valor_desagio,
	        bt.rotulo,
	        bt.codigo_estagio_titulo,
                SUBSTRING(bt.numero_nfe, 27,9) as numero_nota_fiscal,
                bt.numero_nfe
	from 
			postgres.ccred_schema_{Variable.get('STAGE')}_default.boleto_titulo bt
			left join postgres.ccred_schema_{Variable.get('STAGE')}_default.boleto_titulo_endosso bte on bt.id = bte.boleto_titulo_id_endossado 
			left join postgres.ccred_schema_{Variable.get('STAGE')}_default.boleto_titulo bt2 on bte.boleto_titulo_id  = bt2.id and bt2.codigo_empresa = 3
	where 
		bt.titulo_pagamento
		and bt.excluido != true
		and bt.codigo_estagio_titulo in (5, 6)
		and bt.data_efetivacao is not null
		and bt.codigo_cedente not in (12, 188, 6910, 14099, 40585, 99241, 101880, 13974, 14688, 105372, 109619, 109151, 107738, 108454, 108455, 10798, 109485, 123326, 109485)
		and coalesce(date(bt2.data_efetivacao), date(bt.data_efetivacao)) <= cast('2024-04-30' as date)
union 
select 
		distinct bt.numero_sequencial_titulo,
		bt.codigo_filial,
		bt.codigo_empresa,
		bt.numero_titulo,
		coalesce(bt.codigo_cedente_endossante, bt.codigo_cedente) as codigo_cedente,
		coalesce(bt.cedente_endossante_id, bt.cedente_id) as cedente_id,
		bt.codigo_sacado,
		bt.sacado_id,
		bt.status_titulo, 
		bt.status_liquidez,
		bt.codigo_situacao_titulo,
		to_char(date(bt.data_emissao), 'yyyy-mm-dd') as data_emissao,
		to_char(case 
			when date(bt.data_emissao) < cast('2024-05-01' as date)
			then date('2024-05-01')
			else date(bt.data_emissao) end, 'yyyy-mm-dd') as data_efetivacao, 
		to_char(date(bt.data_vencimento), 'yyyy-mm-dd') as data_vencimento,
		to_char(date(bt.data_baixa), 'yyyy-mm-dd') as data_baixa,
        to_char(date(bt.data_original_vencimento), 'yyyy-mm-dd') as data_original_vencimento,
		bt.valor_face,
		bt.valor_titulo,
		bt.valor_baixado,
		bt.valor_desagio,
	        bt.rotulo,
	        bt.codigo_estagio_titulo,
               SUBSTRING(bt.numero_nfe, 27,9) as numero_nota_fiscal,
               bt.numero_nfe
	from 
			postgres.ccred_schema_{Variable.get('STAGE')}_default.boleto_titulo bt
			left join postgres.ccred_schema_{Variable.get('STAGE')}_default.boleto_titulo_endosso bte on bt.id = bte.boleto_titulo_id_endossado 
			left join postgres.ccred_schema_{Variable.get('STAGE')}_default.boleto_titulo bt2 on bte.boleto_titulo_id  = bt2.id and bt2.codigo_empresa = 3
	where 
		bt.titulo_pagamento
		and bt.excluido != true
		and bt.codigo_estagio_titulo in (5, 6)
		and bt.data_efetivacao is not null
		and bt.codigo_cedente not in (12, 188, 6910, 14099, 40585, 99241, 101880, 13974, 14688, 105372, 109619, 109151, 107738, 108454, 108455, 10798, 109485, 123326, 109485)
		and coalesce(date(bt2.data_efetivacao), date(bt.data_efetivacao)) > cast('2024-04-30' as date)
    """
    boleto = execute_query(conn, query_boleto)

    print(f"Quantidade de linhas no DataFrame 'boleto': {boleto.shape[0]}")

    # Base Sacado
    query_sacado = f"""
        select 
        id as sacado_id, nome_fantasia_sacado, nome_sacado, numero_cnpj_sacado_formatado as cnpj_sacado, uf_endereco as uf_sacado, cidade_endereco as cidade_sacado
        from postgres.ccred_schema_{Variable.get('STAGE')}_default.sacado
    """
    sacado = execute_query(conn, query_sacado)
    print(f"Quantidade de linhas no DataFrame 'sacado': {sacado.shape[0]}")


    # Base Cedente
    query_cedente = f"""
        select 
        id as cedente_id, cpf_cnpj as cnpj_cedente, nome as nome_cedente, nome_fantasia as nome_fantasia_cedente
        from postgres.ccred_schema_{Variable.get('STAGE')}_default.pessoa_cedente
    """
    cedente = execute_query(conn, query_cedente)
    print(f"Quantidade de linhas no DataFrame 'cedente': {cedente.shape[0]}")


    print("Iniciando cruzamento de DFs")
    # Cruzando Bases
    df = pd.merge(boleto, sacado, left_on='sacado_id', right_on='sacado_id', how='inner')
    df = pd.merge(df, cedente, left_on='cedente_id', right_on='cedente_id', how='inner')
    df.reset_index(drop=True, inplace=True)
    print("Cruzamento de DFs concluído")
    
    print("Iniciando tratamento dos dados")
    # Tratando Dados
    def converter_para_datetime(df, colunas, formato='%Y-%m-%d'):
        for coluna in colunas:
            df[coluna] = pd.to_datetime(df[coluna], format=formato)
            df[coluna] = df[coluna].dt.date
        return df
    
    colunas_para_converter_datetime = ['data_emissao', 'data_efetivacao', 'data_vencimento', 'data_baixa', 'data_original_vencimento']
    df = converter_para_datetime(df, colunas_para_converter_datetime)

    # Criando safras
    df['safra_concessao'] = df['data_efetivacao'].apply(lambda x: x.replace(day=1))
    df['safra_vencimento'] = df['data_vencimento'].apply(lambda x: x.replace(day=1))
    df['safra_baixa'] = df['data_baixa'].apply(lambda x: x.replace(day=1))

    colunas_para_converter_datetime = ['safra_concessao', 'safra_vencimento', 'safra_baixa']
    df = converter_para_datetime(df, colunas_para_converter_datetime)

    # Tratando casos de baixa parcial
    df.loc[df['status_titulo'] == 'VENCIDO', 'data_baixa'] = pd.NaT

    # Função para ajustar os valores ao formato decimal(8, 2)
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None  # Mantém valores nulos como estão
        else:
            # Limitar para no máximo 8 dígitos, com 2 casas decimais
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

    # Aplicar a função na coluna 'valor_titulo'
    df['valor_titulo'] = df['valor_titulo'].apply(ajustar_decimal)

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    print(f"Quantidade de linhas no DataFrame final: {df.shape[0]}")

    # Exportando dados para a camada Trusted
    # # Conectando na Trusted        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "payments"
    FOLDER_DESTINATION_TRUSTED = "boletos_internos"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )