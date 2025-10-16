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
import numpy as np
import time

def rating_mais_antigo(access_params=None,  **kwargs):

    # Conectando ao Trino para Leitura
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
    
    
    # Query Propostas
    query_propostas = f"""
    -- CTE 1: Seleciona apenas propostas aprovadas e trata CNPJ
    WITH propostas_aprovadas AS (
        SELECT 
            p.data_criado,                
            p.data_resolvido,             
            p.issue_key,                
            SUBSTR(LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0'), 1, 8) AS cnpj_raiz, 
            LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0') AS cnpj_sacado,              
            dc.razao_social,              
            p.politica,                   
            p.limite_pedido,            
            p.limite_aprovado,           
            p.gerente_tratado,           
            ROW_NUMBER() OVER (
                PARTITION BY SUBSTR(LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0'), 1, 8)  -- Agrupa por CNPJ raiz
                ORDER BY(p.data_resolvido) ASC -- Mantém a primeira aprovação
            ) AS rn
        FROM deltalaketrusted.jira.propostas p
        LEFT JOIN deltalakerefined.receita_federal.dados_cadastrais dc
            ON LPAD(REGEXP_REPLACE(p.cnpj, '[^0-9]', ''), 14, '0') 
            = LPAD(REGEXP_REPLACE(dc.cnpj_sem_formatacao, '[^0-9]', ''), 14, '0')
        WHERE p.decisao = 'APROVADO'  
    ),

    -- CTE 2: Junta faturamento estimado ao cliente da primeira proposta aprovada
    propostas_com_faturamento AS (
        SELECT 
            pa.*,                           -- Todas as colunas da CTE anterior
            f.faturamento_estimado           -- Faturamento estimado do cliente
        FROM propostas_aprovadas pa
        LEFT JOIN deltalakerefined.motor.faturamento_estimado f
        ON pa.cnpj_raiz = f.cnpj_raiz
        WHERE pa.rn = 1                     -- Apenas a primeira proposta aprovada de cada cliente
    ),

    -- CTE 3: Enriquecimento com dados do Serasa, pegando a consulta mais próxima da data da proposta
    serasa_ranked AS (
        SELECT
            pcf.*,  
            s.id,                           
            s.data_consulta,               
            s.score_positivo_pj AS score,           
            s.grande_empresa AS empresa_grande,              
            s.restritivos_pj AS total_restritivos_pj,               
            s.restritivos_pf AS total_restritivos_pf,              
            s.cheque_pf AS qtd_cheque_pf,                   
            s.cheque_pj AS qtd_cheque_pj,                    
            s.total_restritivos AS valor_total_restritivos,           
            s.total_cheques AS qtd_total_cheques,               
            ROW_NUMBER() OVER (
                PARTITION BY pcf.cnpj_raiz   -- Para cada proposta
                ORDER BY CASE 
                            WHEN s.data_consulta <= pcf.data_resolvido 
                            THEN s.data_consulta 
                        END DESC           -- Consulta mais próxima da data de resolução da proposta
            ) AS rn_serasa
        FROM propostas_com_faturamento pcf
        LEFT JOIN deltalaketrusted.serasa.historico_compras_serasa s
            ON s.cnpj_raiz = pcf.cnpj_raiz
    )

    -- Seleção final: traz apenas a consulta do Serasa mais próxima (ou nenhuma, se não houver)
    SELECT *
    FROM serasa_ranked
    WHERE rn_serasa = 1 OR rn_serasa IS NULL
    ORDER BY cnpj_raiz, data_resolvido

    """
    propostas_enriquecidas = execute_query(conn, query_propostas)
    print(f"Quantidade de propostas que retornaram da query: {propostas_enriquecidas.shape[0]}")

    # Verificando cnpj_raiz da base df_propostas_enriquecidas e atribuindo na Query de Pontualidade
    cnpjs = propostas_enriquecidas['cnpj_raiz'].unique()
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query = f"({ids_query})"


    query_pontualidade = (
            f"""
            with pontualidade_alpe as (
                select substring(regexp_replace(cnpj_sacado, '[^0-9]', ''),1,8) as cnpj_raiz, nome_sacado, 
                sum(case when status_titulo = 'NO PRAZO' then valor_face else 0 END) as valor_pago_no_prazo,
                sum(case when status_titulo = 'FORA DO PRAZO' then valor_face else 0 end) as valor_pago_fora_do_prazo,
                sum(case when status_titulo = 'VENCIDO' then valor_face else 0 end) as valor_nao_pago,
                sum(valor_face) as valor_performado,
                (sum(case when status_titulo = 'NO PRAZO' then valor_face else 0 end)) / (sum(valor_face)) * 100 as pontualidade
            from 
                deltalaketrusted.payments.boletos_internos 
            where 
                status_titulo not in ('A VENCER') 
            group by 
                substring(regexp_replace(cnpj_sacado, '[^0-9]', ''),1,8), nome_sacado
            ),
            pontualidade_arcelor as(
            select
                raiz_cnpj as cnpj_raiz,
                pontualidade
            from 
                deltalakerefined.motor.pontualidade  
            ),
            base_pontualidade as (
            select 
            coalesce(palpe.cnpj_raiz, par.cnpj_raiz) as cnpj_raiz, min(coalesce(palpe.pontualidade, par.pontualidade)) as pontualidade
            from pontualidade_alpe palpe
            full join pontualidade_arcelor par on palpe.cnpj_raiz = par.cnpj_raiz
            group by
            coalesce(palpe.cnpj_raiz, par.cnpj_raiz)
            )
            select 
                *
            from 
                base_pontualidade
            where
            cnpj_raiz in {ids_query}
    """
    )

    base_pontualidade = execute_query(conn, query_pontualidade)

    print(f"Quantidade de CNPJs que retornou da base_pontualidade: {base_pontualidade.shape[0]}")

    # Cruzando df_propostas_enriquecidas com a Base de Pontualidade
    base_analisar = pd.merge(propostas_enriquecidas, base_pontualidade, on = ['cnpj_raiz'], how = 'left')

    print(f"Quantidade de linhas df_propostas_enriquecidas: {propostas_enriquecidas.shape[0]}")
    print(f"Quantidade de linhas após cruzamento: {base_analisar.shape[0]}")

    base_analisar['flag_restritivo'] = base_analisar['valor_total_restritivos'].apply(lambda x: 1 if x > 10 else 0)
    base_analisar['empresa_grande'] = base_analisar['empresa_grande'].fillna(0).astype(int)
    base_analisar['score'] = base_analisar['score'].fillna(0).astype(int)

    
    # Regras parte da política desafiante

    def set_resultado(row, ram1='Sem Informacao', ram2='Sem Informacao', decisao='MESA', parecer=None):
        row['ramificacao'] = ram1
        row['ramificacao_2'] = ram2
        row['decisao'] = decisao
        if parecer is not None:
            row['parecer'] = parecer
        return row


    # Garantindo que sejam float
    base_analisar['faturamento_estimado'] = pd.to_numeric(base_analisar['faturamento_estimado'], errors='coerce').astype(float)
    base_analisar['valor_total_restritivos'] = pd.to_numeric(base_analisar['valor_total_restritivos'], errors='coerce').astype(float)

    # Cálculo da coluna percentual
    base_analisar['percentual_restritivo_fat_estimado'] = np.where(
        base_analisar['faturamento_estimado'] > 0,
        round((base_analisar['valor_total_restritivos'] / base_analisar['faturamento_estimado']) * 100, 2),
        np.where(
            base_analisar['valor_total_restritivos'] > 10,
            100.0,
            0.0
        )
    )


    # Regras sem HP
    def regra_aprovacao_limite_sem_hp(row, ram1, ram2):
        limite = 0
        if limite <= 100000:
            return set_resultado(row, ram1, ram2, 'APROVADO', 'Motor - Aprovado')
        else:
            return set_resultado(row, ram1, ram2, 'MESA', 'Motor - Aprovado, contudo, sem alçada. Direcionar para avaliação da mesa de crédito')
        
    def aplica_regras_sem_hp(row):
        # Checa se alguma das variáveis essenciais está ausente (NaN)
        if pd.isnull(row[['score', 'valor_total_restritivos']]).any():
            return set_resultado(row, 'Sem Informacao', 'Sem Informacao', 'MESA')

        if row['empresa_grande'] == 1:
            return set_resultado(row, 'B6', 'B3', 'MESA')

        if row['percentual_restritivo_fat_estimado'] <= 5:
            if row['score'] > 900:
                return regra_aprovacao_limite_sem_hp(row, 'A6', 'A1')
            elif row['score'] > 700:
                return regra_aprovacao_limite_sem_hp(row, 'A7', 'A2')
            elif row['score'] > 600:
                return regra_aprovacao_limite_sem_hp(row, 'A8', 'A3')
            elif row['score'] > 316:
                return set_resultado(row, 'B4', 'B1', 'MESA')
            else:
                return set_resultado(row, 'C5', 'C1', 'REPROVADO')

        if row['valor_total_restritivos'] < 100000:
            if row['score'] > 316:
                return set_resultado(row, 'B5', 'B2', 'MESA')
            else:
                return set_resultado(row, 'C6', 'C2', 'REPROVADO')
        else:
            return set_resultado(row, 'D4', 'D1', 'REPROVADO')
        

    # Regras com HP
    def regra_aprovacao_limite_com_hp(row, ram1, ram2):
        limite = 0
        if limite <= 100000:
            return set_resultado(row, ram1, ram2, 'APROVADO', 'Motor - Aprovado')
        else:
            return set_resultado(row, ram1, ram2, 'MESA', 'Motor - Aprovado, contudo, sem alçada. Direcionar para avaliação da mesa de crédito')
        
    def aplica_regras_com_hp(row):
        # Checa se alguma das variáveis essenciais está ausente (NaN)
        if pd.isnull(row[['score', 'valor_total_restritivos']]).any():
            return set_resultado(row, 'Sem Informacao', 'Sem Informacao', 'MESA')

        if row['empresa_grande'] == 1:
            return set_resultado(row, 'B3', 'B3', 'MESA')

        if row['pontualidade'] >= 99:
            if row['percentual_restritivo_fat_estimado'] <= 5:
                if row['score'] > 900:
                    return regra_aprovacao_limite_com_hp(row, 'AA', 'AA')
                elif row['score'] > 700:
                    return regra_aprovacao_limite_com_hp(row, 'A1', 'A1')
                elif row['score'] > 500:
                    return regra_aprovacao_limite_com_hp(row, 'A2', 'A2')
                elif row['score'] > 316:
                    return set_resultado(row, 'B1', 'B1', 'MESA')
                else:
                    return set_resultado(row, 'C1', 'C1', 'REPROVADO')
            elif row['valor_total_restritivos'] < 100000:
                if row['score'] > 316:
                    return set_resultado(row, 'B2', 'B2', 'MESA')
                else:
                    return set_resultado(row, 'C2', 'C2', 'REPROVADO')
            else:
                return set_resultado(row, 'D1', 'D1', 'REPROVADO')

        elif row['pontualidade'] >= 40:
            if row['percentual_restritivo_fat_estimado'] <= 5 and row['pontualidade'] > 90:
                if row['score'] > 900:
                    return regra_aprovacao_limite_com_hp(row, 'A3', 'A1')
                elif row['score'] > 700:
                    return regra_aprovacao_limite_com_hp(row, 'A4', 'A2')
                elif row['score'] > 600:
                    return regra_aprovacao_limite_com_hp(row, 'A5', 'A3')
                elif row['score'] > 316:
                    return set_resultado(row, 'B3', 'B1', 'MESA')
                else:
                    return set_resultado(row, 'C3', 'C1', 'REPROVADO')
            elif row['valor_total_restritivos'] < 100000:
                if row['score'] > 316:
                    return set_resultado(row, 'B4', 'B2', 'MESA')
                else:
                    return set_resultado(row, 'C4', 'C2', 'REPROVADO')
            else:
                return set_resultado(row, 'D2', 'D1', 'REPROVADO')
        
        else:
            return set_resultado(row, 'D3', 'D1', 'REPROVADO')


    # Criando DF
    def aplicar_regra_certa(row):
        if pd.isnull(row['pontualidade']):
            return aplica_regras_sem_hp(row)
        else:
            return aplica_regras_com_hp(row)

    df_rating_concessao = base_analisar.apply(aplicar_regra_certa, axis=1)

    # Tratamento base
    df_rating_concessao['ramificacao_final'] = df_rating_concessao['ramificacao'] + ' | ' + df_rating_concessao['ramificacao_2']

    # Função para ajustar os valores ao formato decimal(8, 2)
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None  # Mantém valores nulos como estão
        valor_str = str(valor).strip().replace(',', '.')  # Normaliza string
        if valor_str == '':
            return None  # Trata string vazia como None
        try:
            return Decimal(valor_str).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        except Exception:
            return None

    if df_rating_concessao is not None and not df_rating_concessao.empty:
        # Aplicar a função nas colunas desejadas
        df_rating_concessao['limite_pedido'] = df_rating_concessao['limite_pedido'].apply(ajustar_decimal)
        df_rating_concessao['limite_aprovado'] = df_rating_concessao['limite_aprovado'].apply(ajustar_decimal)
        df_rating_concessao['total_restritivos_pj'] = df_rating_concessao['total_restritivos_pj'].apply(ajustar_decimal)
        df_rating_concessao['total_restritivos_pf'] = df_rating_concessao['total_restritivos_pf'].apply(ajustar_decimal)
        df_rating_concessao['valor_total_restritivos'] = df_rating_concessao['valor_total_restritivos'].apply(ajustar_decimal)

    # Lista de colunas que devem ser float
    cols_float = [
        'limite_pedido',
        'limite_aprovado',
        'total_restritivos_pj',
        'total_restritivos_pf',
        'valor_total_restritivos'
    ]

    # Converter para float, tratar valores inválidos como NaN e arredondar para 2 casas decimais
    df_rating_concessao[cols_float] = df_rating_concessao[cols_float].apply(pd.to_numeric, errors='coerce').round(2)

    # Colunas de texto
    colunas_string = ['razao_social', 'parecer', 'id']

    for coluna in colunas_string:
        df_rating_concessao[coluna] = df_rating_concessao[coluna].fillna('').astype('string')


    # Colunas de data
    # Converter data_consulta para datetime64[ns, UTC], depois extrai somente a data, esse campo já é date, apenas para garantir
    df_rating_concessao['data_consulta'] = pd.to_datetime(df_rating_concessao['data_consulta'], errors='coerce', utc=True).dt.date

    # Extrair apenas a data para todas as colunas, já são datetime64[ns, UTC]
    df_rating_concessao['data_criado'] = df_rating_concessao['data_criado'].dt.date
    df_rating_concessao['data_resolvido'] = df_rating_concessao['data_resolvido'].dt.date


    # Tratamento colunas para inteiro com suporte a nulos
    colunas_int = [
        'pontualidade', 'score', 'empresa_grande',
        'flag_restritivo', 'qtd_cheque_pj', 'qtd_cheque_pf', 'qtd_total_cheques'
    ]

    for col in colunas_int:
        df_rating_concessao[col] = (
            pd.to_numeric(df_rating_concessao[col], errors='coerce')  # converte para numérico com NaNs
            .round(0)                                               # arredonda para zero casas decimais
            .astype('Int64')                                        # converte para inteiro com suporte a nulos
        )

    # Remove colunas
    df_rating_concessao = df_rating_concessao.drop(
        columns=['decisao', 'politica', 'limite_pedido', 'limite_aprovado', 'gerente_tratado', 'parecer'],errors='ignore')

    # Reordena as colunas
    ordem_colunas = [
        'data_criado', 'data_resolvido', 'issue_key', 'cnpj_raiz', 'cnpj_sacado','razao_social','pontualidade', 
        'ramificacao', 'ramificacao_2', 'ramificacao_final', 'id', 'data_consulta', 'score', 
        'empresa_grande', 'total_restritivos_pj', 'total_restritivos_pf', 'valor_total_restritivos',
        'flag_restritivo', 'faturamento_estimado', 'qtd_cheque_pj', 'qtd_cheque_pf', 'qtd_total_cheques'
    ]

    # Aplica a ordem e reseta o índice
    df_rating_concessao = df_rating_concessao[ordem_colunas].reset_index(drop=True)

    # Colunas de data
    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_rating_concessao['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_rating_concessao['year'], df_rating_concessao['month'], df_rating_concessao['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")


    # Configuração do Delta Lake
    storage_options = {
    "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
    "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
    "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    BUCKET_SOURCE_TRUSTED = "serasa-trusted"
    FOLDER_DESTINATION_TRUSTED = "rating_mais_antigo_serasa"

    # Escrevendo no Delta Lake com schema fixado
    write_deltalake(
    f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
    df_rating_concessao,
    partition_by=["year", "month", "day"],
    storage_options=storage_options,
    mode="overwrite"
    )






