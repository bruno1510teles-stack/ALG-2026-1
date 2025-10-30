# Importando Bibliotecas
import requests
import pandas as pd
import numpy as np
import base64
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from requests.auth import HTTPBasicAuth
import json
from deltalake import write_deltalake, DeltaTable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_DOWN


def trusted_to_refined (access_params=None, **kwargs):


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
    

    # Definindo a consulta
    query_jira_trusted = """
        WITH propostas AS (
            -- 1 Seleciona todas as propostas 
            SELECT *
            FROM deltalaketrusted.jira.propostas p
        ),

        serasa_ranked AS (
            -- 2 Junta as propostas com o histórico Serasa
            -- Considera apenas consultas realizadas até a data_resolvido
            -- Usa ROW_NUMBER() para pegar o registro Serasa mais recente antes da resolução
            SELECT
                p.*,
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
                    PARTITION BY p.issue_key
                    ORDER BY s.data_consulta DESC
                ) AS rn_serasa
            FROM propostas p
            LEFT JOIN deltalaketrusted.serasa.historico_compras_serasa s
                ON p.raiz_cnpj = s.cnpj_raiz
            AND CAST(s.data_consulta AS DATE) <= CAST(p.data_resolvido AS DATE)
        ),

        pontualidade_ranked AS (
            -- 3 Junta com a base de pontualidade interna
            -- Considera apenas meses anteriores ou iguais à data_resolvido
            SELECT
                sr.*,
                pi.safra_referencia AS safra_pontualidade,
                CAST(pi.pontualidade AS DECIMAL(6,2)) AS pontualidade,
                ROW_NUMBER() OVER (
                    PARTITION BY sr.issue_key
                    ORDER BY ABS(DATE_DIFF('day', CAST(sr.data_resolvido AS DATE), pi.safra_referencia)) ASC
                ) AS rn_pontualidade
            FROM serasa_ranked sr
            LEFT JOIN deltalakerefined.payments.pontualidade_interna pi
                ON sr.raiz_cnpj = pi.cnpj_raiz
            AND pi.safra_referencia <= CAST(sr.data_resolvido AS DATE)  -- só considera meses anteriores ou igual
        )

        -- 4 Seleciona apenas o registro mais recente do Serasa e o mais relevante de pontualidade
        SELECT *
        FROM pontualidade_ranked
        WHERE (rn_serasa = 1 OR rn_serasa IS NULL)
        AND (rn_pontualidade = 1 OR rn_pontualidade IS NULL)
    """

    # Verifica se a conexão foi bem-sucedida antes de executar a consulta
    if conn is not None:
        df = execute_query(conn, query_jira_trusted)
    else:
        df = None
        print("A consulta não foi executada porque a conexão com o Trino falhou.")


    # Inspecionando colunas do DataFrame
    print("Colunas carregadas e disponíveis no DataFrame:")
    print(df.columns.tolist())



    # LOGICA ANALISTA RESPONSAVEL, PRIMEIRO ANALISTA QUE APARECE NA PRIMEIRA PROPOSTA DO CLIENTE APROVADA

    # TRATANDO A COLUNA 'CNPJ'
    # Converte para string e remove todos os caracteres não numéricos
    df['cnpj'] = (df['cnpj'].astype(str).str.replace(r'\D', '', regex=True))

    # TRATANDO A COLUNA 'RAIZ_CNPJ'
    # Pega os 8 primeiros dígitos do CNPJ já limpo
    df['raiz_cnpj'] = df['cnpj'].astype(str).str[:8]

    # Convertendo a coluna de data para datetime
    df['data_resolvido'] = pd.to_datetime(df['data_resolvido'])

    # Filtrando as linhas onde a decisão é "APROVADO"
    df_aprovado = df[df['decisao'] == 'APROVADO']

    # Ordenando o DataFrame pela raiz_cnpj e data_resolvido
    df_sorted = df_aprovado.sort_values(by=['raiz_cnpj', 'data_resolvido'], ascending=[True, True])

    df_analista_responsavel = df_sorted.groupby('raiz_cnpj')['analista_tratado'].first().reset_index()

    # Renomeia a coluna para analista_responsavel
    df_analista_responsavel = df_analista_responsavel.rename(columns={'analista_tratado': 'analista_responsavel'})

    # Cruzando nova coluna
    df = df.merge(df_analista_responsavel, on='raiz_cnpj', how='left')

    # Preenchendo com 'NA' para as linhas que não têm analista responsável (onde for NaN)
    df['analista_responsavel'] = df['analista_responsavel'].fillna('NÃO ATRIBUIDA')



    # LOGICA GERENTE RESPONSAVEL, PRIMEIRO GERENTE QUE APARECE NA PRIMEIRA PROPOSTA DO CLIENTE APROVADA

    df_gerente_responsavel = df_sorted.groupby('raiz_cnpj')['gerente_tratado'].first().reset_index()

    # Renomeia a coluna para analista_responsavel
    df_gerente_responsavel = df_gerente_responsavel.rename(columns={'gerente_tratado': 'gerente_responsavel'})

    # Cruzando nova coluna
    df = df.merge(df_gerente_responsavel, on='raiz_cnpj', how='left')

    # Preenchendo com 'NA' para as linhas que não têm analista responsável (onde for NaN)
    df['gerente_responsavel'] = df['gerente_responsavel'].fillna('NÃO ATRIBUIDA')



    # GERANDO O CARGO DO ANALISTA RESPONSAVEL

    # LISTA DE NOMES POR CARGO
    lista_cargo_motor = ['MOTOR']
    lista_cargo_assistente = ['ANA BEATRIZ RODRIGUES ANDRADE']
    lista_cargo_junior = ['LARISSA FREIRE SOARES', 'VANESSA SOUZA']
    lista_cargo_pleno = ['LEANDRO QUINTINO DA ANUNCIACAO', 'DIANA TIEMI YAMAMOTO', 'CAROLINE FREIHAT HENRIQUE DE ALCANTARA SANTANA']
    lista_cargo_senior = ['CLAUDIA CINARE RODRIGUES ETO', 'ROSEMEIRE DIAS FERREIRA', 'JOSE CARVALHO', 'ALEXANDRE DE MEDEIROS']
    lista_cargo_gerente = ['ROGERIO DE CAMPOS FRIAS']
    lista_cargo_outros = ['OUTROS', 'NA']


    # FUNÇÃO ATRIBUIR CARGO
    def categorizar_cargo_analista(nome):
        nome = nome.upper()
        if nome in lista_cargo_motor:
            return '6 - MOTOR'
        elif nome in lista_cargo_assistente:
            return '1 - ASSISTENTE'
        elif nome in lista_cargo_junior:
            return '2 - JÚNIOR'
        elif nome in lista_cargo_pleno:
            return '3 - PLENO'
        elif nome in lista_cargo_senior:
            return '4 - SÊNIOR'
        elif nome in lista_cargo_gerente:
            return '5 - GERENTE'
        elif nome in lista_cargo_outros:
            return '7 - OUTROS'
        else:
            return 'ADICIONAR NO DICIONARIO DE NOMES DE ANALISTAS'


    # Aplicando a função de categorizar no DataFrame
    df['cargo_analista_responsavel'] = df['analista_responsavel'].apply(categorizar_cargo_analista)


    # Criando Funções para formatar DataFrame
    print("CRIANDO FUNÇÕES PARA FORMATAR DATAFRAME...")

    def format_faixa_valor_solicitado(vlr):
        if pd.isnull(vlr) or not isinstance(vlr, (int, float)):
            return "VALOR INVALIDO"
        
        if vlr <= 30000:
            return "01 - ATE R$30.000"
        elif 30000 < vlr <= 50000:
            return "02 - R$30.000 A R$50.000"
        elif 50000 < vlr <= 80000:
            return "03 - R$50.000 A R$80.000"
        elif 80000 < vlr <= 120000:
            return "04 - R$80.000 A R$120.000"
        elif 120000 < vlr <= 200000:
            return "05 - R$120.000 A R$200.000"
        elif 200000 < vlr <= 250000:
            return "06 - R$200.000 A R$250.000"
        elif 250000 < vlr <= 400000:
            return "07 - R$250.000 A R$400.000"
        elif 400000 < vlr <= 500000:
            return "08 - R$400.000 A R$500.000"
        elif 500000 < vlr <= 1000000:
            return "09 - R$500.000 A R$1.000.000"
        elif 1000000 < vlr <= 5000000:
            return "10 - R$1.000.000 A R$5.000.000"
        elif vlr > 5000000:
            return "11 - MAIOR QUE R$5.000.000"

    def format_faixa_aprov(percent):
        if percent <= 0.5:
            return "01 - 0-50%"
        elif 0.5 < percent <= 0.75:
            return "02 - 50-75%"
        elif 0.75 < percent < 1:
            return "03 - 75-100%"
        elif percent == 1:
            return "04 - 100%"
        elif percent > 1:
            return "05 - > 100%"
        else:
            return "VALOR NÃO INFORMADO"


    def status_decisao_relacional(status_decisao, status_aprovacao):
        if status_decisao == "REPROVADO":
            return "REPROVADO"
        elif status_aprovacao == "01 - 0-50%":
            return "APROVADO COM REDUÇÃO"
        elif status_aprovacao == "02 - 50-75%":
            return "APROVADO COM REDUÇÃO"
        elif status_aprovacao == "03 - 75-100%":
            return "APROVADO COM REDUÇÃO"
        elif status_aprovacao == "04 - 100%":
            return "APROVADO"
        elif status_decisao == "APROVADO":
            return "APROVADO"
        else:
            return "NÃO ATRIBUIDA"


    # SLA DE DECISÃO

    # Função que calcula horas úteis entre duas datas
    def calcular_tempo_util(inicio_str, fim_str):
        if pd.isna(inicio_str) or pd.isna(fim_str):
            return None

        inicio = pd.to_datetime(inicio_str)
        fim = pd.to_datetime(fim_str)

        if inicio.tzinfo is not None and fim.tzinfo is not None:
            inicio = inicio.tz_convert(None)
            fim = fim.tz_convert(None)

        hora_trabalho_inicio = time(7, 0)
        hora_trabalho_fim = time(20, 0)

        if inicio > fim:
            inicio, fim = fim, inicio

        dias_uteis = pd.date_range(inicio.date(), fim.date(), freq='B')
        total_horas = 0

        for dia in dias_uteis:
            dia_data = dia.date()
            inicio_dia = datetime.combine(dia_data, hora_trabalho_inicio)
            fim_dia = datetime.combine(dia_data, hora_trabalho_fim)

            if dia_data == inicio.date():
                inicio_dia = max(inicio, inicio_dia)
            if dia_data == fim.date():
                fim_dia = min(fim, fim_dia)

            if inicio_dia < fim_dia:
                diff = (fim_dia - inicio_dia).total_seconds() / 3600
                total_horas += diff

        return round(total_horas, 2)


    # Função para contar dias úteis entre duas datas
    def calcular_dias_uteis(inicio, fim):
        if pd.isna(inicio) or pd.isna(fim):
            return None
        inicio = pd.to_datetime(inicio)
        fim = pd.to_datetime(fim)
        if inicio > fim:
            inicio, fim = fim, inicio
        return len(pd.bdate_range(inicio.date(), fim.date())) - 1  # exclui o dia de início


    def classificar_sla_horas(diferenca_horas):
        if pd.isna(diferenca_horas):
            return "SLA NÃO DEFINIDO"
        diferenca_horas = round(diferenca_horas, 2)
        if diferenca_horas <= 2:
            return "01 - ATE 2 HORAS"
        elif diferenca_horas <= 4:
            return "02 - 2-4 HORAS"
        elif diferenca_horas <= 8:
            return "03 - 4-8 HORAS"
        elif diferenca_horas <= 24:
            return "04 - 8-24 HORAS"
        elif diferenca_horas <= 48:
            return "05 - D + 1"
        elif diferenca_horas <= 72:
            return "06 - D + 2"
        return "07 - D + 3"
        
        
    def classificar_sla_dias(dias):
        if pd.isna(dias):
            return "SLA NÃO DEFINIDO"
        elif dias <= 0:
            return "01 - D = 0"
        elif dias == 1:
            return "02 - D + 1"
        elif dias == 2:
            return "03 - D + 2"
        elif dias >= 3:
            return "04 - >= D + 3"
        

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

    if df is not None and not df.empty:
        # Aplicar a função nas colunas desejadas
        df['total_restritivos_pj'] = df['total_restritivos_pj'].apply(ajustar_decimal)
        df['total_restritivos_pf'] = df['total_restritivos_pf'].apply(ajustar_decimal)
        df['valor_total_restritivos'] = df['valor_total_restritivos'].apply(ajustar_decimal)

    # Lista de colunas que devem ser float
    cols_float = [
        'total_restritivos_pj',
        'total_restritivos_pf',
        'valor_total_restritivos'
    ]

    # Converter para float, tratar valores inválidos como NaN e arredondar para 2 casas decimais
    df[cols_float] = df[cols_float].apply(pd.to_numeric, errors='coerce').round(2)


    # Tratamento colunas para inteiro com suporte a nulos
    colunas_int = [
        'score', 'empresa_grande','qtd_cheque_pj', 'qtd_cheque_pf', 'qtd_total_cheques'
    ]

    for col in colunas_int:
        df[col] = (
            pd.to_numeric(df[col], errors='coerce')  # converte para numérico com NaNs
            .round(0)                                               # arredonda para zero casas decimais
            .astype('Int64')                                        # converte para inteiro com suporte a nulos
        )


    # Adiciona colunas de atualização
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day

    # Cálculo de horas e dias úteis
    df['diferenca_horas_criado_resolvido'] = df.apply(lambda row: calcular_tempo_util(row['data_criado'], row['data_resolvido']), axis=1)
    df['diferenca_dias_criado_resolvido'] = df.apply(lambda row: calcular_dias_uteis(row['data_criado'], row['data_resolvido']), axis=1)

    df['diferenca_horas_disp_mesa_resolvido'] = df.apply(lambda row: calcular_tempo_util(row['data_disponivel_mesa'], row['data_resolvido']), axis=1)
    df['diferenca_dias_disp_mesa_resolvido'] = df.apply(lambda row: calcular_dias_uteis(row['data_disponivel_mesa'], row['data_resolvido']), axis=1)

    # Classificações de SLA baseadas em horas e dias úteis
    df['sla_hora_criado_resolvido'] = df["diferenca_horas_criado_resolvido"].apply(classificar_sla_horas)
    df['sla_hora_disp_mesa_resolvido'] = df["diferenca_horas_disp_mesa_resolvido"].apply(classificar_sla_horas)

    df['sla_dias_criado_resolvido'] = df["diferenca_dias_criado_resolvido"].apply(classificar_sla_dias)
    df['sla_dias_disp_mesa_resolvido'] = df["diferenca_dias_disp_mesa_resolvido"].apply(classificar_sla_dias)


    # Aplicando funções para criar colunas com informações formatadas
    df['faixa_valor_solicitado'] = df['limite_pedido'].apply(format_faixa_valor_solicitado)


    # Criando df de Aprovação %
    df['aprovacao_percent'] = np.where(df['limite_pedido'] != 0, 
                            df['limite_aprovado'] / df['limite_pedido'], 
                            0)

    # Criando df Status Aprovação
    df['status_aprovacao_percent'] = df['aprovacao_percent'].apply(format_faixa_aprov)

    df['status_relacional'] = df.apply(
        lambda row: status_decisao_relacional(row['status'], row['status_aprovacao_percent']), axis=1
    )

    

    # Selecionando as colunas relevantes
    df_final = df[
        ['issue_key', 'politica', 'cnpj', 'raiz_cnpj', 'pgid', 'limite_pedido',
        'limite_aprovado', 'gerente_tratado', 'nome_vendedor_fn', 'filial_fn',
        'prioridade', 'status','categoria_decisor', 'decisao',
        'parecer', 'ramificacao_motor', 'tipo_proposta', 'data_criado',
        'data_resolvido', 'data_atualizado', 'data_disponivel_mesa',
        'diferenca_dias_criado_resolvido', 'diferenca_horas_criado_resolvido',
        'diferenca_dias_disp_mesa_resolvido',
        'diferenca_horas_disp_mesa_resolvido', 'sla_hora_criado_resolvido',
        'sla_hora_disp_mesa_resolvido', 'sla_dias_criado_resolvido',
        'sla_dias_disp_mesa_resolvido', 'faixa_valor_solicitado',
        'aprovacao_percent', 'status_aprovacao_percent', 'status_relacional','analista_tratado',
        'analista_responsavel','cargo_analista_responsavel','gerente_responsavel', 'id', 'data_consulta', 'score', 'empresa_grande',
        'total_restritivos_pf', 'total_restritivos_pj', 'valor_total_restritivos', 'qtd_cheque_pf', 'qtd_cheque_pj', 'qtd_total_cheques', 
        'safra_pontualidade', 'pontualidade','atualizado_em', 'year', 'month', 'day',
        ]
    ].reset_index(drop=True)


    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }


        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_REFINED = "jira"
        FOLDER_DESTINATION_REFINED = "propostas"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            df_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            # overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")