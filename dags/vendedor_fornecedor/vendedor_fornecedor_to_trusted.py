# Carregando libs
import pandas as pd
import numpy as np
import re
from datetime import datetime, timezone, timedelta
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from unidecode import unidecode
from airflow.models import Variable
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity



def vendedor_fornecedor_to_trusted(access_params=None,  **kwargs):

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
	
	### Lógica de Processamento - Início
	print("Iniciando a busca e processamento dos dados...")

	# A variável 'valores_vazios' foi movida para o início do bloco de processamento para ser definida antes de ser usada.
	valores_vazios = ['', 'nan', 'none', '#n/d', None, np.nan]
	
	#Base API da Arcelor
	query_arcelor = f"""
			WITH base_seller_info AS (
				SELECT 
					si.*,
					CASE
						WHEN regexp_like(lower(si.branch_name), 'balc[aã]o.*bras[ií]lia') THEN 'DBA Brasília'
						ELSE si.branch_name
					END AS branch_name_ajustado
				FROM postgres.knkt_fndt_{Variable.get('STAGE')}_default.seller_info si
			),
			
			base_instructions AS (
				SELECT
					i.*,
					bsi.branch_name_ajustado,
					bsi.seller_code
				FROM postgres.knkt_fndt_{Variable.get('STAGE')}_default.instruction i
				INNER JOIN base_seller_info bsi ON i.seller_id = bsi.id
			)
			
			SELECT DISTINCT 
				i.id AS instruction_id,
				i.seller_id,
				i.amount,
				i.status,
				i.created_date AS instruction_created_date,
				i.last_modified_date AS instruction_last_modified_date,
			
				-- Substitui branch_name pela filial_consolidada quando existir
				CASE
					WHEN REGEXP_LIKE(lower(COALESCE(dpr.filial_consolidada, i.branch_name_ajustado)), 'balc[aã]o.*bras[ií]lia') THEN 'DBA Brasília'
					ELSE COALESCE(dpr.filial_consolidada, i.branch_name_ajustado)
				END AS escritorio_vendas,
			
				i.seller_code AS cod_vendedor_fornecedor,
			
				-- Nome do vendedor Arcelor
				COALESCE(NULLIF(TRIM(v."salesperson__r.name"), ''), v.nome) AS vendedor_fornecedor,
			
				-- Nome do vendedor_alpe
				dpr.regional_alpe AS vendedor_alpe,
			
				nfe.id AS nfe_id,
				nfe.access_key AS numero_nfe,
				nfe.created_date AS nfe_created_date,
				nfe.status AS nfe_status
			
			FROM base_instructions i
			LEFT JOIN postgres.knkt_fndt_{Variable.get('STAGE')}_default.invoice_internal_payment_instruction_association iipia 
				ON iipia.internal_payment_instruction_id = i.id
			LEFT JOIN postgres.knkt_fndt_{Variable.get('STAGE')}_default.invoice_nota_fiscal_eletronica infe 
				ON infe.invoice_id = iipia.invoice_id
			LEFT JOIN postgres.knkt_fndt_{Variable.get('STAGE')}_default.nota_fiscal_eletronica nfe 
				ON nfe.id = infe.nota_fiscal_eletronica_id
			LEFT JOIN minioraw.planejamento_comercial.vendedor_am v 
				ON i.seller_code = v.salespersoncode
			LEFT JOIN minioraw.planejamento_comercial.de_para_unidade_regional dpr 
				ON i.branch_name_ajustado = dpr.filial
			"""
	df_api_arcelor = execute_query(conn, query_arcelor)

	# --- Tratamentos df_api_arcelor ---

	# Filtra o DataFrame para remover linhas onde 'numero_nfe' está vazio ou nulo e padroniza a coluna
	df_api_arcelor = df_api_arcelor.loc[df_api_arcelor['numero_nfe'].notna()]
	df_api_arcelor.loc[:, 'numero_nfe'] = df_api_arcelor['numero_nfe'].astype(str).str.strip()

	# Padroniza a coluna 'escritorio_vendas'
	df_api_arcelor.loc[:, 'escritorio_vendas'] = (df_api_arcelor['escritorio_vendas'].str.replace('regional', 'Usina', regex=False, case=False))

	# Lista de colunas a tratar
	colunas_para_tratar = ['cod_vendedor_fornecedor', 'vendedor_fornecedor', 'escritorio_vendas', 'vendedor_alpe']

	for col in colunas_para_tratar:
		df_api_arcelor.loc[:, col] = df_api_arcelor[col].astype(str).str.strip()
		df_api_arcelor.loc[:, col] = df_api_arcelor[col].apply(lambda x: unidecode(x) if pd.notna(x) else x)
		df_api_arcelor.loc[:, col] = df_api_arcelor[col].str.replace(r'[^\w\s/-]', '', regex=True)
		mascara = df_api_arcelor[col].str.lower().isin([str(v).lower() for v in valores_vazios])
		df_api_arcelor.loc[mascara, col] = 'Nao Atribuido'

	# Padroniza a capitalização do nome do vendedor
	mascara_nao_atribuido = df_api_arcelor['vendedor_fornecedor'].str.lower() == 'nao atribuido'
	df_api_arcelor.loc[~mascara_nao_atribuido, 'vendedor_fornecedor'] = \
		df_api_arcelor.loc[~mascara_nao_atribuido, 'vendedor_fornecedor'].str.title()

	# Padroniza o nome do vendedor com base no código, priorizando o nome mais longo
	nomes_padronizados = (
		df_api_arcelor[df_api_arcelor['cod_vendedor_fornecedor'].str.lower() != 'nao atribuido']
		.groupby('cod_vendedor_fornecedor')['vendedor_fornecedor']
		.apply(lambda x: max(x, key=len) if x.any() else None)
		.to_dict()
	)
	mascara_vendedores = df_api_arcelor['cod_vendedor_fornecedor'].isin(nomes_padronizados.keys())
	df_api_arcelor.loc[mascara_vendedores, 'vendedor_fornecedor'] = df_api_arcelor.loc[mascara_vendedores, 'cod_vendedor_fornecedor'].map(nomes_padronizados)

	# Adiciona a coluna de origem
	df_api_arcelor.loc[:, 'origem'] = 'df_api_arcelor'

	# Seleciona colunas finais
	df_api_arcelor = df_api_arcelor[['numero_nfe', 'cod_vendedor_fornecedor', 'vendedor_fornecedor', 'escritorio_vendas', 'vendedor_alpe', 'origem']]

	print(f"DataFrame Arcelor processado com sucesso. Linhas: {df_api_arcelor.shape[0]}")

	# Base consolidado_venda
	query_consolidado_venda = f"""
			WITH base_venda AS (
				SELECT
					nf_completa AS numero_nfe,
					cod_vendedor AS cod_vendedor_fornecedor,
					vendedor AS vendedor_fornecedor,
				CASE
					WHEN REGEXP_LIKE(regional, 'Teixeira.*Freitas') THEN 'DBA Teixeira Freitas'
					WHEN REGEXP_LIKE(regional, 'Ribeir.*Preto') THEN 'DBA Ribeirao Preto'
					WHEN REGEXP_LIKE(regional, 'Campos do Goytacaz.*') THEN 'DBA Campos do Goytacazes'
					-- Se o nome da regional comecar com 'Regional ', troca esse prefixo por 'Usina '
					WHEN substr(regional, 1, 9) = 'Regional ' THEN 'Usina ' || substr(regional, 10)
					ELSE regional
				END AS escritorio_vendas_ajustado
				FROM minioraw.planejamento_comercial.consolidado_venda
			),
			de_para_filial AS (
				SELECT
					filial_sem_acentuacao,
					filial_consolidada,
					regional_alpe AS vendedor_alpe
				FROM minioraw.planejamento_comercial.de_para_unidade_regional
			)
			SELECT DISTINCT
				bv.numero_nfe,
				-- Prioriza o código do vendedor da tabela 'vendedor_am' (v)
				COALESCE(v.salespersoncode, bv.cod_vendedor_fornecedor) AS cod_vendedor_fornecedor,
				-- Prioriza o nome do vendedor da tabela 'vendedor_am' (v)
				COALESCE(NULLIF(TRIM(v."salesperson__r.name"), ''), v.nome, bv.vendedor_fornecedor) AS vendedor_fornecedor,
				COALESCE(dpf.filial_consolidada, bv.escritorio_vendas_ajustado) AS escritorio_vendas,
				dpf.vendedor_alpe
			FROM base_venda bv
			LEFT JOIN de_para_filial dpf
				ON bv.escritorio_vendas_ajustado = dpf.filial_sem_acentuacao
			LEFT JOIN minioraw.planejamento_comercial.vendedor_am v
				ON bv.cod_vendedor_fornecedor = v.salespersoncode
	"""
	df_consolidado_venda = execute_query(conn, query_consolidado_venda)

	# --- Tratamentos df_consolidado_venda ---

	# Remove linhas com valores indesejados na coluna 'numero_nfe' e padroniza
	valores_indesejados_nfe = ['#N/D', '', 'nan', None, np.nan]
	df_consolidado_venda = df_consolidado_venda[~df_consolidado_venda['numero_nfe'].astype(str).str.strip().str.lower().isin(valores_indesejados_nfe)]
	df_consolidado_venda.loc[:, 'numero_nfe'] = df_consolidado_venda['numero_nfe'].astype(str).str.strip()

	# Aplica ajustes manuais específicos de NFe
	ajustes_manuaes_nfe = {
		'33250517469701016170550000003097211306183511': ('Fabio Fonseca da Silva', 'CDB Rio de Janeiro'),
		'31250517469701003869550000004141241397867543': ('Raffaela Papa', 'CDB Belo Horizonte')
	}

	for nfe, (vendedor, escritorio) in ajustes_manuaes_nfe.items():
		df_consolidado_venda.loc[df_consolidado_venda['numero_nfe'] == nfe, ['vendedor_fornecedor', 'escritorio_vendas']] = [vendedor, escritorio]

	# Padroniza 'escritorio_vendas'
	df_consolidado_venda.loc[:, 'escritorio_vendas'] = (df_consolidado_venda['escritorio_vendas'].str.replace('regional', 'Usina', regex=False, case=False))

	# Tratamento para valores N/D
	mascara_nd = (
		df_consolidado_venda['vendedor_fornecedor'].str.strip().str.upper().isin(['N/D', '#N/D']) |
		df_consolidado_venda['escritorio_vendas'].str.strip().str.upper().isin(['N/D', '#N/D']) |
		df_consolidado_venda['cod_vendedor_fornecedor'].str.strip().str.upper().isin(['N/D', '#N/D'])
	)
	df_consolidado_venda.loc[mascara_nd, ['vendedor_fornecedor', 'escritorio_vendas', 'cod_vendedor_fornecedor']] = 'Nao Atribuido'

	# Lista de colunas a tratar
	colunas_para_tratar = ['cod_vendedor_fornecedor', 'vendedor_fornecedor', 'escritorio_vendas', 'vendedor_alpe']

	for col in colunas_para_tratar:
		df_consolidado_venda.loc[:, col] = df_consolidado_venda[col].astype(str).str.strip()
		df_consolidado_venda.loc[:, col] = df_consolidado_venda[col].apply(lambda x: unidecode(x) if pd.notna(x) else x)
		df_consolidado_venda.loc[:, col] = df_consolidado_venda[col].str.replace(r'[^\w\s/-]', '', regex=True)
		mascara = df_consolidado_venda[col].str.lower().isin([str(v).lower() for v in valores_vazios])
		df_consolidado_venda.loc[mascara, col] = 'Nao Atribuido'

	# Padroniza capitalização do nome do vendedor
	mascara_nao_atribuido = df_consolidado_venda['vendedor_fornecedor'].str.lower() == 'nao atribuido'
	df_consolidado_venda.loc[~mascara_nao_atribuido, 'vendedor_fornecedor'] = \
		df_consolidado_venda.loc[~mascara_nao_atribuido, 'vendedor_fornecedor'].str.title()

	# Padroniza o nome do vendedor com base no código
	nomes_padronizados = (
		df_consolidado_venda[df_consolidado_venda['cod_vendedor_fornecedor'].str.lower() != 'nao atribuido']
		.groupby('cod_vendedor_fornecedor')['vendedor_fornecedor']
		.apply(lambda x: max(x, key=len) if x.any() else None)
		.to_dict()
	)
	mascara_vendedores = df_consolidado_venda['cod_vendedor_fornecedor'].isin(nomes_padronizados.keys())
	df_consolidado_venda.loc[mascara_vendedores, 'vendedor_fornecedor'] = df_consolidado_venda.loc[mascara_vendedores, 'cod_vendedor_fornecedor'].map(nomes_padronizados)

	# Adiciona a coluna de origem
	df_consolidado_venda.loc[:, 'origem'] = 'df_consolidado_venda'

	# Seleciona colunas finais
	df_consolidado_venda = df_consolidado_venda[
		['numero_nfe', 'cod_vendedor_fornecedor', 'vendedor_fornecedor', 'escritorio_vendas', 'vendedor_alpe', 'origem']
	]

	print(f"DataFrame Venda Consolidada processado com sucesso. Linhas: {df_consolidado_venda.shape[0]}")


	# Base consolidado_venda_historico
	query_venda_historico = fr"""
			WITH base_padronizada AS (
				SELECT
					nota_fical_completa,
					vendedor_am,
					filial_consolidada,
					vendedor_alpe
				FROM minioraw.planejamento_comercial.consolidado_venda_historico
			)
			
			SELECT DISTINCT
				bp.nota_fical_completa AS numero_nfe,
				-- Traz o código do vendedor da tabela 'vendedor_am'
				COALESCE(v.salespersoncode, 'Nao Atribuido') AS cod_vendedor_fornecedor,

				-- Prioriza o nome limpo da tabela 'vendedor_am', caso contrário usa o da base
				COALESCE(
					NULLIF(TRIM(v."salesperson__r.name"), ''),
					REGEXP_REPLACE(v.nome, ' \([A-Z0-9]+\)', ''),
					bp.vendedor_am
				) AS vendedor_fornecedor,
				
				bp.filial_consolidada AS escritorio_vendas,
				COALESCE(dpr.regional_alpe, bp.vendedor_alpe) AS vendedor_alpe
			FROM base_padronizada AS bp
			LEFT JOIN minioraw.planejamento_comercial.de_para_unidade_regional AS dpr
				ON bp.filial_consolidada = dpr.filial_consolidada
			LEFT JOIN minioraw.planejamento_comercial.vendedor_am AS v
				-- A condição de JOIN agora é baseada em um nome limpo
				ON bp.vendedor_am = REGEXP_REPLACE(v.nome, ' \([A-Z0-9]+\)', '')
	"""
	df_consolidado_venda_historico = execute_query(conn, query_venda_historico)

	# --- Tratamentos df_consolidado_venda_historico ---

	# Padroniza a coluna 'numero_nfe'
	df_consolidado_venda_historico.loc[:, 'numero_nfe'] = df_consolidado_venda_historico['numero_nfe'].astype(str).str.strip()

	# Padroniza a coluna 'escritorio_vendas'
	df_consolidado_venda_historico.loc[:, 'escritorio_vendas'] = (df_consolidado_venda_historico['escritorio_vendas'].str.replace('regional', 'Usina', regex=False, case=False))

	# Lista de colunas a tratar
	colunas_para_tratar = ['cod_vendedor_fornecedor', 'vendedor_fornecedor', 'escritorio_vendas', 'vendedor_alpe']

	# Preenche valores nulos e padroniza
	df_consolidado_venda_historico['cod_vendedor_fornecedor'] = df_consolidado_venda_historico['cod_vendedor_fornecedor'].fillna('Nao Atribuido')

	for col in colunas_para_tratar:
		# Converte para string e remove espaços
		df_consolidado_venda_historico.loc[:, col] = df_consolidado_venda_historico[col].astype(str).str.strip()
		# Remove acentos e caracteres especiais
		df_consolidado_venda_historico.loc[:, col] = df_consolidado_venda_historico[col].apply(lambda x: unidecode(x) if pd.notna(x) else x)
		df_consolidado_venda_historico.loc[:, col] = df_consolidado_venda_historico[col].str.replace(r'[^\w\s/-]', '', regex=True)
		# Substitui valores vazios por 'Nao Atribuido'
		mascara = df_consolidado_venda_historico[col].str.lower().isin([str(v).lower() for v in valores_vazios])
		df_consolidado_venda_historico.loc[mascara, col] = 'Nao Atribuido'

	# Padroniza capitalização do nome do vendedor (exclui 'Nao Atribuido')
	mascara_nao_atribuido = df_consolidado_venda_historico['vendedor_fornecedor'].str.lower() == 'nao atribuido'
	df_consolidado_venda_historico.loc[~mascara_nao_atribuido, 'vendedor_fornecedor'] = \
		df_consolidado_venda_historico.loc[~mascara_nao_atribuido, 'vendedor_fornecedor'].str.title()

	# Adiciona a coluna de origem
	df_consolidado_venda_historico.loc[:, 'origem'] = 'df_consolidado_venda_historico'

	# Seleciona colunas finais
	df_consolidado_venda_historico = df_consolidado_venda_historico[
		['numero_nfe', 'cod_vendedor_fornecedor', 'vendedor_fornecedor', 'escritorio_vendas', 'vendedor_alpe', 'origem']
	]

	print(f"DataFrame Histórico de Vendas processado com sucesso. Linhas: {df_consolidado_venda_historico.shape[0]}")


	# =============================
	# Normalização vetorizada
	# =============================
	def normaliza_texto_vetorizado(serie):
		serie = serie.fillna('').astype(str).str.strip().str.lower()
		return pd.Series([unidecode(x) for x in serie], index=serie.index)

	for df in [df_consolidado_venda_historico, df_consolidado_venda, df_api_arcelor]:
		df['vendedor_norm'] = normaliza_texto_vetorizado(df['vendedor_fornecedor'])
		df['escritorio_norm'] = normaliza_texto_vetorizado(df['escritorio_vendas'])

	# =============================
	# Mapeamento para casos de exceção
	# =============================
	EXCECAO_MAP_NOME = {
		'alex junior de arrude': 'Alex Junior De Arrude',
		'alex krumholz': 'Alex Krumholz',
		'alexandre ribeiro chaves': 'Alexandre Ribeiro Chaves',
		'amanda matsuda': 'Amanda Matsuda',
		'ana claudia mariano': 'Ana Claudia Mariano',
		'ana lucia farias de souza': 'Ana Lucia Farias De Souza',
		'anna beatriz felix': 'Anna Beatriz Felix',
		'aridan mota brito': 'Ariadson Mota Brito',
		'arliane silva balbino melo': 'Arliane Silva Balbino Melo',
		'aruan rangel': 'Aruam Rangel Galaxe',
		'aruam rangel galaxe': 'Aruam Rangel Galaxe',
		'barbara catusso vicenzi': 'Barbara Catusso Vicenzi',
		'belquior emanuel morao prado': 'Belquior Emanuel Morao Prado',
		'bruno cassolato': 'Bruno Cassolat',
		'bruno spangnolo': 'Bruno Spangnolo',
		'carlos daniel reinert': 'Carlos Daniel Reinert',
		'carlos manoel rodrigues do santos': 'Carlos Manoel Rodrigues Do Santos',
		'cbh - bruno cassolat': 'CBH - Bruno Cassolat',
		'cbh 53': 'Sergio Ferreira',
		'cleber santiago pereira': 'Cleber Santiago Pereira',
		'daniel junior cordeiro oliveira': 'Daniel Junior Cordeiro Oliveira',
		'danielle oliveira da silva': 'Danielle Oliveira Da Silva',
		'dba  - 10': 'Mery Vieira',
		'dba  - 6': 'Amanda Matsuda',
		'dba t freitas 1': 'TF0',
		'dba t freitas 5': 'Marcia Nascimento Dos Santos',
		'dba t freitas 6': 'Ariadson Mota Brito',
		'dba t freitas 8': 'Cleber Santiago Pereira',
		'dba t freitas 15': 'Romair Tiago Brito Soares',
		'dba volta redonda': 'FB0',
		'dbg -11': 'Mirella Miranda',
		'dbl - 6': 'Carlos Daniel Reinert',
		'dbr -11 tulio': 'Tulio Eugenio',
		'dcb 7': 'Belquior Emanuel Morao Prado',
		'dcb 8': 'Oscar Joao',
		'dcg campo grande 13': 'Diego De Moura Ferreira',
		'dcg campo grande 5': 'Danielle Oliveira Da Silva',
		'dch - 10': 'Reginaldo Lamp',
		'dch - 11': 'Bruno Spangnolo',
		'dch - 3': 'Marcio Luiz Braghini',
		'dch - 5': 'Nilson Rigoni',
		'dch - 8': 'Neimar Da Silva',
		'dch - 9': 'Alex Junior De Arrude',
		'dcm 4': 'Geyce Guedes',
		'dcm 6': 'Jessica Dayane Silva',
		'dcs caxias 10': 'Mariza Chiapetti',
		'dcs caxias 16': 'Barbara Catusso Vicenzi',
		'dcs caxias 18': 'Luciano Angelo Lipreri',
		'dcs caxias 2': 'Romel Paulo Miglioranza',
		'dcs caxias 7': 'Odinei Ribeiro De Almeida',
		'dct - 1': 'Mayara Faria',
		'ddf dist federal 7': 'Anna Beatriz Felix',
		'ddi divinopolis 17': 'Kely Ronara Costa',
		'ddi divinopolis 3': 'Ana Claudia Mariano',
		'ddi divinopolis 5': 'Arliane Silva Balbino Melo',
		'ddi divinopolis 9': 'Marcos Rodrigo De Carvalho',
		'dep - 1': 'Ana Lucia Farias De Souza',
		'dep - 13': 'Daniel Junior Cordeiro Oliveira',
		'dep - 17': 'Matheus Souza Silva',
		'dep - 20': 'Sebastiao Filho Pereira Da Cunha',
		'dep - 5': 'Alexandre Ribeiro Chaves',
		'dep - 6': 'Evandro Alves De Magalhaes',
		'dep - 7': 'Silvalino Felix Camara',
		'dfs - 15': 'Sabrina Andrade',
		'djf - 10': 'Leonardo Araujo Da Silva',
		'djf - 3': 'Isabella Da Silva Pereira',
		'djf - 9': 'Vinicius Motta Hallack',
		'djf-9': 'Vinicius Motta Hallack',
		'djo - 14': 'Sandra Mara Bacca',
		'djo - 2': 'Guilherme H Belloto',
		'djo - 4': 'Gelson Jose De Souza',
		'dnt - 12': 'Karla Moreno',
		'dnt - 19': 'Una Araujo',
		'dou - 1': 'Tiago Marcelo Lima Da Costa',
		'dou - 2': 'Nelinton Dias',
		'dou - 4': 'Douglas Rafael Tamiosso',
		'dpa pouso alegre 03': 'Eloah Teolis Bernardes',
		'dpa pouso alegre 04': 'Jeniffer Santos',
		'dpa pouso alegre 09': 'Tamara Pereira De Mendonça',
		'dpa pouso alegre 10': 'Larissa Rodrigues Da Silva',
		'dpe penapolis 10': 'Wanderlei Claus',
		'dpl 1': 'Giovane Da Cruz Tunas',
		'dpl 11': 'Fernando Kines Alves',
		'dpl 2': 'Douglas Rosa',
		'dri - 13': 'Dri - 13',
		'dro - 2': 'Edelmo Ferreira De Oliveira',
		'dro - 7': 'Carlos Manoel Rodrigues Do Santos',
		'dsa - 2': 'Valteir Solda Gonzales',
		'dsi  3': 'Indiana Karine Scheffler',
		'dsi  4': 'Diele De Melo Schneider',
		'dsm  5': 'Pablo Toneto Da Costa',
		'dsm  8': 'Micheline Prates Bassi Cado',
		'dub - 3': 'Paulo Sergio Nogueira',
		'dub - 4': 'Ronaldo - Vendas Arcelormittal',
		'dvr 16': 'BF6',
		'dvr 4': 'Orlando Goncalves Brandao',
		'dvr 5': 'Vanildo Rodrigues Gomes',
		'ianne amanda vasconcelos gomes avila': 'Ianne Amanda Vasconcelos Gomes Avila',
		'kaio silva': 'Kaio Vinicius Da Silva',
		'kaio vinicius da silva': 'Kaio Vinicius Da Silva',
		'kely ronara costa': 'Kely Ronara Costa',
		'luciano angelo lipreri': 'Luciano Angelo Lipreri',
		'marcio luiz braghini': 'Marcio Luiz Braghini',
		'mariza chiapetti': 'Mariza Chiapetti',
		'mery vieira': 'Mery Vieira',
		'micheline prates bassi cado': 'Micheline Prates Bassi Cado',
		'mirella miranda': 'Mirella Miranda',
		'neimar da silva': 'Neimar Da Silva',
		'nilson rigoni': 'Nilson Rigoni',
		'odinei ribeiro de almeida': 'Odinei Ribeiro De Almeida',
		'oscar joao': 'Oscar Joao',
		'paulo sergio nogueira': 'Paulo Sergio Nogueira',
		'romel paulo miglioranzo': 'Romel Paulo Miglioranza',
		'sabrina andrade': 'Sabrina Andrade',
		'sebastiao filho pereira da cunha': 'Sebastiao Filho Pereira Da Cunha',
		'tiago marcelo lima da costa': 'Tiago Marcelo Lima Da Costa',
		'tulio eugenio': 'Tulio Eugenio',
		'una araujo': 'Una Araujo',
		'valteir solda gonzales': 'Valteir Solda Gonzales',
		'wanderlei claus': 'Wanderlei Claus',
		'ysa reprcomltda me': 'Maria Aparecida Silva Marques'
	}


	EXCECAO_MAP_COD = {
		'alex junior de arrude': 'AU9',
		'alex krumholz': 'C29',
		'alexandre ribeiro chaves': 'AX5',
		'amanda matsuda': 'BC5',
		'ana claudia mariano': 'O03',
		'ana lucia farias de souza': 'AX1',
		'anna beatriz felix': 'I07',
		'ariadson mota brito': 'TF5',
		'arliane silva balbino melo': 'O05',
		'aruan rangel': 'XE1',
		'aruam rangel galaxe': 'XE1',
		'barbara catusso vicenzi': 'M16',
		'belquior emanuel morao prado': 'CC7',
		'bruno cassolato': 'F72',
		'bruno spangnolo': 'AV2',
		'carlos daniel reinert': 'BI6',
		'carlos manoel rodrigues do santos': 'FE7',
		'cbh - bruno cassolat': 'F72',
		'cbh 53': 'CBH 53',
		'cleber santiago pereira': 'TF7',
		'daniel junior cordeiro oliveira': 'AZ4',
		'danielle oliveira da silva': 'N05',
		'dba  - 10': 'BC9',
		'dba  - 6': 'BC5',
		'dba t freitas 1': 'TF0',
		'dba t freitas 5': 'TF4',
		'dba t freitas 6': 'TF5',
		'dba t freitas 8': 'TF7',
		'dba t freitas 15': 'TG4',
		'dba volta redonda': 'FB0',
		'dbg -11': 'GS1',
		'dbl - 6': 'BI6',
		'dbr -11 tulio': 'FJ0',
		'dcb 7': 'CC7',
		'dcb 8': 'CC8',
		'dcg campo grande 13': 'N13',
		'dcg campo grande 5': 'N05',
		'dch - 10': 'AV1',
		'dch - 11': 'AV2',
		'dch - 3': 'AU3',
		'dch - 5': 'AU5',
		'dch - 8': 'AU8',
		'dch - 9': 'AU9',
		'dcm 4': 'BG4',
		'dcm 6': 'BG6',
		'dcs caxias 10': 'M10',
		'dcs caxias 16': 'M16',
		'dcs caxias 18': 'M18',
		'dcs caxias 2': 'M02',
		'dcs caxias 7': 'M07',
		'dct - 1': 'AE0',
		'ddf dist federal 7': 'I07',
		'ddi divinopolis 17': 'O17',
		'ddi divinopolis 3': 'O03',
		'ddi divinopolis 5': 'O05',
		'ddi divinopolis 9': 'O09',
		'dep - 1': 'AX1',
		'dep - 13': 'AZ4',
		'dep - 17': 'AZ8',
		'dep - 20': 'AY2',
		'dep - 5': 'AX5',
		'dep - 6': 'AX6',
		'dep - 7': 'AX7',
		'dfs - 15': 'FT5',
		'djf - 10': 'AN9',
		'djf - 3': 'AN2',
		'djf - 9': 'AN8',
		'djf-9': 'AN8',
		'djo - 14': 'T14',
		'djo - 2': 'T02',
		'djo - 4': 'T04',
		'dnt - 12': 'NU2',
		'dnt - 19': 'NU9',
		'dou - 1': 'PM0',
		'dou - 2': 'PM1',
		'dou - 4': 'PM3',
		'dpa pouso alegre 03': 'D03',
		'dpa pouso alegre 04': 'D04',
		'dpa pouso alegre 09': 'D09',
		'dpa pouso alegre 10': 'D10',
		'dpe penapolis 10': 'A10',
		'dpl 1': 'AH0',
		'dpl 11': 'BT5',
		'dpl 2': 'AH1',
		'dri - 13': 'Nao Atribuido',
		'dro - 2': 'FE2',
		'dro - 7': 'FE7',
		'dsa - 2': 'AA1',
		'dsi  3': 'BO2',
		'dsi  4': 'BO3',
		'dsm  5': 'BT4',
		'dsm  8': 'BT7',
		'dub - 3': 'U03',
		'dub - 4': 'Nao Atribuido',
		'dvr 16': 'BF6',
		'dvr 4': 'BE4',
		'dvr 5': 'BE5',
		'ianne amanda vasconcelos gomes avila': 'L08',
		'kaio silva': 'BA2',
		'kaio vinicius da silva': 'BA2',
		'luciano angelo lipreri': 'M18',
		'marcio luiz braghini': 'AU3',
		'mariza chiapetti': 'M10',
		'mery vieira': 'BC9',
		'mirella miranda': 'GS1',
		'neimar da silva': 'AU8',
		'nilson rigoni': 'AU5',
		'odinei ribeiro de almeida': 'M07',
		'oscar joao': 'CC8',
		'paulo sergio nogueira': 'U03',
		'romel paulo miglioranzo': 'M02',
		'sabrina andrade': 'FT5',
		'sebastiao filho pereira da cunha': 'AY2',
		'tiago marcelo lima da costa': 'PM0',
		'tulio eugenio': 'FJ0',
		'una araujo': 'NU9',
		'valteir solda gonzales': 'AA1',
		'wanderlei claus': 'A10',
		'ysa reprcomltda me': 'Nao Atribuido'
	}


	# =============================
	# Aplicar exceções preservando API
	# =============================
	api_vendedores_norm = set(df_api_arcelor['vendedor_norm'])

	# Criar auditoria vazia
	alteracoes_excecao = []

	for df_name, df in zip(['historico','venda'], [df_consolidado_venda_historico, df_consolidado_venda]):
		for idx, (vnorm, vreal, cod) in enumerate(zip(df['vendedor_norm'], df['vendedor_fornecedor'], df['cod_vendedor_fornecedor'])):
			if vnorm not in api_vendedores_norm:
				novo_nome = EXCECAO_MAP_NOME.get(vnorm, vreal)
				novo_cod = EXCECAO_MAP_COD.get(vnorm, cod)
				if (novo_nome != vreal) or (novo_cod != cod):
					alteracoes_excecao.append({
						'df': df_name,
						'indice': idx,
						'etapa': 'excecao',
						'cod_ant': cod,
						'cod_novo': novo_cod,
						'nome_ant': vreal,
						'nome_novo': novo_nome
					})
					df.at[idx, 'vendedor_fornecedor'] = novo_nome
					df.at[idx, 'cod_vendedor_fornecedor'] = novo_cod
		df['vendedor_norm'] = normaliza_texto_vetorizado(df['vendedor_fornecedor'])

	alteracoes_excecao = pd.DataFrame(alteracoes_excecao)

	# =============================
	# Merge exato com a API
	# =============================
	api_dict_nome = df_api_arcelor.set_index(['escritorio_norm','vendedor_norm'])['vendedor_fornecedor'].to_dict()
	api_dict_cod = df_api_arcelor.set_index(['escritorio_norm','vendedor_norm'])['cod_vendedor_fornecedor'].to_dict()

	for df in [df_consolidado_venda_historico, df_consolidado_venda]:
		key_tuples = list(zip(df['escritorio_norm'], df['vendedor_norm']))
		df['vendedor_fornecedor'] = [api_dict_nome.get(k, v) for k,v in zip(key_tuples, df['vendedor_fornecedor'])]
		df['cod_vendedor_fornecedor'] = [api_dict_cod.get(k, v) for k,v in zip(key_tuples, df['cod_vendedor_fornecedor'])]

	# =============================
	# Função para escolher nome mais completo
	# =============================
	def escolher_nome_mais_completo(grp):
		return grp.loc[grp['vendedor_fornecedor'].str.len().idxmax()]

	for i, df in enumerate([df_consolidado_venda_historico, df_consolidado_venda]):
		df_temp = (
			df.groupby('cod_vendedor_fornecedor', group_keys=False, as_index=False, sort=False)
			.apply(lambda g: escolher_nome_mais_completo(g))
		)
		df_temp.reset_index(drop=True, inplace=True)
		if i == 0:
			df_consolidado_venda_historico = df_temp
		else:
			df_consolidado_venda = df_temp

	# =============================
	# TF-IDF contra API
	# =============================
	SIMILARITY_THRESHOLD = 0.6
	vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2,5))
	vectorizer.fit(df_api_arcelor['vendedor_norm'])

	alteracoes_tfidf = []

	def aplicar_tfidf_api(df, df_name, limiar=SIMILARITY_THRESHOLD):
		api_map_nome = df_api_arcelor.set_index('vendedor_norm')['vendedor_fornecedor'].to_dict()
		api_map_cod = df_api_arcelor.set_index('vendedor_norm')['cod_vendedor_fornecedor'].to_dict()
		
		mask_sem_codigo = df['cod_vendedor_fornecedor'].isin(['Nao Atribuido', np.nan])
		vendedores_na = df.loc[mask_sem_codigo, 'vendedor_norm'].unique()
		
		for escritorio in df.loc[mask_sem_codigo, 'escritorio_norm'].unique():
			idx_escritorio = df[(mask_sem_codigo) & (df['escritorio_norm']==escritorio)].index
			candidatos = df_api_arcelor[df_api_arcelor['escritorio_norm'] == escritorio]
			if candidatos.empty:
				continue
			tfidf_sem = vectorizer.transform(df.loc[idx_escritorio, 'vendedor_norm'])
			tfidf_cand = vectorizer.transform(candidatos['vendedor_norm'])
			sim = cosine_similarity(tfidf_sem, tfidf_cand)
			idx_max = np.argmax(sim, axis=1)
			max_sim = sim.max(axis=1)
			vendedores_encontrados_norm = candidatos['vendedor_norm'].iloc[idx_max]
			
			for i, (sim_score, vendedor_norm_candidato) in enumerate(zip(max_sim, vendedores_encontrados_norm)):
				if sim_score >= limiar:
					real_idx = idx_escritorio[i]
					cod_ant = df.at[real_idx, 'cod_vendedor_fornecedor']
					nome_ant = df.at[real_idx, 'vendedor_fornecedor']
					df.at[real_idx,'vendedor_fornecedor'] = api_map_nome[vendedor_norm_candidato]
					df.at[real_idx,'cod_vendedor_fornecedor'] = api_map_cod[vendedor_norm_candidato]
					alteracoes_tfidf.append({
						'df': df_name,
						'indice': real_idx,
						'etapa': 'tfidf',
						'cod_ant': cod_ant,
						'cod_novo': api_map_cod[vendedor_norm_candidato],
						'nome_ant': nome_ant,
						'nome_novo': api_map_nome[vendedor_norm_candidato]
					})
		return df

	df_consolidado_venda_historico = aplicar_tfidf_api(df_consolidado_venda_historico, 'historico')
	df_consolidado_venda = aplicar_tfidf_api(df_consolidado_venda, 'venda')
	alteracoes_tfidf = pd.DataFrame(alteracoes_tfidf)

	# =============================
	# Fallback interno entre históricos
	# =============================
	base_ref = pd.concat([df_consolidado_venda, df_consolidado_venda_historico], ignore_index=True)
	base_ref = base_ref[base_ref['cod_vendedor_fornecedor'] != 'Nao Atribuido']

	vectorizer_interno = TfidfVectorizer(analyzer='char', ngram_range=(2,5))
	vectorizer_interno.fit(base_ref['vendedor_norm'])
	referencia_matrix = vectorizer_interno.transform(base_ref['vendedor_norm'])

	mapa_interno_cod = base_ref.set_index('vendedor_norm')['cod_vendedor_fornecedor'].to_dict()
	mapa_interno_nome = base_ref.set_index('vendedor_norm')['vendedor_fornecedor'].to_dict()

	alteracoes_fallback = []

	def aplicar_fallback_interno(df, df_name, threshold=0.4):
		vendedores_na = df.loc[df['cod_vendedor_fornecedor'].isin(['Nao Atribuido', np.nan]), 'vendedor_norm'].unique()
		if len(vendedores_na) == 0:
			return df
		venda_matrix = vectorizer_interno.transform(vendedores_na)
		sim_matrix = cosine_similarity(venda_matrix, referencia_matrix)
		idx_best = sim_matrix.argmax(axis=1)
		scores = sim_matrix.max(axis=1)
		for i, vendedor_na in enumerate(vendedores_na):
			if scores[i] >= threshold:
				vendedor_ref = base_ref['vendedor_norm'].iloc[idx_best[i]]
				idx_real = df[(df['vendedor_norm']==vendedor_na) & df['cod_vendedor_fornecedor'].isin(['Nao Atribuido', np.nan])].index
				for idx in idx_real:
					cod_ant = df.at[idx, 'cod_vendedor_fornecedor']
					nome_ant = df.at[idx, 'vendedor_fornecedor']
					df.at[idx,'vendedor_fornecedor'] = mapa_interno_nome[vendedor_ref]
					df.at[idx,'cod_vendedor_fornecedor'] = mapa_interno_cod[vendedor_ref]
					alteracoes_fallback.append({
						'df': df_name,
						'indice': idx,
						'etapa': 'fallback',
						'cod_ant': cod_ant,
						'cod_novo': mapa_interno_cod[vendedor_ref],
						'nome_ant': nome_ant,
						'nome_novo': mapa_interno_nome[vendedor_ref]
					})
		return df

	df_consolidado_venda_historico = aplicar_fallback_interno(df_consolidado_venda_historico, 'historico')
	df_consolidado_venda = aplicar_fallback_interno(df_consolidado_venda, 'venda')
	alteracoes_fallback = pd.DataFrame(alteracoes_fallback)

	# =============================
	# Limpeza final
	# =============================
	for df in [df_consolidado_venda_historico, df_consolidado_venda]:
		df.drop(columns=['vendedor_norm','escritorio_norm'], inplace=True)

	# =============================
	# Relatório final de auditoria
	# =============================
	relatorio_alteracoes = pd.concat([alteracoes_excecao, alteracoes_tfidf, alteracoes_fallback], ignore_index=True)
	relatorio_alteracoes = relatorio_alteracoes.sort_values(['df','indice','etapa']).reset_index(drop=True)

	print("Relatório de alterações criado!")

	# --- Concatenação ---
	df_concatenado = pd.concat([df_api_arcelor, df_consolidado_venda, df_consolidado_venda_historico], ignore_index=True)

	print("\nProcessamento completo! DataFrames prontos para uso.")
	print(f"Número total de linhas após a união: {df_concatenado.shape[0]}")

	#Tratamentos df_concatenado

	# Mapeia prioridade: menor número = maior prioridade
	prioridade = {'df_api_arcelor': 0, 'df_consolidado_venda': 1, 'df_consolidado_venda_historico': 2}
	df_concatenado['prioridade'] = df_concatenado['origem'].map(prioridade)

	# Ordena pela prioridade
	df_concatenado = df_concatenado.sort_values(by='prioridade')

	# Remove duplicatas, mantendo o de maior prioridade (primeiro após ordenação)
	qtd_antes = df_concatenado.shape[0]
	df_concatenado = df_concatenado.drop_duplicates(subset=['numero_nfe'], keep='first')
	qtd_depois = df_concatenado.shape[0]

	# Remove a coluna de prioridade
	df_concatenado = df_concatenado.drop(columns='prioridade')

	print(f"Removidas {qtd_antes - qtd_depois} duplicatas com base em 'numero_nfe'.")
	print(f"Total de linhas finais: {qtd_depois}")

	# Base de Faturamento
	query_faturamento = f"""
			SELECT 
				data,
				cnpj_cedente,
				nome_cedente,
				LPAD(REGEXP_REPLACE(cnpj_sacado, '[^0-9]', ''), 14, '0') AS cnpj_sacado,
				substr(cnpj_sacado, 1,8) as raiz_cnpj,
				nome_sacado,
				numero_nfe,
				valor_fatura,
				valor_fatura_pos_sefaz,
				valor_fatura_oficial,
				valor_face_qprof

			FROM deltalakerefined.payments.faturamento
	"""

	df_faturamento = execute_query(conn, query_faturamento)

	# Tratamentos df_faturamento
	# Garante que nfe_access_key é string e remove espaços em branco antes e depois
	df_faturamento['numero_nfe'] = df_faturamento['numero_nfe'].astype(str).str.strip()

	# Identificar origem para o concat
	df_faturamento['origem'] = 'df_faturamento'

	df_faturamento_arcelor = pd.merge(df_concatenado, df_faturamento, on='numero_nfe', how='outer', suffixes=('_concatenado', '_faturamento'))

	# Calcula as diferenças de linhas
	linhas_faturamento = df_faturamento.shape[0]
	linhas_concatenado = df_concatenado.shape[0]
	linhas_merge = df_faturamento_arcelor.shape[0]
	diferenca = linhas_merge - linhas_concatenado

	# Mensagem mais clara do resultado do merge
	print("\n=== Resultado do cruzamento com a base de faturamento ===")
	print(f"- Linhas na base consolidada: {linhas_concatenado}")
	print(f"- Linhas na base de faturamento: {linhas_faturamento}")
	print(f"- Linhas após o outer join: {linhas_merge}")

	if diferenca > 0:
		print(f"> {diferenca} novas linhas foram incluídas (presentes apenas na base de faturamento).")
	elif diferenca < 0:
		print(f"> {-diferenca} linhas da base consolidada não tiveram correspondência no faturamento.")
	else:
		print("> Não houve alteração no número total de linhas após o cruzamento.")

	# Localização
	# ============================
	cnpjs = df_faturamento_arcelor['cnpj_sacado'].dropna().unique()
	ids_cnpjs = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)

	# Base de localização
	query_localizacao = f"""
			SELECT  
				cnpj_raiz as raiz_cnpj,
				LPAD(REGEXP_REPLACE(cnpj_sem_formatacao, '[^0-9]', ''), 14, '0') AS cnpj_sacado,
				cep,
				uf,
				municipio
			FROM deltalakerefined.receita_federal.dados_cadastrais AS dc
			WHERE cnpj_sem_formatacao in ({ids_cnpjs})
	"""
	df_localizacao = execute_query(conn, query_localizacao)

	df_final = pd.merge(df_faturamento_arcelor, df_localizacao, on='cnpj_sacado', how='left', suffixes=('_faturamento_arcelor', '_localizacao'))

	# Calcula as contagens
	linhas_iniciais = df_faturamento_arcelor.shape[0]
	linhas_finais = df_final.shape[0]

	# Mensagem clara e informativa
	print("\n=== Resultado do cruzamento com a base de localização ===")
	print(f"- Linhas antes do cruzamento: {linhas_iniciais}")
	print(f"- Linhas após o cruzamento: {linhas_finais}")

	if linhas_finais > linhas_iniciais:
		print(f"> {linhas_finais - linhas_iniciais} linhas adicionais foram criadas (possível efeito de multiplicação por correspondências múltiplas no CNPJ sacado).")
	elif linhas_finais < linhas_iniciais:
		print(f"> {linhas_iniciais - linhas_finais} linhas foram perdidas (isso não é esperado em um LEFT JOIN — vale verificar).")
	else:
		print("> O número de linhas permaneceu o mesmo após o cruzamento.")


	# --- Tratamentos df_final ---

	# Tratamento origem
	df_final['origem'] = df_final['origem_concatenado'].combine_first(df_final['origem_faturamento'])
	df_final.drop(columns=['origem_concatenado', 'origem_faturamento'], inplace=True)


	# --- Tratamento da coluna de raiz CNPJ ---
	df_final['cnpj_raiz'] = df_final['raiz_cnpj_faturamento_arcelor'].combine_first(
		df_final['raiz_cnpj_localizacao']
	)
	df_final.drop(columns=['raiz_cnpj_faturamento_arcelor', 'raiz_cnpj_localizacao'], inplace=True)


	# Remove notas fiscais inválidas
	df_final = df_final[df_final['numero_nfe'].str.upper() != 'N/D']

	# Ajustes manuais
	# Dicionários de mapeamento para os ajustes manuais de notas fiscais
	ajustes_vendedor_fornecedor = {
		'42250717469701012264550000001382921054567429': 'Alex Junior De Arrude',
		'35250717469701022731550000000281391508452930': 'Fabricio Elton Gasparin Da Rocha',
		'33250717469701016766550000000857061706644575': 'Cristina Maria Ribeiro Da Silva',
		'32250117469701010482550000076326871414495251': 'Renato Rendeiro',
		'53250317469701002200550000001542451836824289': 'Ianne Amanda Vasconcelos Gomes Avila'
	}

	ajustes_codigo_fornecedor = {
		'32250117469701010482550000076326871414495251': '58',
		'53250317469701002200550000001542451836824289': 'L08',
		'33250817469701010806550010002029031620416606': 'BU7'

	}

	ajustes_escritorio_vendas = {
		'42250717469701012264550000001382921054567429': 'DBA Chapeco',
		'35250717469701022731550000000281391508452930': 'CDB Curitiba',
		'33250717469701016766550000000857061706644575': 'DBA Campos dos Goytacazes',
		'32250117469701010482550000076326871414495251': 'Usina SP',
		'33250817469701010806550010002029031620416606': 'Usina RJ/ES/MG'
		
	}

	ajustes_vendedor_alpe = {
		'42250717469701012264550000001382921054567429': 'Talita',
		'35250717469701022731550000000281391508452930': 'Talita',
		'33250717469701016766550000000857061706644575': 'Cassio',
		'32250117469701010482550000076326871414495251': 'Glauciele',
		'33250817469701010806550010002029031620416606': 'Cassio'
	}


	# 1. Aplica os ajustes manuais de forma eficiente
	# Usa o .map() para aplicar os ajustes de vendedor_fornecedor e escritorio_vendas
	df_final.loc[df_final['numero_nfe'].isin(ajustes_vendedor_fornecedor.keys()), 'vendedor_fornecedor'] = df_final['numero_nfe'].map(ajustes_vendedor_fornecedor)
	df_final.loc[df_final['numero_nfe'].isin(ajustes_vendedor_alpe.keys()), 'cod_vendedor_fornecedor'] = df_final['numero_nfe'].map(ajustes_codigo_fornecedor)
	df_final.loc[df_final['numero_nfe'].isin(ajustes_escritorio_vendas.keys()), 'escritorio_vendas'] = df_final['numero_nfe'].map(ajustes_escritorio_vendas)
	df_final.loc[df_final['numero_nfe'].isin(ajustes_vendedor_alpe.keys()), 'vendedor_alpe'] = df_final['numero_nfe'].map(ajustes_vendedor_alpe)


	# Dicionário de ajustes por código de vendedor
	ajustes_codigo_vendedor_fornecedor = {
		'ZQE': 'Lilian Cristina Reis',
		'KUI': 'Mauricio Amorim',
		'RA8': 'Airton Mendonca Dos Santos'
	}

	# Aplica os ajustes de forma vetorizada e performática
	df_final['vendedor_fornecedor'] = df_final['cod_vendedor_fornecedor'].map(ajustes_codigo_vendedor_fornecedor).fillna(df_final['vendedor_fornecedor'])

	condicao = (
		((df_final['cod_vendedor_fornecedor'] == 'NU9') & (df_final['escritorio_vendas'] == 'DBA Manaus')) |
		((df_final['vendedor_fornecedor'] == 'Una Araujo') & (df_final['escritorio_vendas'] == 'DBA Manaus'))
	)

	df_final.loc[condicao, 'cod_vendedor_fornecedor'] = 'A3K'
	df_final.loc[condicao, 'vendedor_fornecedor'] = 'Vanessa De Araujo Andrade'
	# escritorio_vendas já continua como DBA Manaus



	# Limpeza de strings
	colunas_para_limpar_string = [
		'cnpj_sacado','cnpj_raiz','nome_sacado','cnpj_cedente','nome_cedente',
		'cod_vendedor_fornecedor','vendedor_fornecedor','escritorio_vendas','vendedor_alpe',
		'cep','municipio','uf','numero_nfe','origem'
	]
	df_final[colunas_para_limpar_string] = df_final[colunas_para_limpar_string].astype(str).apply(lambda x: x.str.strip())

	# Remove linhas indesejadas
	df_final = df_final[~(
		df_final['numero_nfe'].notna() &
		df_final['valor_fatura'].isna() &
		df_final['origem'].isin(['df_consolidado_venda','df_consolidado_venda_historico'])
	)]

	# Pré-processamento e normalização
	valores_vazios = ['', 'nan', 'none', '#n/d', None, np.nan, 'Nao Atribuido', "Outros FN's"]
	colunas_para_tratar = ['cod_vendedor_fornecedor','vendedor_fornecedor','escritorio_vendas','vendedor_alpe']
	variacoes_outros_fns = ['Outros Fns',"Outros FN's",'Outros FNs']

	for coluna in ['vendedor_alpe','vendedor_fornecedor','escritorio_vendas']:
		df_final[coluna] = df_final[coluna].replace(variacoes_outros_fns,'Nao Atribuido')

	for coluna in colunas_para_tratar:
		mascara_coluna = df_final[coluna].astype(str).str.strip().str.lower().isin([str(x).lower() for x in valores_vazios])
		df_final.loc[mascara_coluna, coluna] = 'Nao Atribuido'

	# Remove pontos finais
	for coluna in ['nome_cedente','nome_sacado']:
		df_final[coluna] = df_final[coluna].str.rstrip('.')

	# Normaliza para regras
	df_final['nome_cedente_norm'] = df_final['nome_cedente'].apply(lambda x: unidecode(str(x)).upper().strip())
	df_final['nome_sacado_norm'] = df_final['nome_sacado'].apply(lambda x: unidecode(str(x)).upper().strip())
	df_final['uf_sacado_norm'] = df_final['uf'].str.upper()


	print(f"O DataFrame final foi concluído com sucesso, contendo {df_final.shape[0]} linhas prontas para demais alteraçoes.")


	# --- Atribuição por Regras Específicas e Mapeamentos ---

	# Regra 1: Priscilla
	sacados_priscilla = [
		'FERMAZON FERRO E ACO DO AMAZONAS LTDA',
		'BALDAN IMPLEMENTOS AGRICOLAS S A',
		'DIACO DISTRIBUIDORA DE ACO S/A',
		'GREIF EMBALAGENS INDUSTRIAIS DE MINAS GERAIS LTDA',
		'PERFINASA METAIS LTDA',
		'METALFORTE INDUSTRIA METALURGICA LTDA'
	]

	# Máscara para as outras empresas, excluindo 'PERFINASA METAIS LTDA'
	sacados_outros = [s for s in sacados_priscilla if s != 'PERFINASA METAIS LTDA']
	mascara_outros = (
		(df_final['escritorio_vendas'] == 'Nao Atribuido') &
		(df_final['vendedor_alpe'] == 'Nao Atribuido') &
		(df_final['nome_sacado'].isin(sacados_outros)) &
		(df_final['nome_cedente'] == 'ARCELORMITTAL BRASIL S.A')
	)

	# Máscara específica para 'PERFINASA METAIS LTDA'
	mascara_perfinasa = (
		(df_final['escritorio_vendas'] == 'Nao Atribuido') &
		(df_final['vendedor_alpe'] == 'Nao Atribuido') &
		(df_final['nome_sacado'] == 'PERFINASA METAIS LTDA') &
		(df_final['nome_cedente'] == 'ARCELORMITTAL BRASIL S.A')
	)

	# Aplica as regras separadamente
	df_final.loc[mascara_outros, ['vendedor_alpe', 'escritorio_vendas']] = ['Priscilla', 'Distribuicao']
	df_final.loc[mascara_perfinasa, ['vendedor_alpe', 'escritorio_vendas']] = ['Priscilla', 'Distribuicao']

	# Regra 2: TUPER
	mascara_tuper = (df_final['nome_cedente'] == 'TUPER S/A') & (df_final['escritorio_vendas'] == 'Nao Atribuido')
	df_final.loc[mascara_tuper, ['vendedor_alpe', 'escritorio_vendas']] = ['Priscilla', 'TUPER S/A']

	# Regras 3,4,5: cedentes Rafael
	cedentes_rafael = {
		'CASAL COMERCIO E SERVICOS LTDA': 'CASAL COMERCIO E SERVICOS LTDA',
		'CASA DO ADUBO S.A': 'CASA DO ADUBO S.A',
		'ASUS - INDUSTRIA DE MAQUINAS AGRICOLAS LTDA': 'ASUS - INDUSTRIA DE MAQUINAS AGRICOLAS LTDA',
	}
	for cedente, escritorio in cedentes_rafael.items():
		mascara_cedente = df_final['nome_cedente'] == cedente
		df_final.loc[mascara_cedente, 'vendedor_alpe'] = 'Rafael'
		df_final.loc[mascara_cedente, 'escritorio_vendas'] = escritorio

	# Regra 6: BELGO
	mapa_sacados_belgo = {
		'TECNO ARAMES COMERCIO, IMPORTACAO E EXPORTACAO LTDA': 'Glauciele',
		'INNARA INDUSTRIA NACIONAL DE ARAMADOS LTDA': 'Glauciele',
		'NEW ARMS INDUSTRIA E COMERCIO DE HASTES LTDA': 'Glauciele',
		'TELAFER COMERCIO DE TELAS E FERRAGENS LTDA': 'Glauciele',
		'PUMA LAJES ALVEOLARES LTDA': 'Glauciele',
		'RAC TELAS LTDA.': 'Rafael',
		'PRECON PRE FABRICADOS LTDA': 'Rafael',
		'PREMOBRAS PREMOLDADOS BRASILEIROS LTDA': 'Cassio'
	}
	mapa_uf_belgo = {
		'RS': 'Leonardo', 'SC': 'Talita', 'PR': 'Talita', 'SP': 'Glauciele',
		'RJ': 'Cassio', 'MG': 'Rafael', 'ES': 'Cassio', 'MS': 'Pedro',
		'MT': 'Pedro', 'DF': 'Pedro', 'GO': 'Pedro', 'TO': 'Tiago',
		'PA': 'Tiago', 'AP': 'Tiago', 'AM': 'Tiago', 'RR': 'Tiago',
		'RO': 'Tiago', 'AC': 'Tiago', 'BA': 'Wilma', 'SE': 'Wilma',
		'AL': 'Wilma', 'PE': 'Wilma', 'PB': 'Wilma', 'RN': 'Wilma',
		'CE': 'Wilma', 'PI': 'Wilma', 'MA': 'Wilma'
	}
	mascara_belgo = df_final['nome_cedente_norm'] == 'BELGO BEKAERT ARAMES LTDA'
	df_final.loc[mascara_belgo, 'vendedor_alpe'] = df_final.loc[mascara_belgo, 'nome_sacado_norm'].map(mapa_sacados_belgo)
	df_final.loc[mascara_belgo, 'vendedor_alpe'] = df_final.loc[mascara_belgo, 'vendedor_alpe'].fillna(df_final.loc[mascara_belgo, 'uf_sacado_norm'].map(mapa_uf_belgo))

	# --- ETAPA 3: Atribuições de 'escritorio_vendas' ---

	# Cedentes próprios
	cedentes_proprios = ['BELGO BEKAERT ARAMES LTDA','DISCOR DISTRIBUIDORA DE TINTAS LTDA','APERAM INOX AMERICA DO SUL S.A']
	mascara_cedentes_proprios = df_final['nome_cedente'].isin(cedentes_proprios)
	df_final.loc[mascara_cedentes_proprios,'escritorio_vendas'] = df_final['nome_cedente']

	# CARELLI
	mascara_carelli = df_final['nome_sacado'].str.upper() == 'CARELLI & CIA LTDA'
	df_final.loc[mascara_carelli, 'vendedor_alpe'] = 'Talita'


	# CITRINO
	mascara_citrino = df_final['nome_sacado'].str.upper() == 'CITRINO - CONSTRUCOES INDUSTRIAIS TRINO LTDA'
	df_final.loc[mascara_citrino, ['vendedor_fornecedor', 'cod_vendedor_fornecedor']] = ['Jose Carlos Albani', 'BE6']


	#Escritorio_vendas como DBA Campos do Goytacazes e vendedor Cassio

	# Crie uma máscara para identificar as linhas onde o nome do escritório precisa ser corrigido
	mascara_goytacazes = df_final['escritorio_vendas'].str.contains(r'(?i)DBA Campos do Goytacazes', na=False, regex=True)

	# Aplica a correção de nome do escritório
	df_final.loc[mascara_goytacazes, 'escritorio_vendas'] = 'DBA Campos dos Goytacazes'

	# Atribui o vendedor_alpe 'Cassio' para as mesmas linhas
	df_final.loc[mascara_goytacazes, 'vendedor_alpe'] = 'Cassio'


	#Regras Usina RJ/ES/MG e CDB Sao Paulo

	# Crie a condição para a regra do Cassio
	condicao_cassio = (
		df_final['escritorio_vendas'].isin(['Usina RJ/ES/MG', 'DBA Sao Paulo'])
		& (
			(df_final['vendedor_fornecedor'] == 'Airton Mendonca Dos Santos') |
			(df_final['vendedor_fornecedor'] == 'Fernando Henrique Do Carmo Fernandes') |
			(df_final['vendedor_fornecedor'] == 'Elaine Salles Chow') |
			(df_final['vendedor_fornecedor'] == 'Marluzia Celesti Lucio') |
			(df_final['vendedor_fornecedor'] == 'Anirene Aguiar') |
			(df_final['vendedor_fornecedor'] == 'Rafael Sandinha Rep') |
			(df_final['vendedor_fornecedor'] == 'Jane Iris Weberling') |
			(df_final['vendedor_fornecedor'] == 'Edivam Cardoso Pinheiro')
		)
	)

	# Crie a condição para a regra do Rafael
	condicao_rafael = (
		(df_final['vendedor_fornecedor'] == 'Lilian Silva') |
		(df_final['vendedor_fornecedor'] == 'Vanessa Nathalia Resende Magalhaes') |
		(df_final['vendedor_fornecedor'] == 'Alexandra Silva Fernandez T Arauj') |
		(df_final['vendedor_fornecedor'] == 'Rosemeire De Carvalho Barbosa Fidelis') |
		(df_final['vendedor_fornecedor'] == 'Raffaela de Oliveira') |
		(df_final['vendedor_fornecedor'] == 'Raffaela Papa') |
		(df_final['vendedor_fornecedor'] == 'Cruz de Aco Repr') |
		(df_final['vendedor_fornecedor'] == 'Marcos Cruz') |
		(df_final['vendedor_fornecedor'] == 'Ana Cristina Castilho Assis') |
		(df_final['vendedor_fornecedor'] == 'Larissa Fontes Nunes') |
		(df_final['vendedor_fornecedor'] == 'Leticia Patriny Silva Cruz') |
		(df_final['vendedor_fornecedor'] == 'Leticia Cruz') |
		(df_final['vendedor_fornecedor'] == 'Bruno Cassolat') |
		(df_final['vendedor_fornecedor'] == 'Alexandre Carvalho Barroso')
	)

	# Crie a condição para a regra do Pedro
	condicao_pedro = (
		(df_final['vendedor_fornecedor'] == 'Robson Roger Pereira') |
		(df_final['vendedor_fornecedor'] == 'Rep.YSA-MG')
	)

	# Crie a condição para a regra da Jessica
	condicao_jessica = (
		(df_final['vendedor_fornecedor'] == 'Raquel Pereira Purificacao Rosa') |
		(df_final['vendedor_fornecedor'] == 'Raquel Rosa') |
		(df_final['vendedor_fornecedor'] == 'Aline Cristina Abreu Lianza') |
		(df_final['vendedor_fornecedor'] == 'Carlos Rogerio Alves') |
		(df_final['vendedor_fornecedor'] == 'Daniel Rodrigues De Campos') |
		(df_final['vendedor_fornecedor'] == 'Murillo Souza') |
		(df_final['vendedor_fornecedor'] == 'Julio Cesar Campos Luz') |
		(df_final['vendedor_fornecedor'] == 'Arthur Gouveia Naccarati') |
		(df_final['vendedor_fornecedor'] == 'Juliana Souza') |
		(df_final['vendedor_fornecedor'] == 'Leticia Sampaio Figueiredo') |
		(df_final['vendedor_fornecedor'] == 'Kaio Vinicius Da Silva') |
		(df_final['vendedor_fornecedor'] == 'Milene Silva') |
		(df_final['vendedor_fornecedor'] == 'Mauricio Amorim') |
		(df_final['vendedor_fornecedor'] == 'Lilian Cristina Reis')
	)

	# Aplique as regras usando .loc
	df_final.loc[condicao_cassio, 'vendedor_alpe'] = 'Cassio'
	df_final.loc[condicao_rafael, 'vendedor_alpe'] = 'Rafael'
	df_final.loc[condicao_pedro, 'vendedor_alpe'] = 'Pedro'
	df_final.loc[condicao_jessica, 'vendedor_alpe'] = 'Jessica'

	# Exiba o DataFrame para verificar a mudança
	print("Regras aplicadas com sucesso!")

	# Limpeza final: remove colunas temporárias
	df_final = df_final.drop(columns=['nome_cedente_norm','nome_sacado_norm','uf_sacado_norm'])

	# Timestamp
	now = datetime.now(tz=timezone(timedelta(hours=-3)))
	df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
	df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

	# Reorganiza colunas na ordem desejada
	ordem_colunas = ['cnpj_sacado','cnpj_raiz','nome_sacado','cnpj_cedente','nome_cedente',
					'cod_vendedor_fornecedor','vendedor_fornecedor','escritorio_vendas','vendedor_alpe',
					'cep','municipio','uf','data','numero_nfe','valor_fatura','valor_fatura_pos_sefaz',
					'valor_fatura_oficial','valor_face_qprof','origem']


	print(f"O DataFrame final foi concluído com sucesso, contendo {df_final.shape[0]} linhas.")

	# Aplica a ordem e reseta o índice
	df_final = df_final[ordem_colunas].reset_index(drop=True)

	# Timestamp
	now = datetime.now(tz=timezone(timedelta(hours=-3)))
	df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
	df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

	
	# Configuração do Delta Lake
	storage_options = {
		"AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
		"AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
		"AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
		"AWS_REGION": "us-east-1",
		"AWS_S3_ALLOW_UNSAFE_RENAME": "true"
	}

	BUCKET_SOURCE_TRUSTED = "vendedor-fornecedor"
	FOLDER_DESTINATION_TRUSTED = "vendedor-fornecedor"

	# Escrevendo no Delta Lake com schema fixado
	write_deltalake(
		f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
		df_final,
		partition_by=["year", "month", "day"],
		storage_options=storage_options,
		mode="overwrite"
	)












	
