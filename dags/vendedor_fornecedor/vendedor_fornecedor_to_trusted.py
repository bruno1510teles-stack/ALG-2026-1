# Carregando libs
import pandas as pd
import numpy as np
import re
from datetime import datetime, timezone, timedelta
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from deltalake import write_deltalake
from unidecode import unidecode
from airflow.models import Variable



def vendedor_fornecedor_to_trusted(access_params=None,  **kwargs):

	# Conectando com o banco
	conn = connect(
		host='trino.alpe.tech',
		port=443,
		user='beatriz_anjos',
		auth=BasicAuthentication('beatriz_anjos', '[qRo!?0IpB&9P&-]*{SU'),
		http_scheme="https",
	)

	def execute_query(conn, query):
		cur = conn.cursor()  # Abre o cursor
		cur.execute(query)
		rows = cur.fetchall()
		columns = [desc[0] for desc in cur.description]
		cur.close()  # Fecha o cursor após a execução
		return pd.DataFrame(rows, columns=columns)
	
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
			
				i.seller_code AS cod_vendedor_arcelor,
			
				-- Nome do vendedor Arcelor
				COALESCE(NULLIF(TRIM(v."salesperson__r.name"), ''), v.nome) AS vendedor_arcelor,
			
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
	api_arcelor = execute_query(conn, query_arcelor)

	# Filtra o DataFrame para remover linhas onde 'numero_nfe' está vazio ou nulo 
	api_arcelor = api_arcelor.loc[api_arcelor['numero_nfe'].notna()]

	api_arcelor.loc[:, 'escritorio_vendas'] = api_arcelor['escritorio_vendas'].str.replace('Regional', 'Usina', regex=False)

	api_arcelor.loc[:, 'numero_nfe'] = api_arcelor['numero_nfe'].astype(str).str.strip()

	# Remove acentos e caracteres especiais de 'vendedor_arcelor'
	api_arcelor.loc[:, 'vendedor_arcelor'] = (
		api_arcelor['vendedor_arcelor']
		.apply(lambda x: unidecode(x) if pd.notna(x) else x)
		.str.replace(r'[^\w\s/]', '', regex=True)
	)

	# Remove acentos e caracteres especiais de 'escritorio_vendas'
	api_arcelor.loc[:, 'escritorio_vendas'] = (
		api_arcelor['escritorio_vendas']
		.apply(lambda x: unidecode(x) if pd.notna(x) else x)
		.str.replace(r'[^\w\s/]', '', regex=True)
	)

	valores_vazios = ['', 'nan', 'none', '#n/d', None, np.nan]

	# Função para normalizar nomes (remove acento e capitaliza)
	def normalizar_nome(nome):
		return unidecode(str(nome)).title()  # str() para evitar erro se vier None


	# Padroniza as colunas para string e strip
	for col in ['cod_vendedor_arcelor', 'vendedor_arcelor', 'escritorio_vendas']:
		api_arcelor.loc[:, col] = api_arcelor[col].astype(str).str.strip()

	#Trata código vendedor arcelor
	# Aplica a máscara considerando valores em minúsculas
	mask_cod_vazio = api_arcelor['cod_vendedor_arcelor'].str.lower().isin(valores_vazios)
	api_arcelor.loc[mask_cod_vazio, 'cod_vendedor_arcelor'] = 'Nao Atribuido'
	#===================================

	# Trata nome vendedor_arcelor
	mask_nome_vazio = api_arcelor['vendedor_arcelor'].str.lower().isin(valores_vazios)
	api_arcelor.loc[mask_nome_vazio, 'vendedor_arcelor'] = 'Nao Atribuido'
	#===================================

	# Trata código e nome vendedor_arcelor
	mask_nome_cod_vazios = mask_nome_vazio & mask_cod_vazio
	# Atualiza as duas colunas juntas para 'Não Atribuído' onde ambos estão vazios
	api_arcelor.loc[mask_nome_cod_vazios, ['vendedor_arcelor', 'cod_vendedor_arcelor']] = 'Nao Atribuido'
	#===================================

	# Onde nome está vazio e código NÃO está vazio, nome vira 'Não Atribuído'
	mask_nome_vazio_cod_ok = mask_nome_vazio & (~mask_cod_vazio)
	api_arcelor.loc[mask_nome_vazio_cod_ok, 'vendedor_arcelor'] = 'Nao Atribuido'

	#===================================
	# Trata escritorio_vendas
	mask_esc_vazio = api_arcelor['escritorio_vendas'].str.lower().isin(valores_vazios)
	api_arcelor.loc[mask_esc_vazio, 'escritorio_vendas'] = 'Nao Atribuido'
	#===================================

	# Trata vendedor_alpe 
	api_arcelor.loc[:, 'vendedor_alpe'] = api_arcelor['vendedor_alpe'].apply(
		lambda x: 'Nao Atribuido' if pd.isna(x) or str(x).strip().lower() in valores_vazios else str(x).strip()
	)
	#===================================

	# Função para normalizar nomes, preservando 'Não Atribuído'
	def normalizar_nome_condicional(nome):
		nome_str = str(nome).strip()
		if nome_str.lower() == 'nao atribuido':
			return 'Nao Atribuido'
		return unidecode(nome_str).title()

	api_arcelor.loc[:,'vendedor_arcelor'] = api_arcelor['vendedor_arcelor'].apply(normalizar_nome_condicional)

	# Dicionário com nome mais longo por código, ignorando registros 'Não Atribuído'
	nomes_padronizados = (
		api_arcelor[
			api_arcelor['cod_vendedor_arcelor'].notna() &
			(api_arcelor['cod_vendedor_arcelor'].str.strip().str.lower() != 'nao atribuido') &
			api_arcelor['vendedor_arcelor'].notna() &
			(api_arcelor['vendedor_arcelor'].str.strip().str.lower() != 'nao atribuido')
		]
		.groupby('cod_vendedor_arcelor')['vendedor_arcelor']
		.apply(lambda x: max(x, key=len))
		.to_dict()
	)

	# Atualiza os nomes direto no consolidado_venda com base no dicionário, mantendo 'Não Atribuído'
	api_arcelor.loc[:,'vendedor_arcelor'] = api_arcelor.apply(
		lambda r: nomes_padronizados.get(r['cod_vendedor_arcelor'], r['vendedor_arcelor'])
		if r['vendedor_arcelor'] != 'Nao Atribuido' else 'Nao Atribuido',
		axis=1
	)

	#Identificar origem para o concat
	api_arcelor = api_arcelor.assign(origem='api_arcelor')

	# Mantém somente as colunas necessárias no DataFrame final
	api_arcelor = api_arcelor[['numero_nfe', 'cod_vendedor_arcelor', 'vendedor_arcelor', 'escritorio_vendas', 'vendedor_alpe', 'origem']]

	#Base consolidado_venda
	query_consolidado_venda = f"""
			WITH base_venda AS (
				SELECT
					nf_completa AS numero_nfe,
					cod_vendedor AS cod_vendedor_arcelor, 
					vendedor AS vendedor_arcelor,
				CASE
					WHEN REGEXP_LIKE(regional, 'Teixeira.*Freitas') THEN 'DBA Teixeira Freitas'
					WHEN REGEXP_LIKE(regional, 'Ribeir.*Preto') THEN 'DBA Ribeirao Preto'
					WHEN REGEXP_LIKE(regional, 'Campos do Goytacaz.*') THEN 'DBA Campos do Goytacazes'
					-- Se o nome da regional começar com 'Regional ', troca esse prefixo por 'Usina ' 
					-- para alinhar com os valores da coluna 'filial_sem_acentuacao' na tabela de mapeamento
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
				bv.cod_vendedor_arcelor,
				bv.vendedor_arcelor,
				COALESCE(dpf.filial_consolidada, bv.escritorio_vendas_ajustado) AS escritorio_vendas,
				dpf.vendedor_alpe
			FROM base_venda bv
			LEFT JOIN de_para_filial dpf
				ON bv.escritorio_vendas_ajustado = dpf.filial_sem_acentuacao

	"""
	consolidado_venda = execute_query(conn, query_consolidado_venda)

	# Tratamentos consolidado_venda
	consolidado_venda.loc[:, 'escritorio_vendas'] = consolidado_venda['escritorio_vendas'].str.replace('Regional', 'Usina', regex=False)

	# Padroniza os valores como string limpa
	consolidado_venda['numero_nfe'] = consolidado_venda['numero_nfe'].astype(str).str.strip()

	# Remove linhas com valores indesejados
	consolidado_venda.drop(consolidado_venda.loc[(consolidado_venda['numero_nfe'].isin(['#N/D', '', 'nan']))].index,inplace=True)


	consolidado_venda.loc[:, 'vendedor_arcelor'] = (
		consolidado_venda['vendedor_arcelor']
		.apply(lambda x: unidecode(x) if pd.notna(x) else x)
		.str.replace(r'[^\w\s/]', '', regex=True)
	)

	consolidado_venda.loc[:, 'escritorio_vendas'] = (
		consolidado_venda['escritorio_vendas']
		.apply(lambda x: unidecode(x) if pd.notna(x) else x)
		.str.replace(r'[^\w\s/]', '', regex=True)
	)

	# Ajustes vendedor_arcelor realizados no dash
	consolidado_venda.loc[
		consolidado_venda['numero_nfe'] == '33250517469701016170550000003097211306183511',
		['vendedor_arcelor', 'escritorio_vendas']
	] = ['Fabio Fonseca da Silva', 'CDB Rio de Janeiro']

	consolidado_venda.loc[
		consolidado_venda['numero_nfe'] == '31250517469701003869550000004141241397867543',
		['vendedor_arcelor', 'escritorio_vendas']
	] = ['Raffaela Papa', 'CDB Belo Horizonte']

	valores_vazios = ['', 'nan', 'none', '#n/d', None, np.nan]

	# Função para normalizar nomes (remove acento e capitaliza)
	def normalizar_nome(nome):
		return unidecode(str(nome)).title()  # str() para evitar erro se vier None

	# Padroniza as colunas para string e strip
	for col in ['cod_vendedor_arcelor', 'vendedor_arcelor', 'escritorio_vendas']:
		consolidado_venda.loc[:, col] = consolidado_venda[col].astype(str).str.strip()

	# Trata código vendedor arcelor
	mask_cod_vazio = consolidado_venda['cod_vendedor_arcelor'].str.lower().isin(valores_vazios)
	consolidado_venda.loc[mask_cod_vazio, 'cod_vendedor_arcelor'] = 'Nao Atribuido'
	#===================================

	# Trata nome vendedor_arcelor
	mask_nome_vazio = consolidado_venda['vendedor_arcelor'].str.lower().isin(valores_vazios)
	consolidado_venda.loc[mask_nome_vazio, 'vendedor_arcelor'] = 'Nao Atribuido'
	#===================================

	# Trata código e nome vendedor_arcelor
	mask_nome_cod_vazios = mask_nome_vazio & mask_cod_vazio
	consolidado_venda.loc[mask_nome_cod_vazios, ['vendedor_arcelor', 'cod_vendedor_arcelor']] = 'Nao Atribuido'
	#===================================

	# Onde nome está vazio e código NÃO está vazio, nome vira 'Não Atribuído'
	mask_nome_vazio_cod_ok = mask_nome_vazio & (~mask_cod_vazio)
	consolidado_venda.loc[mask_nome_vazio_cod_ok, 'vendedor_arcelor'] = 'Nao Atribuido'
	#===================================

	# Trata escritorio_vendas
	mask_esc_vazio = consolidado_venda['escritorio_vendas'].str.lower().isin(valores_vazios)
	consolidado_venda.loc[mask_esc_vazio, 'escritorio_vendas'] = 'Nao Atribuido'
	#===================================

	# Trata vendedor_alpe 
	consolidado_venda.loc[:, 'vendedor_alpe'] = consolidado_venda['vendedor_alpe'].apply(
		lambda x: 'Nao Atribuido' if pd.isna(x) or str(x).strip().lower() in valores_vazios else str(x).strip()
	)
	#===================================

	# Função para normalizar nomes, preservando 'Não Atribuído'
	def normalizar_nome_condicional(nome):
		nome_str = str(nome).strip()
		if nome_str.lower() == 'nao atribuido':
			return 'Nao Atribuido'
		return unidecode(nome_str).title()

	consolidado_venda.loc[:, 'vendedor_arcelor'] = consolidado_venda['vendedor_arcelor'].apply(normalizar_nome_condicional)

	# Dicionário com nome mais longo por código, ignorando registros 'Não Atribuído'
	nomes_padronizados = (
		consolidado_venda[
			consolidado_venda['cod_vendedor_arcelor'].notna() &
			(consolidado_venda['cod_vendedor_arcelor'].str.strip().str.lower() != 'nao atribuido') &
			consolidado_venda['vendedor_arcelor'].notna() &
			(consolidado_venda['vendedor_arcelor'].str.strip().str.lower() != 'nao atribuido')
		]
		.groupby('cod_vendedor_arcelor')['vendedor_arcelor']
		.apply(lambda x: max(x, key=len))
		.to_dict()
	)

	# Atualiza os nomes direto no consolidado_venda com base no dicionário, mantendo 'Não Atribuído'
	consolidado_venda.loc[:, 'vendedor_arcelor'] = consolidado_venda.apply(
		lambda r: nomes_padronizados.get(r['cod_vendedor_arcelor'], r['vendedor_arcelor'])
		if r['vendedor_arcelor'] != 'Nao Atribuido' else 'Nao Atribuido',
		axis=1
	)

	# Identificar origem para o concat
	consolidado_venda['origem'] = 'consolidado_venda'

	consolidado_venda = consolidado_venda.drop_duplicates()

	#Base consolidado_venda_historico
	query_venda_historico = f"""
			WITH base_padronizada AS (
				SELECT
					nota_fiscal_completa,
					vendedor_am,
					filial_consolidada,
					vendedor_alpe
				FROM minioraw.planejamento_comercial.consolidado_venda_historico
			)
			
			SELECT DISTINCT
				bp.nota_fiscal_completa AS numero_nfe,
				bp.vendedor_am AS vendedor_arcelor,
				bp.filial_consolidada AS escritorio_vendas,
			
				COALESCE(dpr.regional_alpe, bp.vendedor_alpe) AS vendedor_alpe   
			
			FROM base_padronizada AS bp
			
			LEFT JOIN minioraw.planejamento_comercial.de_para_unidade_regional AS dpr
				ON bp.filial_consolidada = dpr.filial_consolidada

	"""
	consolidado_venda_historico = execute_query(conn, query_venda_historico)

	#Tratamentos consolidado_venda_historico
	consolidado_venda_historico.loc[:, 'escritorio_vendas'] = consolidado_venda_historico['escritorio_vendas'].str.replace('Regional', 'Usina', regex=False)

	consolidado_venda_historico['numero_nfe'] = consolidado_venda_historico['numero_nfe'].astype(str).str.strip()

	consolidado_venda_historico.loc[:, 'vendedor_arcelor'] = (
		consolidado_venda_historico['vendedor_arcelor']
		.apply(lambda x: unidecode(x) if pd.notna(x) else x)
		.str.replace(r'[^\w\s/]', '', regex=True)
	)

	consolidado_venda_historico.loc[:, 'escritorio_vendas'] = (
		consolidado_venda_historico['escritorio_vendas']
		.apply(lambda x: unidecode(x) if pd.notna(x) else x)
		.str.replace(r'[^\w\s/]', '', regex=True)
	)

	valores_vazios = ['', 'nan', 'none', '#n/d', None, np.nan]

	# Padroniza as colunas para string e strip
	for col in ['vendedor_arcelor', 'escritorio_vendas']:
		consolidado_venda_historico[col] = consolidado_venda_historico[col].astype(str).str.strip()

	# Trata vendedor_arcelor
	mask_nome_vazio = consolidado_venda_historico['vendedor_arcelor'].str.lower().isin(valores_vazios)
	consolidado_venda_historico.loc[mask_nome_vazio, 'vendedor_arcelor'] = 'Nao Atribuido'

	#===================================
	# Trata escritorio_vendas
	mask_esc_vazio = consolidado_venda_historico['escritorio_vendas'].str.lower().isin(valores_vazios)
	consolidado_venda_historico.loc[mask_esc_vazio, 'escritorio_vendas'] = 'Nao Atribuido'

	#===================================
	# Trata vendedor_alpe
	consolidado_venda_historico['vendedor_alpe'] = consolidado_venda_historico['vendedor_alpe'].apply(
		lambda x: 'Nao Atribuido' if pd.isna(x) or str(x).strip().lower() in valores_vazios else str(x).strip()
	)

	# Função para normalizar nomes, preservando 'Não Atribuído'
	def normalizar_nome_condicional(nome):
		nome_str = str(nome).strip()
		if nome_str.lower() == 'nao atribuido':
			return 'Nao Atribuido'
		return unidecode(nome_str).title()

	# Aplica a normalização apenas se o nome não for 'Não Atribuído'
	consolidado_venda_historico['vendedor_arcelor'] = consolidado_venda_historico['vendedor_arcelor'].apply(normalizar_nome_condicional)


	#Criar coluna antes da concatenação para evitar erros
	consolidado_venda_historico['cod_vendedor_arcelor'] = 'Nao Atribuido'

	#Identificar origem para o concat
	consolidado_venda_historico['origem'] = 'consolidado_venda_historico'

	#Concatenado das bases API Arcelor e manuais
	concatenado = pd.concat([api_arcelor, consolidado_venda, consolidado_venda_historico ], ignore_index=True)
	
	#Tratamento cod_vendedor_arcelor que fica em branco no df_consolidado_venda_historico, pois essa base não possui cod_vendedor_arcelor
	valores_vazios = ['', 'nan', 'none', '#n/d', None, np.nan]
	mask_cod_vazio = concatenado['cod_vendedor_arcelor'].str.lower().isin(valores_vazios)
	concatenado.loc[mask_cod_vazio, 'cod_vendedor_arcelor'] = 'Nao Atribuido'

	# Mapeia prioridade: menor número = maior prioridade
	prioridade = {'api_arcelor': 0, 'consolidado_venda': 1, 'consolidado_venda_historico': 2}
	concatenado['prioridade'] = concatenado['origem'].map(prioridade)

	# Ordena pela prioridade
	concatenado = concatenado.sort_values(by='prioridade')

	# Remove duplicatas, mantendo o de maior prioridade (primeiro após ordenação)
	concatenado = concatenado.drop_duplicates(
		subset=['numero_nfe', 'cod_vendedor_arcelor', 'vendedor_arcelor', 'escritorio_vendas', 'vendedor_alpe'],
		keep='first'
	)

	concatenado = concatenado.drop(columns='prioridade')

	#Base de Faturamento
	query_faturamento = f"""
			SELECT 
				data,
				cnpj_cedente,
				nome_cedente,
				cnpj_sacado,
				substr(cnpj_sacado, 1,8) as raiz_cnpj,
				nome_sacado,
				numero_nfe,
				valor_fatura_total,
				valor_fatura_pos_sefaz,
				valor_fatura_oficial,
				valor_face_qprof

			FROM deltalakerefined.payments.faturamento
	"""
	faturamento = execute_query(conn, query_faturamento)

	# Garante que nfe_access_key é string e remove espaços em branco antes e depois
	faturamento['numero_nfe'] = faturamento['numero_nfe'].astype(str).str.strip()

	faturamento['cnpj_sacado'] = faturamento['cnpj_sacado'].astype(str).str.strip().str.zfill(14)

	# Identificar origem para o concat
	faturamento['origem'] = 'faturamento'


	faturamento_arcelor = pd.merge(concatenado, faturamento, on='numero_nfe', how='outer', suffixes=('_concatenado', '_faturamento'))

	print(f"Quantidade de linhas df_concatenado: {concatenado.shape[0]}")
	print(f"Quantidade de linhas após cruzamento: {faturamento_arcelor.shape[0]}")

	cnpjs = faturamento_arcelor['cnpj_sacado'].unique()
	ids_cnpjs = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)

	#Base de localização
	query_localizacao = f"""
			SELECT  
				dc.cnpj_raiz as raiz_cnpj,
				dc.cnpj_sem_formatacao as cnpj_sacado,
				dc.cep,
				dc.uf,
				dc.municipio
			FROM deltalakerefined.receita_federal.dados_cadastrais AS dc
			WHERE cnpj_sem_formatacao in ({ids_cnpjs})
		"""
	localizacao = execute_query(conn, query_localizacao)

	#Tratamentos dados_cadastrais
	localizacao['cnpj_sacado'] = localizacao['cnpj_sacado'].astype(str).str.strip().str.zfill(14)


	final = pd.merge(faturamento_arcelor, localizacao, on='cnpj_sacado', how='left', suffixes=('_faturamento_arcelor', '_localizacao'))

	print(f"Quantidade de linhas faturamento_arcelor: {faturamento_arcelor.shape[0]}")
	print(f"Quantidade de linhas após cruzamento: {final.shape[0]}")

	# Tratamentos final

	# Tratamento origem
	final['origem'] = final['origem_concatenado'].combine_first(final['origem_faturamento'])
	final.drop(columns=['origem_concatenado', 'origem_faturamento'], inplace=True)

	# #Tratamento coluna raiz_cnpj
	final['raiz_cnpj_sacado'] = final['raiz_cnpj_faturamento_arcelor'].combine_first(final['raiz_cnpj_localizacao'])
	final.drop(columns=['raiz_cnpj_faturamento_arcelor', 'raiz_cnpj_localizacao'], inplace=True)

	# Converte as colunas listadas para o tipo string e remove espaços em branco antes e depois dos valores
	colunas = ['cnpj_sacado', 'raiz_cnpj_sacado', 'nome_sacado', 'cnpj_cedente', 'nome_cedente',
		'cod_vendedor_arcelor', 'vendedor_arcelor', 'escritorio_vendas', 'vendedor_alpe', 'origem',
		'cep', 'municipio', 'uf', 'numero_nfe']

	final[colunas] = final[colunas].astype(str).apply(lambda x: x.str.strip())

	final = final[['cnpj_sacado','raiz_cnpj_sacado','nome_sacado','cnpj_cedente','nome_cedente','cod_vendedor_arcelor','vendedor_arcelor','escritorio_vendas','vendedor_alpe','cep','municipio','uf','data','numero_nfe','valor_fatura_total','valor_fatura_pos_sefaz','valor_fatura_oficial','valor_face_qprof', 'origem']]

	# 1. Define colunas que serão tratadas
	colunas_para_tratar = ['cod_vendedor_arcelor', 'vendedor_arcelor', 'escritorio_vendas']
	valores_vazios = ['', 'nan', 'none', '#n/d', None, np.nan]

	# 2. Máscaras auxiliares

	# Cria uma máscara booleana onde o nome do cedente é válido 
	mascara_nome_valido = ~final['nome_cedente'].astype(str).str.strip().str.lower().isin(valores_vazios) # Identifica cedentes com nome válido (não vazio, nulo ou inválido)

	# Cria uma máscara onde o nome do cedente contém o termo "ARCELORMITTAL"
	mascara_arcelor = final['nome_cedente'] .astype(str).str.upper().str.contains('ARCELORMITTAL', na=False)  # Verifica se contém "ARCELORMITTAL"; ignora NaNs

	# Cria uma máscara onde o nome do cedente é válido e NÃO é da ArcelorMittal
	mascara_nao_arcelor = mascara_nome_valido & ~mascara_arcelor


	# 3. Preenche colunas para tratar
	for coluna in colunas_para_tratar:
		mascara_coluna_ausente = final[coluna].astype(str).str.strip().str.lower().isin(valores_vazios)
		final.loc[mascara_nao_arcelor & mascara_coluna_ausente, coluna] = "N/A"
		final.loc[mascara_arcelor & mascara_coluna_ausente, coluna] = "Nao encontrado"

	# 4. Função auxiliar para normalizar nomes
	def normalizar_nome(nome):
		if pd.isna(nome):
			return ""
		nome = str(nome).upper().strip()
		nome = unidecode(nome)
		return " ".join(nome.split())

	# 5. Mapeamento direto
	mapa_direto = {
		normalizar_nome("CASAL COMERCIO E SERVICOS LTDA"): "Rafael",
		normalizar_nome("CASA DO ADUBO S.A"): "Rafael",
		normalizar_nome("ASUS - INDUSTRIA DE MAQUINAS AGRICOLAS LTDA"): "Rafael",
		normalizar_nome("ASUS INDUSTRIA DE MAQUINAS AGRICOLAS LTDA"): "Rafael",
		normalizar_nome("DISCOR DISTRIBUIDORA DE TINTAS LTDA"): "Discor",
		normalizar_nome("APERAM INOX AMERICA DO SUL S.A"): "APERAM INOX AMERICA DO SUL S.A"
	}

	# 6. Função principal para definir 'vendedor_alpe'
	def definir_vendedor_alpe(row):
		nome_cedente = normalizar_nome(row.get('nome_cedente', ''))
		sacado = normalizar_nome(row.get('nome_sacado', ''))
		uf = row.get('uf', '').upper()

		if nome_cedente in mapa_direto:
			return mapa_direto.get(nome_cedente)

		if nome_cedente == "BELGO BEKAERT ARAMES LTDA":
			sacados_ignorar = [
				"TECNO ARAMES COMERCIO, IMPORTACAO E EXPORTACAO LTDA",
				"INNARA INDUSTRIA NACIONAL DE ARAMADOS LTDA",
				"RAC TELAS LTDA."
			]
			if sacado in sacados_ignorar:
				return "N/A"
			
			if uf in ['RS', 'SC', 'PR', 'SP', 'RJ', 'MG', 'ES']:
				return "Priscilla"

			sacados_wilma = [
				"CONSTRUTORA ALBATROZ REAL LTDA",
				"DRX CONSTRUCOES E INCORPORACOES LTDA",
				"MGA CONSTRUCOES E INCORPORACOES LTDA",
				"ABC E AGS MANAIRA PREMIUM CONSTRUCOES SP",
				"EDRO ENGENHARIA LTDA",
				"LAJES PONTE PEQUENA LTDA",
				"ACQUA VENTURE EUROPA II RESIDENCE SPE LT",
				"DIMENSIONAL CONSTRUCOES LTDA",
				"MARCOS ANDRADE EMPREENDIMENTOS EIRELI",
				"CITTA EMPREENDIMENTOS LTDA",
				"5S CONSTRUCAO E INCORPORACAO LTDA",
				"NACOES AM LMF CONSTRUCOES SPE LTDA",
				"BAUTEN CABO BRANCO MAR LTDA",
				"ILHEUS SELECT SPE LTDA",
				"HONU INTERMARES CONSTRUCAO E INCORPORACA"
			]
			if sacado in sacados_wilma:
				return "Wilma"
			
			if sacado == "PANUCCI PRE-MOLDADOS DE CONCRETOS LTDA":
				return "Pedro"
			
			return "Tiago"

		# Fallback: mantém valor original se já tiver algo na coluna
		vendedor_atual = row.get('vendedor_alpe', '')
		if vendedor_atual and str(vendedor_atual).strip().lower() not in valores_vazios:
			return vendedor_atual
		
		return "Nao Atribuido"

	# 7. Aplica a lógica para atribuir vendedor_alpe
	final['vendedor_alpe'] = final.apply(definir_vendedor_alpe, axis=1)


	# 8. Colunas de data

	# Timestamp e partições
	now = datetime.now(tz=timezone(timedelta(hours=-3)))
	final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
	final['year'], final['month'], final['day'] = now.year, now.month, now.day
	print("Tratamento dos dados concluído")

	
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
		final,
		partition_by=["year", "month", "day"],
		storage_options=storage_options,
		mode="overwrite"
	)












	
