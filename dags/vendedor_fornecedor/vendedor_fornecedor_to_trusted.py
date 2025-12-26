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


	# 1. Base API da Arcelor
	query_arcelor = f"""
			WITH base_seller_info AS (
				SELECT 
					si.*,
			
					-- Ajustes do branch_name
					CASE
						WHEN REGEXP_LIKE(lower(TRIM(si.branch_name)), 'balc[aã]o.*bras[ií]lia') 
							THEN 'DBA Brasília'
						WHEN substr(TRIM(si.branch_name), 1, 9) = 'Regional ' 
							THEN 'Usina ' || substr(TRIM(si.branch_name), 10)
						WHEN REGEXP_LIKE(lower(TRIM(si.branch_name)), 'matcon.*sinop') 
							THEN 'DBA Sinop'
						WHEN REGEXP_LIKE(lower(TRIM(si.branch_name)), 'dba.*campos.*goyta')
							THEN 'DBA Campos dos Goytacazes'
						ELSE TRIM(si.branch_name)
					END AS branch_name_ajustado
				FROM postgres.knkt_fndt_{Variable.get('STAGE')}_default.seller_info si
			),
			
			base_instructions AS (
				SELECT
					i.*,
					bsi.branch_name_ajustado,
					bsi.seller_code
				FROM postgres.knkt_fndt_{Variable.get('STAGE')}_default.instruction i
				INNER JOIN base_seller_info bsi 
					ON i.seller_id = bsi.id
			)
			
			SELECT DISTINCT 
				i.id AS instruction_id,
				i.seller_id,
				i.amount,
				i.status,
				i.created_date AS instruction_created_date,
				i.last_modified_date AS instruction_last_modified_date,
			
				-- Escritorio ajustado
				CASE
					WHEN REGEXP_LIKE(lower(TRIM(i.branch_name_ajustado)), 'balc[aã]o.*bras[ií]lia') 
						THEN 'DBA Brasília'
					ELSE TRIM(i.branch_name_ajustado)
				END AS escritorio_venda,
			
				-- Código vendedor
				COALESCE(i.seller_code, v.salespersoncode) AS cod_vendedor_fornecedor,
			
				-- Nome vendedor fornecedor
				COALESCE(NULLIF(TRIM(v."salesperson__r.name"), ''), v.nome) AS vendedor_fornecedor,
			
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
				ON LPAD(TRIM(i.seller_code), 3, '0') = TRIM(v.salespersoncode)
			"""
	df_api_arcelor = execute_query(conn, query_arcelor)

	# Tratamentos df_api_arcelor

	# Função para limpar NFe
	def limpar_nfe(nfe):
		"""
		Remove tudo que não seja número de uma NFe.
		Se nulo, retorna None.
		"""
		if pd.isna(nfe):
			return None
		return re.sub(r'[^0-9]', '', str(nfe))

	# Limpeza do df_api_arcelor
	df_api_arcelor['numero_nfe'] = df_api_arcelor['numero_nfe'].apply(limpar_nfe)

	# Remove linhas com NFe nula ou vazia após limpeza
	df_api_arcelor = df_api_arcelor[df_api_arcelor['numero_nfe'].notna()]


	# Adiciona coluna de origem
	df_api_arcelor['origem'] = 'df_api_arcelor'

	# Seleciona colunas finais
	df_api_arcelor = df_api_arcelor[['numero_nfe', 'cod_vendedor_fornecedor', 'vendedor_fornecedor',
									'escritorio_venda', 'origem']]

	print(f"DataFrame Arcelor processado com sucesso. Linhas: {df_api_arcelor.shape[0]}")

	# 2. Base consolidado_vendas_unificadas

	print("\n=== Processamento df_consolidado_venda ===")

	query_consolidado_venda = f"""
	-- Base consolidado_vendas_unificadas
	WITH base_venda AS (
		SELECT 
			nfe AS numero_nfe,
			cod_vendedor AS cod_vendedor_fornecedor,
			vendedor_am AS vendedor_fornecedor,
			escritorio_venda
		FROM minioraw.planejamento_comercial.identificacao_nfe_arcelor
	)

	SELECT DISTINCT
		bv.numero_nfe,

		-- Código do vendedor (prioriza código; fallback por nome)
		COALESCE(
			v1.salespersoncode,       -- match pelo código
			v2.salespersoncode,       -- match pelo nome limpo
			bv.cod_vendedor_fornecedor
		) AS cod_vendedor_fornecedor,

		-- Nome do vendedor (prioriza nome completo da v1; depois v2; depois BV)
		COALESCE(
			NULLIF(TRIM(v1."salesperson__r.name"), ''),
			NULLIF(TRIM(v1.nome), ''),
			NULLIF(TRIM(v2."salesperson__r.name"), ''),
			NULLIF(TRIM(v2.nome), ''),
			bv.vendedor_fornecedor
		) AS vendedor_fornecedor,

		bv.escritorio_venda

	FROM base_venda bv

	-- 1 Join pelo código do vendedor
	LEFT JOIN minioraw.planejamento_comercial.vendedor_am v1
		ON TRIM(bv.cod_vendedor_fornecedor) = TRIM(v1.salespersoncode)

	-- 2 Fallback: join pelo nome exato (limpando parênteses do cadastro vendedor_am)
	LEFT JOIN minioraw.planejamento_comercial.vendedor_am v2
		ON UPPER(TRIM(bv.vendedor_fornecedor)) = UPPER(
			TRIM(
				REGEXP_REPLACE(v2.nome, '\\s*\\(.*\\)', '')   -- remove "(VR3)" por ex.
			)
		)

	"""
	df_consolidado_venda = execute_query(conn, query_consolidado_venda)

	# Tratamentos df_consolidado_venda 

	# Tamanho antes da limpeza
	qtd_antes = df_consolidado_venda.shape[0]

	# Aplica limpeza
	df_consolidado_venda['numero_nfe'] = df_consolidado_venda['numero_nfe'].apply(limpar_nfe)

	# Remove linhas com NFe nula ou vazia após limpeza
	df_consolidado_venda = df_consolidado_venda[df_consolidado_venda['numero_nfe'].notna()]

	# Tamanho depois da limpeza
	qtd_depois = df_consolidado_venda.shape[0]


	# Origem e colunas finais
	df_consolidado_venda['origem'] = 'df_consolidado_venda'
	df_consolidado_venda = df_consolidado_venda[['numero_nfe', 'cod_vendedor_fornecedor', 'vendedor_fornecedor',
												'escritorio_venda','origem']]


	print(f"DataFrame df_consolidado_venda processado com sucesso. Linhas: {qtd_depois}")
	print(f"NFe inválidas removidas: {qtd_antes - qtd_depois}")


	# --- Concatenação df_api_arcelor + df_consolidado_venda ---
	print("\n=== Concatenação df_api_arcelor + df_consolidado_venda ===")

	df_consolidado = pd.concat([df_api_arcelor, df_consolidado_venda], ignore_index=True)

	print("\nProcessamento completo! DataFrames prontos para uso.")
	print(f"Número total de linhas após a união: {df_consolidado.shape[0]}")


	# Tratamentos df_consolidado

	# Mapeia prioridade: menor número = maior prioridade
	prioridade = {'df_api_arcelor': 0, 'df_consolidado_venda': 1}
	df_consolidado['prioridade'] = df_consolidado['origem'].map(prioridade)

	# Ordena pela prioridade
	df_consolidado = df_consolidado.sort_values(by='prioridade')

	# Quantidade de linhas antes
	qtd_antes = df_consolidado.shape[0]
	print(f"Quantidade de linhas antes: {qtd_antes}")

	# Remove duplicatas, mantendo o de maior prioridade (primeiro após ordenação)
	df_consolidado = df_consolidado.drop_duplicates(subset=['numero_nfe'], keep='first')

	# Quantidade de linhas depois
	qtd_depois = df_consolidado.shape[0]

	# Remove a coluna de prioridade
	df_consolidado = df_consolidado.drop(columns='prioridade')

	print(f"Removidas {qtd_antes - qtd_depois} duplicatas com base em 'numero_nfe'.")
	print(f"Total de linhas finais: {qtd_depois}")


	# Base de Faturamento
	print("\n=== Processamento padronizado do df_faturamento ===")

	query_faturamento = f"""
			WITH faturamento_padronizado AS (
				SELECT 
					data,
					
					-- CNPJ cedente: somente números, 14 dígitos
					LPAD(REGEXP_REPLACE(cnpj_cedente, '[^0-9]', ''), 14, '0') AS cnpj_cedente,
					
					-- Nome cedente: padronização limpa
					UPPER(
						REGEXP_REPLACE(
							TRIM(REGEXP_REPLACE(nome_cedente, '\\s+', ' ')),  -- remove múltiplos espaços
							'[^\\w\\s/]|(?<=\\S)\\.$', ''                    -- remove pontuação, exceto '/', remove ponto final
						)
					) AS nome_cedente,
					
					-- CNPJ sacado: somente números, 14 dígitos
					LPAD(REGEXP_REPLACE(cnpj_sacado, '[^0-9]', ''), 14, '0') AS cnpj_sacado,
					
					-- Raiz CNPJ sacado
					SUBSTR(REGEXP_REPLACE(cnpj_sacado, '[^0-9]', ''), 1, 8) AS raiz_cnpj,
					
					-- Nome sacado: padronização limpa
					UPPER(
						REGEXP_REPLACE(
							TRIM(REGEXP_REPLACE(nome_sacado, '\\s+', ' ')),
							'[^\\w\\s/]|(?<=\\S)\\.$', ''
						)
					) AS nome_sacado,
					
					numero_nfe,
					valor_fatura,
					valor_fatura_pos_sefaz,
					valor_fatura_oficial,
					valor_face_qprof
				FROM deltalakerefined.payments.faturamento
			)
			
			SELECT *
			FROM faturamento_padronizado
	"""

	df_faturamento = execute_query(conn, query_faturamento)


	# Tratamentos df_faturamento

	# Captura quantidade antes da limpeza
	qtd_nfe_antes = df_faturamento.shape[0]

	for col in ['nome_cedente', 'nome_sacado']:
		df_faturamento[col] = df_faturamento[col].astype(str).str.strip()

	# Limpeza do df_faturamento
	df_faturamento['numero_nfe'] = df_faturamento['numero_nfe'].apply(limpar_nfe)

	# Remove linhas com NFe nula ou vazia após limpeza
	df_faturamento = df_faturamento[df_faturamento['numero_nfe'].notna()]

	df_faturamento['origem'] = 'df_faturamento'

	# Quantidade final após limpeza
	qtd_nfe_depois = df_faturamento.shape[0]

	print("\n=== Tratamento df_faturamento concluído ===")
	print(f"Linhas após limpeza: {qtd_nfe_depois}")
	print(f"NFe inválidas removidas: {qtd_nfe_antes - qtd_nfe_depois}")


	# Merge com faturamento
	print("\n=== Merge com df_consolidado ===")
	df_faturamento_total = pd.merge(df_consolidado, df_faturamento, on='numero_nfe', how='outer', suffixes=('_concatenado', '_faturamento'))


	# Remove linhas com data nula também
	df_faturamento_total = df_faturamento_total[df_faturamento_total['data'].notna()]

	# Ajuste da coluna origem
	df_faturamento_total['origem'] = df_faturamento_total['origem_concatenado'].combine_first(df_faturamento_total['origem_faturamento'])
	df_faturamento_total.drop(columns=['origem_concatenado', 'origem_faturamento'], inplace=True)

	print("\n=== Merge df_consolidado + df_faturamento concluído ===")
	print(f"Linhas totais após merge: {df_faturamento_total.shape[0]}")


	# Regra - Escritório de outros fornecedores 

	# Função de padronização
	def padronizar_nome(x):
		x = str(x).upper().replace('.', '').strip()
		x = re.sub(r'\s+', ' ', x)   # substitui múltiplos espaços por 1
		return x

	# Dicionário original
	mapeamento_escritorio = {
		'ARCELORMITTAL GONVARRI BRASIL PRODUTOS SIDERURGICOS S/A': 'ARCELORMITTAL GONVARRI BRASIL PRODUTOS SIDERURGICOS S/A',
		'APERAM INOX AMERICA DO SUL S.A': 'APERAM INOX AMERICA DO SUL S.A',
		'ASUS  INDUSTRIA DE MAQUINAS AGRICOLAS LTDA': 'ASUS - INDUSTRIA DE MAQUINAS AGRICOLAS LTDA',
		'CASA DO ADUBO SA': 'CASA DO ADUBO S.A',
		'CASAL COMERCIO E SERVICOS LTDA': 'CASAL COMERCIO E SERVICOS LTDA',
		'DISCOR DISTRIBUIDORA DE TINTAS LTDA': 'DISCOR DISTRIBUIDORA DE TINTAS LTDA',
		'MJR CUNHA DISTRIBUIDORA DE MATERIAIS PARA CONSTRUCAO LTDA': 'MJR CUNHA DISTRIBUIDORA DE MATERIAIS PARA CONSTRUCAO LTDA',
		'TUPER S/A': 'TUPER S/A'
	}

	# Padronizar as chaves do dicionário
	mapeamento_pad = {
		padronizar_nome(k): v
		for k, v in mapeamento_escritorio.items()
	}

	# Aplicar a regra diretamente no nome_cedente
	df_faturamento_total['escritorio_venda'] = (
		df_faturamento_total['nome_cedente']
			.apply(padronizar_nome)     # padroniza o texto
			.map(mapeamento_pad)        # aplica o mapeamento quando existir
			.combine_first(df_faturamento_total['escritorio_venda'])  # mantém valor existente
	)

	# Lista de valores considerados vazios (somente para limpeza dos nomes)
	valores_vazios = ['', 'nan', 'none', 'null', '#n/d', None, np.nan]

	# Nomes inválidos que não entram no aprendizado
	nomes_invalidos = ['Nao Atribuido']

	# Função para limpar colunas de texto (somente nomes)
	def limpar_colunas(df, colunas, valores_vazios):
		valores_vazios = [str(v).lower() for v in valores_vazios]
		for col in colunas:
			df[col] = df[col].astype(str).str.strip()
			df[col] = df[col].str.replace(r'[^a-zA-Z0-9À-ÿ\s./-]', '', regex=True)

			# Transformar apenas valores vazios em NaN 
			mask = df[col].str.lower().isin(valores_vazios)
			df.loc[mask, col] = None

	# Função para padronizar nomes
	def formatar_nome(nome):
		if pd.isna(nome):
			return None
		partes = nome.title().split()
		preposicoes = {"Da", "De", "Do", "Das", "Dos"}
		return ' '.join([p.lower() if p in preposicoes else p for p in partes])

	# Limpeza e padronização da coluna vendedor_fornecedor

	limpar_colunas(df_faturamento_total, ['vendedor_fornecedor'], valores_vazios)
	df_faturamento_total['vendedor_fornecedor'] = df_faturamento_total['vendedor_fornecedor'].apply(formatar_nome)

	print("\n--- 2. Atribuindo Códigos (Somente Aprendizado) ---")

	# Base de referência para aprendizado de códigos

	df_referencia = df_faturamento_total[
		df_faturamento_total['cod_vendedor_fornecedor'].notna() &
		(~df_faturamento_total['vendedor_fornecedor'].isin(nomes_invalidos)) &
		(df_faturamento_total['vendedor_fornecedor'].notna())
	].drop_duplicates(subset=['vendedor_fornecedor'])

	# Mapa vendedor → código aprendido
	mapa_codigos = df_referencia.set_index('vendedor_fornecedor')['cod_vendedor_fornecedor']

	# APLICAR O APRENDIZADO
	# Somente preencher códigos nulos com códigos aprendidos
	df_faturamento_total['cod_vendedor_fornecedor'] = df_faturamento_total['cod_vendedor_fornecedor'].fillna(
		df_faturamento_total['vendedor_fornecedor'].map(mapa_codigos)
	)


	print(f"Vendedores aprendidos: {df_referencia['vendedor_fornecedor'].nunique()}")
	print("--- Concluído! ---\n")


	# Enriquecimento com localização
	cnpjs = df_faturamento_total['cnpj_sacado'].dropna().unique()
	ids_cnpjs = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)


	# Base de localização
	print("\n=== Enriquecimento com localização ===")

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
	df_loc = execute_query(conn, query_localizacao)


	print("\n=== Merge com df_loc ===")

	# Armazena quantidade inicial de linhas antes do merge
	linhas_iniciais = df_faturamento_total.shape[0]

	df_faturamento_total = pd.merge(df_faturamento_total, df_loc, on='cnpj_sacado', how='left',  suffixes=('_faturamento_total', '_localizacao'))

	# Quantidade de linhas após o merge
	linhas_finais = df_faturamento_total.shape[0]
	delta_linhas = linhas_finais - linhas_iniciais

	print("\n=== Resultado do cruzamento com a base de localização ===")
	print(f"- Linhas antes do cruzamento: {linhas_iniciais}")
	print(f"- Linhas após o cruzamento: {linhas_finais}")

	if delta_linhas > 0:
		print(f"> Foram adicionadas {delta_linhas} linhas (possível efeito de múltiplas correspondências de CNPJs).")
	elif delta_linhas < 0:
		print(f"> Foram perdidas {abs(delta_linhas)} linhas (CNPJs sem correspondência na base de localização).")
	else:
		print("> O número de linhas permaneceu o mesmo após o cruzamento.")


	# Ajuste raiz CNPJ combinando a coluna do faturamento com a da localização
	df_faturamento_total['cnpj_raiz'] = df_faturamento_total['raiz_cnpj_faturamento_total'].combine_first(
		df_faturamento_total['raiz_cnpj_localizacao']
	)

	# Remover colunas antigas
	df_faturamento_total.drop(columns=['raiz_cnpj_faturamento_total', 'raiz_cnpj_localizacao'], inplace=True)


	#Tratamento cnpj_cedente
	for col in ['cnpj_cedente']:

		# df_faturamento_total
		if col in df_faturamento_total.columns:
			df_faturamento_total[col] = df_faturamento_total[col].astype(str).str.strip()	


	print("\n=== Enriquecimento do gerente_alpe através das exceções ===")

	query_excecoes = f"""
	WITH excecoes_padronizadas AS (
		SELECT 
			-- CNPJ cedente: somente números, 14 dígitos
			LPAD(REGEXP_REPLACE(cnpj_cedente, '[^0-9]', ''), 14, '0') AS cnpj_cedente,
			
			-- Nome cedente: padronização limpa
			UPPER(
				REGEXP_REPLACE(
					TRIM(REGEXP_REPLACE(cedente, '\\s+', ' ')),  -- remove múltiplos espaços
					'[^\\w\\s/]|(?<=\\S)\\.$', ''                 -- remove pontuação, exceto '/', remove ponto final
				)
			) AS nome_cedente,
			
			-- Nome sacado: padronização limpa
			UPPER(
				REGEXP_REPLACE(
					TRIM(REGEXP_REPLACE(sacado, '\\s+', ' ')),
					'[^\\w\\s/]|(?<=\\S)\\.$', ''
				)
			) AS nome_sacado,
			
			regiao_filial AS escritorio_venda,
			vendedor_externo AS vendedor_fornecedor,
			vendedor_alpe AS gerente_alpe
		FROM minioraw.planejamento_comercial.excecoes_identificacao_nfes
	)

	SELECT *
	FROM excecoes_padronizadas
	"""

	df_excecoes = execute_query(conn, query_excecoes)	


	# Limpeza e padronização dos nomes no df_excecoes
	limpar_colunas(df_excecoes, ['vendedor_fornecedor'], valores_vazios)
	df_excecoes['vendedor_fornecedor'] = df_excecoes['vendedor_fornecedor'].apply(formatar_nome)


	print("\n=== Aplicando regras de exceção para gerente_alpe ===")

	# REGRA 1 — exceções por (nome_cedente + nome_sacado)
	print("Aplicando Regra 1...")

	# Criar chave no faturamento
	df_faturamento_total['chave_excecao1'] = (
		df_faturamento_total['nome_cedente'].astype(str).str.strip().str.upper() + '|' +
		df_faturamento_total['nome_sacado'].astype(str).str.strip().str.upper()
	)

	# Criar chave no df_excecoes
	df_excecoes['chave_excecao1'] = (
		df_excecoes['nome_cedente'].astype(str).str.strip().str.upper() + '|' +
		df_excecoes['nome_sacado'].astype(str).str.strip().str.upper()
	)

	# Seleção da exceção
	df_exc_regra1 = df_excecoes[['chave_excecao1', 'gerente_alpe', 'escritorio_venda']].rename(
		columns={
			'gerente_alpe': 'gerente_alpe_regra1',
			'escritorio_venda': 'escritorio_venda_regra1'
		}
	)

	# Remove duplicados na chave para evitar múltiplas linhas
	df_exc_regra1 = df_exc_regra1.drop_duplicates(subset=['chave_excecao1'])

	# Criar dicionários de mapeamento
	mapa_gerente = df_exc_regra1.set_index('chave_excecao1')['gerente_alpe_regra1'].to_dict()
	mapa_escritorio = df_exc_regra1.set_index('chave_excecao1')['escritorio_venda_regra1'].to_dict()

	# Atualiza as colunas no df_faturamento_total
	df_faturamento_total['gerente_alpe'] = df_faturamento_total['chave_excecao1'].map(mapa_gerente)
	df_faturamento_total['escritorio_venda'] = df_faturamento_total['chave_excecao1'].map(mapa_escritorio).combine_first(df_faturamento_total['escritorio_venda'])

	# Limpeza da coluna auxiliar
	df_faturamento_total.drop(columns=['chave_excecao1'], inplace=True, errors='ignore')

	print("Regra 1 aplicada com sucesso!")	


	print("Aplicando Regra 2...")

	# Condição para permitir aplicar regra 2 (vendedor válido)
	mask_vendedor_valido = (
		df_faturamento_total['vendedor_fornecedor']
			.notna()
			.astype(bool) & 
		(df_faturamento_total['vendedor_fornecedor'].astype(str).str.strip() != '')
	)

	# Criar chave secundária no faturamento SOMENTE onde vendedor é válido
	df_faturamento_total.loc[mask_vendedor_valido, 'chave_excecao2'] = (
		df_faturamento_total.loc[mask_vendedor_valido, 'cnpj_cedente'].astype(str).str.strip() + '|' +
		df_faturamento_total.loc[mask_vendedor_valido, 'vendedor_fornecedor'].astype(str).str.strip().str.upper()
	)

	# Criar chave nas exceções (normal)
	df_excecoes['chave_excecao2'] = (
		df_excecoes['cnpj_cedente'].astype(str).str.strip() + '|' +
		df_excecoes['vendedor_fornecedor'].astype(str).str.strip().str.upper()
	)

	# Seleção e dicionário
	df_exc_regra2 = df_excecoes[['chave_excecao2', 'gerente_alpe']].rename(
		columns={'gerente_alpe': 'gerente_alpe_regra2'}
	).drop_duplicates(subset=['chave_excecao2'])

	mapa_gerente2 = df_exc_regra2.set_index('chave_excecao2')['gerente_alpe_regra2'].to_dict()

	# Aplicar somente onde vendedor é válido E gerente_alpe ainda está vazio
	df_faturamento_total.loc[mask_vendedor_valido, 'gerente_alpe'] = (
		df_faturamento_total.loc[mask_vendedor_valido, 'gerente_alpe']
			.combine_first(
				df_faturamento_total.loc[mask_vendedor_valido, 'chave_excecao2'].map(mapa_gerente2)
			)
	)

	df_faturamento_total.drop(columns=['chave_excecao2'], inplace=True, errors='ignore')

	print("Regra 2 aplicada com sucesso! Ignorando vendedores em branco.")


	# Atualização do gerente_alpe via atuação
	print("\n=== Enriquecimento do gerente_alpe através da atuacao regional ===")

	query_atuacao = f"""
	SELECT
		LPAD(REGEXP_REPLACE(cnpj_cedente, '[^0-9]', ''), 14, '0') AS cnpj_cedente,
		filial_fn AS escritorio_venda,
		uf,
		vendedor_alpe AS gerente_alpe
	FROM minioraw.planejamento_comercial.atuacao_regional_comercial
	"""

	df_atuacao = execute_query(conn, query_atuacao)


	print("\n=== Aplicando Regra 3 (UF + CNPJ, somente BELGO, sem sobrescrever Regra 1) ===")

	# REGRA 3 - Enriquecer o escritorio_venda e gerente_alpe nos casos Belgo da origem df_faturamento

	# 1 — Mascara BELGO
	mask_belgo = df_faturamento_total['nome_cedente'].str.upper() == "BELGO BEKAERT ARAMES LTDA"

	# 2 — Criar chave da REGRA 1 para identificar quem já foi tratado
	df_faturamento_total['chave_r1'] = (
		df_faturamento_total['nome_cedente'].astype(str).str.strip().str.upper() + '|' +
		df_faturamento_total['nome_sacado'].astype(str).str.strip().str.upper()
	)

	df_excecoes['chave_r1'] = (
		df_excecoes['nome_cedente'].astype(str).str.strip().str.upper() + '|' +
		df_excecoes['nome_sacado'].astype(str).str.strip().str.upper()
	)

	# Conjunto de chaves tratadas pela Regra 1
	chaves_r1 = set(df_excecoes['chave_r1'].unique())

	mask_regra1_aplicada = df_faturamento_total['chave_r1'].isin(chaves_r1)

	# 3 — Regra 3 só pode atuar quando:
	#    - é BELGO
	#    - NÃO foi tratado pela Regra 1
	mask_regra3_pode_atuar = (
		mask_belgo &
		(~mask_regra1_aplicada)
	)

	# 4 — Criar chave UF para quem pode receber a Regra 3
	df_faturamento_total.loc[mask_regra3_pode_atuar, 'chave_uf'] = (
		df_faturamento_total.loc[mask_regra3_pode_atuar, 'cnpj_cedente'].astype(str).str.strip() + '|' +
		df_faturamento_total.loc[mask_regra3_pode_atuar, 'uf'].astype(str).str.strip()
	)

	# Criar chave no df_atuacao
	df_atuacao['chave_uf'] = (
		df_atuacao['cnpj_cedente'].astype(str).str.strip() + '|' +
		df_atuacao['uf'].astype(str).str.strip()
	)

	# Dicionários
	df_atuacao_map = df_atuacao[['chave_uf', 'escritorio_venda', 'gerente_alpe']].drop_duplicates()

	map_escritorio = df_atuacao_map.set_index('chave_uf')['escritorio_venda'].to_dict()
	map_gerente = df_atuacao_map.set_index('chave_uf')['gerente_alpe'].to_dict()

	# 5 — Preencher escritório_venda quando estiver vazio
	df_faturamento_total.loc[
		mask_regra3_pode_atuar &
		(df_faturamento_total['escritorio_venda'].isna() | (df_faturamento_total['escritorio_venda'] == "")),
		'escritorio_venda'
	] = df_faturamento_total.loc[mask_regra3_pode_atuar, 'chave_uf'].map(map_escritorio)

	# 6 — Preencher gerente_alpe quando estiver vazio
	df_faturamento_total.loc[
		mask_regra3_pode_atuar &
		(df_faturamento_total['gerente_alpe'].isna() | (df_faturamento_total['gerente_alpe'] == "")),
		'gerente_alpe'
	] = df_faturamento_total.loc[mask_regra3_pode_atuar, 'chave_uf'].map(map_gerente)

	# 7 — Limpeza final
	df_faturamento_total.drop(columns=['chave_r1', 'chave_uf'], inplace=True, errors='ignore')

	print("Regra 3 aplicada com sucesso!")


	# Merge direto no df_faturamento_total
	df_faturamento_total = df_faturamento_total.merge(
		df_atuacao[['cnpj_cedente', 'escritorio_venda', 'uf', 'gerente_alpe']],
		on=['cnpj_cedente', 'escritorio_venda', 'uf'],
		how='left',
		suffixes=('', '_atuacao')
	)

	# Atualiza apenas os valores que estão em NaN
	df_faturamento_total['gerente_alpe'] = df_faturamento_total['gerente_alpe'].combine_first(
		df_faturamento_total['gerente_alpe_atuacao']
	)

	# Remove a coluna auxiliar
	df_faturamento_total.drop(columns=['gerente_alpe_atuacao'], inplace=True)


	# Carregando sacados_planos

	print("\n=== Enriquecimento do gerente_alpe através do cnpj_raiz dos sacados de planos ===")

	query_sacados_planos = f"""
			WITH sacados_padronizados AS (
				SELECT 
					-- CNPJ cedente: somente números, 14 dígitos
					LPAD(REGEXP_REPLACE(cnpj_cedente, '[^0-9]', ''), 14, '0') AS cnpj_cedente,
					
					-- Nome cedente: padronização limpa
					UPPER(
						REGEXP_REPLACE(
							TRIM(REGEXP_REPLACE(cedente, '\\s+', ' ')),  -- remove múltiplos espaços
							'[^\\w\\s/]|(?<=\\S)\\.$', ''               -- remove pontuação, exceto '/', remove ponto final
						)
					) AS nome_cedente,
					
					-- Nome sacado: padronização limpa
					UPPER(
						REGEXP_REPLACE(
							TRIM(REGEXP_REPLACE(sacado, '\\s+', ' ')),
							'[^\\w\\s/]|(?<=\\S)\\.$', ''
						)
					) AS nome_sacado,
					
					raiz_cnpj_sacado AS cnpj_raiz,
					filial AS escritorio_venda,
					vendedor_alpe AS gerente_alpe
				FROM minioraw.planejamento_comercial.sacados_planos
			)
			
			SELECT *
			FROM sacados_padronizados
	"""

	df_sacados_planos = execute_query(conn, query_sacados_planos)


	# REGRA 4 — Enriquecimento do gerente_alpe via sacados_planos
	print("=== Enriquecimento do gerente_alpe através do cnpj_raiz e cnpj_cedente dos sacados de planos ===")

	# Merge direto usando cnpj_raiz + cnpj_cedente
	df_faturamento_total = df_faturamento_total.merge(
		df_sacados_planos[['cnpj_raiz', 'cnpj_cedente', 'escritorio_venda', 'gerente_alpe']],
		left_on=['cnpj_raiz', 'cnpj_cedente'],
		right_on=['cnpj_raiz', 'cnpj_cedente'],
		how='left',
		suffixes=('', '_sacado')
	)

	# Condição de enriquecimento:
	# - faturamento NÃO possui gerente
	# - sacados_planos possui gerente
	# - e NÃO há conflito entre escritorio_venda (faturamento) e filial (sacados)
	condicao_enriquecimento_planos = (
		df_faturamento_total['gerente_alpe'].isna()
		& df_faturamento_total['gerente_alpe_sacado'].notna()
		& (
			df_faturamento_total['escritorio_venda'].isna()
			| (
				df_faturamento_total['escritorio_venda']
				== df_faturamento_total['escritorio_venda_sacado']
			)
		)
	)

	# Aplica enriquecimento SOMENTE quando a condição é verdadeira
	df_faturamento_total.loc[
		condicao_enriquecimento_planos, 'gerente_alpe'
	] = df_faturamento_total.loc[
		condicao_enriquecimento_planos, 'gerente_alpe_sacado'
	]

	df_faturamento_total.loc[
		condicao_enriquecimento_planos, 'escritorio_venda'
	] = df_faturamento_total.loc[
		condicao_enriquecimento_planos, 'escritorio_venda_sacado'
	]

	# Remove colunas auxiliares
	df_faturamento_total.drop(columns=['gerente_alpe_sacado', 'escritorio_venda_sacado'],inplace=True)

	print("Enriquecimento via sacados_planos aplicado com sucesso!")


	# REGRA 5 —  A partir do cnpj cedente de outros fns, trazer o nome do gerente alpe
	print("\n=== Enriquecimento do gerente_alpe através do cnpj_cedente de outros fornecedores ===")

	query_outros_fns = f"""
			WITH outros_fns_padronizados AS (
				SELECT 
					-- CNPJ cedente: somente números, 14 dígitos
					LPAD(REGEXP_REPLACE(cnpj_cedente, '[^0-9]', ''), 14, '0') AS cnpj_cedente,
			
					-- Nome cedente: padronização limpa
					UPPER(
						REGEXP_REPLACE(
							TRIM(REGEXP_REPLACE(cedente, '\\s+', ' ')),       -- remove múltiplos espaços
							'[^\\w\\s/]|(?<=\\S)\\.$', ''                     -- remove pontuação, exceto '/', remove ponto final
						)
					) AS nome_cedente,
			
					vendedor_alpe AS gerente_alpe
				FROM minioraw.planejamento_comercial.atendimento_alpe_outros_fn
			)
			
			SELECT *
			FROM outros_fns_padronizados
	"""

	df_outros_fns = execute_query(conn, query_outros_fns)


	df_faturamento_total = df_faturamento_total.merge(
		df_outros_fns[['cnpj_cedente', 'gerente_alpe']],
		on='cnpj_cedente',
		how='left',
		suffixes=('', '_fns')
	)

	df_faturamento_total['gerente_alpe'] = df_faturamento_total['gerente_alpe_fns'].where(
		df_faturamento_total['gerente_alpe_fns'].notna(),
		df_faturamento_total['gerente_alpe']
	)

	df_faturamento_total.drop(columns=['gerente_alpe_fns'], inplace=True)

	print("Regras outros fns aplicadas e colunas auxiliares removidas!")


	# REGRA 6 —  A partir da filial, trazer o nome do gerente alpe para o que ficou vazio nas regras anteriores
	print("\n=== Enriquecimento do gerente_alpe que ainda ficaram em branco através da filial do de_para ===")

	query_de_para = f"""
		SELECT 
		filial,
		filial_sem_acentuacao,
		filial_consolidada,
		regional_alpe as gerente_alpe_de_para
		FROM minioraw.planejamento_comercial.de_para_unidade_regional 

	"""

	df_de_para = execute_query(conn, query_de_para)	


	# TRATAMENTO PRÉVIO 
	# Garante que "espaços em branco" ou strings vazias sejam tratados como NaN para o fillna funcionar
	df_faturamento_total['gerente_alpe'] = df_faturamento_total['gerente_alpe'].replace(r'^\s*$', np.nan, regex=True)

	# CRIAÇÃO DOS DICIONÁRIOS DE MAPEAMENTO

	# Dicionários para buscar o GERENTE (Chave = Variação do Nome, Valor = Gerente)
	map_por_filial = df_de_para.set_index('filial')['gerente_alpe_de_para'].to_dict()
	map_por_sem_acento = df_de_para.set_index('filial_sem_acentuacao')['gerente_alpe_de_para'].to_dict()
	map_por_consolidada = df_de_para.set_index('filial_consolidada')['gerente_alpe_de_para'].to_dict()

	# Dicionário para padronizar o NOME do escritório (Chave = Variação, Valor = Nome Consolidado)
	# A CORREÇÃO ESTÁ AQUI: usamos drop=False na última linha para não perder a coluna
	map_nomes_padronizados = {
		**df_de_para.set_index('filial')['filial_consolidada'].to_dict(),
		**df_de_para.set_index('filial_sem_acentuacao')['filial_consolidada'].to_dict(),
		**df_de_para.set_index('filial_consolidada', drop=False)['filial_consolidada'].to_dict()
	}

	# APLICAÇÃO DA LÓGICA EM CASCATA 
	print("Aplicando regras de preenchimento...")

	# PASSO 1: Tenta preencher batendo com 'filial'
	# O fillna garante que só preenchemos onde está vazio (não sobrescreve regras anteriores)
	df_faturamento_total['gerente_alpe'] = df_faturamento_total['gerente_alpe'].fillna(
		df_faturamento_total['escritorio_venda'].map(map_por_filial)
	)

	# PASSO 2: Tenta preencher batendo com 'filial_sem_acentuacao'
	# Só afeta quem continuou vazio após o Passo 1
	df_faturamento_total['gerente_alpe'] = df_faturamento_total['gerente_alpe'].fillna(
		df_faturamento_total['escritorio_venda'].map(map_por_sem_acento)
	)

	# PASSO 3: Tenta preencher batendo com 'filial_consolidada'
	# Só afeta quem continuou vazio após o Passo 2
	df_faturamento_total['gerente_alpe'] = df_faturamento_total['gerente_alpe'].fillna(
		df_faturamento_total['escritorio_venda'].map(map_por_consolidada)
	)

	# PASSO 4: Padronização do Nome (escritorio_venda vira filial_consolidada)
	# Se encontrar o escritorio_venda no dicionário, substitui pelo consolidado.
	# Se não encontrar, mantém o original (fillna com ele mesmo).
	df_faturamento_total['escritorio_venda'] = df_faturamento_total['escritorio_venda'].map(map_nomes_padronizados).fillna(df_faturamento_total['escritorio_venda'])

	# Verificação final
	print("Enriquecimento do gerente_alpe e escritorio_venda no de_para concluído com sucesso.")


	# Quantidade de linhas antes
	qtd_antes = df_faturamento_total.shape[0]

	# Remove linhas duplicadas considerando todas as colunas
	df_faturamento_total = df_faturamento_total.drop_duplicates(keep='first')

	# Quantidade de linhas depois
	qtd_depois = df_faturamento_total.shape[0]

	print(f"Duplicatas removidas: {qtd_antes - qtd_depois}")
	print(f"Total de linhas finais: {qtd_depois}")


	# Aplicar ajustes manuais
	ajustes_manuaes_nfe = {
		'35241117469701002897550010015281941001123558': {
			'gerente_alpe': 'Nicolas',
			'cod_vendedor_fornecedor': 'E08'
		}
	}

	for nfe, ajustes in ajustes_manuaes_nfe.items():
		df_faturamento_total.loc[
			df_faturamento_total['numero_nfe'] == nfe,
			['gerente_alpe', 'cod_vendedor_fornecedor']
		] = [ajustes['gerente_alpe'], ajustes['cod_vendedor_fornecedor']]

	
	# Setar "Não Atribuído" SOMENTE onde ainda está vazio
	cols = ['cod_vendedor_fornecedor', 'vendedor_fornecedor', 'escritorio_venda', 'gerente_alpe']

	for col in cols:
		df_faturamento_total.loc[
			df_faturamento_total[col].isna() | (df_faturamento_total[col] == ''),
			col
		] = 'Não Atribuído'


	# Reorganiza colunas na ordem desejada
	ordem_colunas = ['cnpj_sacado','cnpj_raiz','nome_sacado','cnpj_cedente','nome_cedente',
					'cod_vendedor_fornecedor','vendedor_fornecedor','escritorio_venda','gerente_alpe',
					'cep','municipio','uf','data','numero_nfe','valor_fatura','valor_fatura_pos_sefaz',
					'valor_fatura_oficial','valor_face_qprof','origem']

	print(f"O DataFrame final foi concluído com sucesso, contendo {df_faturamento_total.shape[0]} linhas.")

	# Aplica a ordem e reseta o índice
	df_faturamento_total = df_faturamento_total[ordem_colunas].reset_index(drop=True)


	# Timestamp
	now = datetime.now(tz=timezone(timedelta(hours=-3)))
	df_faturamento_total['atualizado_em'] = now.strftime('%Y-%m-%d %X')
	df_faturamento_total['year'], df_faturamento_total['month'], df_faturamento_total['day'] = now.year, now.month, now.day

	
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
		df_faturamento_total,
		partition_by=["year", "month", "day"],
		storage_options=storage_options,
		mode="overwrite"
	)












	
