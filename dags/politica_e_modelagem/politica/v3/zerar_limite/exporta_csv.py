# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from datetime import datetime, timezone, timedelta
import re
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from io import BytesIO
import re
import time


def execucao_politica_zerar_limites(access_params=None,  **kwargs):
	# Conectando com o Trino
	conn = connect(
		host=access_params['trino_endpoint'],
		port=access_params['trino_port'],
		user=access_params['trino_user'],
		auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
		http_scheme="https"
	)

	# Função para execução da query
	def execute_query(conn, query):
		cur = conn.cursor()  # Abre o cursor
		cur.execute(query)
		rows = cur.fetchall()
		columns = [desc[0] for desc in cur.description]
		cur.close()  # Fecha o cursor após a execução
		return pd.DataFrame(rows, columns=columns)

	# Query base boletos
	query_boletos =  f""" 
						select 
							numero_sequencial_titulo		
							,codigo_filial		
							,codigo_empresa		
							,numero_titulo		
							,codigo_cedente	
							,cedente_id		
							,codigo_sacado		
							,sacado_id	
							,status_titulo		
							,status_liquidez		
							,codigo_situacao_titulo		
							,data_emissao		
							,data_efetivacao		
							,data_vencimento		
							,data_baixa		
							,cast(valor_face as double) as valor_face	
							,cast(valor_titulo as double) as valor_titulo		
							,cast(valor_baixado as double) as valor_baixado 		
							,cast(valor_desagio as double) as valor_desagio		
							,rotulo		
							,codigo_estagio_titulo		
							,numero_nota_fiscal	
							,numero_nfe	
							,nome_fantasia_sacado	
							,nome_sacado	
							,cnpj_sacado	
							,uf_sacado	
							,cidade_sacado	
							,cnpj_cedente	
							,nome_cedente	
							,nome_fantasia_cedente	
							,safra_concessao		
							,safra_vencimento		
							,safra_baixa	
							,atualizado_em	
							,year	
							,month	
							,day	
						from deltalaketrusted.payments.boletos_internos
					"""

	boletos = execute_query(conn, query_boletos)

	# Query base limites
	query_limites =  f""" 
						select 	cnpj_sacado,
								cedente as pgid,
								sum(limite_atribuido) as limite_atribuido,
								sum(limite_disponivel) as limite_disponivel 
						from deltalaketrusted.limites.limite
						group by cnpj_sacado, cedente
					"""

	limites = execute_query(conn, query_limites)

	# Tratando coluna de data_vencimento para Datetime
	boletos['data_vencimento'] = pd.to_datetime(boletos['data_vencimento'], errors='coerce')

	# Tratando coluna de data_vencimento para Date
	boletos['data_vencimento'] = boletos['data_vencimento'].dt.date

	# Calculando dias vencidos
	hoje = datetime.today().date()
	boletos['dias_vencidos'] = ((boletos['data_vencimento'] - hoje).apply(lambda x: x.days)) * (-1)

	# Atualizando 'dias_vencidos' para 0 onde 'status_titulo' é diferente de 'VENCIDO'
	boletos.loc[boletos['status_titulo'] != 'VENCIDO', 'dias_vencidos'] = 0

	def tratar_cnpj(cnpj):
		cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
		return cnpj_limpo.zfill(14)

	# Tratando cnpj_sacado para merge
	boletos['cnpj_sacado'] = boletos['cnpj_sacado'].apply(tratar_cnpj)
	boletos['cnpj_cedente'] = boletos['cnpj_cedente'].apply(tratar_cnpj)

	# Merge boletos e limites
	boletos_limites = boletos.merge(limites, on='cnpj_sacado', how='left')

	# Tratando Limite Atribuido e Limite Disponivel Nan
	boletos_limites['limite_atribuido'] = boletos_limites['limite_atribuido'].fillna(0)
	boletos_limites['limite_disponivel'] = boletos_limites['limite_disponivel'].fillna(0)

	# Filtrando Base Vencer Limites
	vencer_limites = boletos_limites[
		(boletos_limites['dias_vencidos'] >= 30) & 
		(boletos_limites['limite_atribuido'] > 0)
	][[
		'cnpj_sacado', 'cnpj_cedente', 'nome_cedente', 'nome_fantasia_cedente', 
		'nome_sacado', 'nome_fantasia_sacado', 'limite_atribuido', 'pgid'
	]]

	vencer_limites = vencer_limites.drop_duplicates().reset_index(drop=True)

	# Adicionando colunas extras com os valores fixos para exportar
	vencer_limites['volume'] = 0
	vencer_limites['limite'] = 0
	vencer_limites[['codigo_filial', 'uf', 'cdb_dba', 'vendedor_alpe', 'vendedor_fn_nome', 'vendedor_fn_email', 'vendedor_fn_telefone']] = ""
	vencer_limites['prioridade'] = 6
	vencer_limites['policy'] = 'V3'
	vencer_limites['pre_filtro'] = 'sim'
	vencer_limites['bucket_pgid'] = 'urn-party-pgid-' + vencer_limites['pgid']

	# Organiza base exportação
	exporta_csv = vencer_limites[[
		'cnpj_sacado', 'volume', 'limite', 'nome_sacado', 'cnpj_cedente', 
		'codigo_filial', 'uf', 'cdb_dba', 'vendedor_alpe', 'vendedor_fn_nome', 
		'vendedor_fn_email', 'vendedor_fn_telefone', 'prioridade', 'policy', 'pre_filtro', 'bucket_pgid'
	]]

	# Agrupando por Bucket PGID
	exporta_csv_pgid = exporta_csv.groupby('bucket_pgid')


	# Configuração do cliente MinIO
	minio_client = Minio(
			"minio-api.alpenet.com.br",
			access_key="pe4MdrBZnRqrLUatARfZ",
			secret_key="d7RdWy02br3Q9Tsvmq8rOXDI9buAWlurPATmTFZh",
			secure=True
	)

	# Connection validation
	try:
		# Try to list the buckets
		buckets = minio_client.list_buckets()
		
		# If the connection was successful, print the buckests
		print("Conexão bem-sucedida. Lista de buckets disponíveis:")
		for bucket in buckets:
			print(bucket.name)
		
	except Exception as e:
			# If the connection was failed, print the error message
			print(f"Erro ao conectar ao MinIO: {e}")


	# Lista para armazenar buckets inexistentes
	buckets_inexistentes = []

	# Exportando cada grupo como CSV para o MinIO
	try:
		for bucket_pgid, grupo in exporta_csv_pgid:
			# Verificar se o bucket existe
			if not minio_client.bucket_exists(bucket_pgid):
				buckets_inexistentes.append(bucket_pgid)
				continue

			# Remover a coluna 'bucket_pgid' antes de exportar
			grupo = grupo.drop('bucket_pgid', axis=1)

			# Gerar o arquivo CSV em memória
			csv_buffer = BytesIO()
			grupo.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8', header=False)
			csv_buffer.seek(0)  # Voltar ao início do arquivo

			# Nome do arquivo e caminho
			object_name = f"politica-credito/direcionamento-analise/in/{bucket_pgid}.csv"  # Criar pasta "in" e nome do arquivo

			# Carregar o arquivo para o MinIO no bucket correto
			minio_client.put_object(
				bucket_name=bucket_pgid,
				object_name=object_name,
				data=csv_buffer,
				length=csv_buffer.getbuffer().nbytes,
				content_type='text/csv'
			)

			print(f"Arquivo {object_name} exportado para o bucket {bucket_pgid} com {len(grupo)} linhas.")
		
		# Verificação após exportação
		if buckets_inexistentes:
			print("Os seguintes buckets não existem no MinIO. Processo será interrompido:")
			for bucket in buckets_inexistentes:
				print(f"- {bucket}")
			# Interrompe o processo
			raise Exception("Processo interrompido!")

	except Exception as e:
		print(f"{e}")
		exit(1)

	# Timer de 1 minuto no final
	print("Aguardando 20 minutos antes de rodar o proximo processo...")
	time.sleep(1200)  # Aguardar 60 segundos (1 minuto)
