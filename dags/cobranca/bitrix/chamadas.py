import pandas as pd
import requests
import time
from datetime import datetime, timezone, timedelta
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from deltalake import write_deltalake

def bitrix_chamadas_to_trusted (access_params=None, **kwargs):

    WEBHOOK_URL = "https://alpenet.bitrix24.com.br/rest/49586/ek5qbi3clr1jhr0r/"
    
    # Conexão Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    def bitrix_call(method, params=None):
        time.sleep(0.5) 
        url = WEBHOOK_URL + method
        response = requests.post(url, json=params or {})
        if response.status_code == 429:
            print("Limite atingido. Aguardando 2 segundos...")
            time.sleep(2)
            return bitrix_call(method, params)
        response.raise_for_status()
        return response.json()

    def bitrix_list_all(method, params):
        all_items = []
        start = 0
        while True:
            params['start'] = start
            result = bitrix_call(method, params)
            items = result.get('result', [])
            all_items.extend(items)
            if 'next' not in result or not items: break
            start = result['next']
        return all_items

    # Mapa de Usuários (Nome do Responsável)
    print("Mapeando usuários...")
    usuarios = bitrix_call('user.get', {'ACTIVE': 'Y'}).get('result', [])
    mapa_usuarios = {str(u['ID']): f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip() for u in usuarios}

    # Chamadas (Atividades)
    print('Extraindo chamadas do Bitrix...')
    atividades = bitrix_list_all('crm.activity.list', {
        'filter': { 'TYPE_ID': 2, '>=START_TIME': '2025-01-01T00:00:00'},
        'select': ['ID', 'OWNER_ID', 'RESPONSIBLE_ID', 'START_TIME', 'END_TIME', 'SUBJECT']
    })    
    
    df_atividades = pd.DataFrame(atividades).rename(columns={'ID': 'id_chamada'})
    if df_atividades.empty:
        return print("Nenhuma atividade encontrada.")

    # Negócios (Filtro Categoria 31 - Alpe Cobranca)
    ids_negocios = df_atividades['OWNER_ID'].unique().tolist()
    negocios = []
    
    for i in range(0, len(ids_negocios), 50):
        res_negocios = bitrix_call('crm.deal.list', {
            'filter': {'ID': ids_negocios[i:i+50], 'CATEGORY_ID': 31},
            'select': ['ID', 'COMPANY_ID', 'UF_CRM_1601660600', 'UF_CRM_1649791013']
        }).get('result', [])
        negocios.extend(res_negocios)
    
    df_negocios = pd.DataFrame(negocios).rename(columns={'ID': 'id_negocio'})
    if df_negocios.empty:
        return print("Sem chamadas vinculadas à Cobrança.")

    # Empresas
    ids_empresas = df_negocios['COMPANY_ID'].unique().tolist()
    empresas = []
    
    for i in range(0, len(ids_empresas), 50):
        res_empresas = bitrix_call('crm.company.list', {
            'filter': {'ID': ids_empresas[i:i+50]},
            'select': ['ID', 'TITLE', 'UF_CRM_1588195367']
        }).get('result', [])
        empresas.extend(res_empresas)
    
    df_empresas = pd.DataFrame(empresas)    
    
    # Merge
    df_chamadas = pd.merge(df_atividades, df_negocios, left_on='OWNER_ID', right_on='id_negocio', how='inner')
    df_chamadas = pd.merge(df_chamadas, df_empresas, left_on='COMPANY_ID', right_on='ID', how='left')

    print("Transformando dados...")

    # Ajuste de Datas
    inicio_chamada = pd.to_datetime(df_chamadas['START_TIME']).dt.tz_convert('America/Sao_Paulo').dt.tz_localize(None)
    fim_chamada = pd.to_datetime(df_chamadas['END_TIME']).dt.tz_convert('America/Sao_Paulo').dt.tz_localize(None)
    
    df_chamadas['duracao_segundos'] = (fim_chamada - inicio_chamada).dt.total_seconds().fillna(0).astype(int)
    df_chamadas['inicio_chamada'] = inicio_chamada.dt.strftime('%Y-%m-%d %H:%M:%S')
    df_chamadas['fim_chamada'] = fim_chamada.dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df_chamadas['discado'] = 1
    df_chamadas['atendido'] = (df_chamadas['duracao_segundos'] > 3).astype(int)
    df_chamadas['alo'] = (df_chamadas['duracao_segundos'] >= 10).astype(int)
    df_chamadas['nome_responsavel'] = df_chamadas['RESPONSIBLE_ID'].astype(str).map(mapa_usuarios)

    # Renomeando colunas
    df_chamadas = df_chamadas.rename(columns={
        'UF_CRM_1649791013': 'cnpj_cedente',
        'UF_CRM_1601660600': 'cedente', 
        'UF_CRM_1588195367': 'cnpj_sacado', 
        'TITLE': 'nome_sacado',
        'SUBJECT': 'descricao',
        'RESPONSIBLE_ID': 'id_responsavel'
    })

    # Remove qualquer caractere que não seja número
    df_chamadas['cnpj_sacado'] = df_chamadas['cnpj_sacado'].astype(str).str.replace(r'\D', '', regex=True)
    df_chamadas['cnpj_cedente'] = df_chamadas['cnpj_cedente'].astype(str).str.replace(r'\D', '', regex=True)

    colunas_finais = [
        'id_negocio', 'id_chamada', 'cnpj_sacado', 'nome_sacado', 'inicio_chamada', 
        'fim_chamada', 'duracao_segundos', 'id_responsavel', 'nome_responsavel', 
        'cnpj_cedente', 'cedente', 'descricao', 'discado', 'atendido', 'alo'
    ]
    
    df_chamadas = df_chamadas[colunas_finais].copy()
    df_chamadas = df_chamadas.reset_index(drop=True)
    
    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_chamadas['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_chamadas['year'], df_chamadas['month'], df_chamadas['day'] = now.year, now.month, now.day

    print("Tratamento dos dados concluído")
    
    # Incremental
    print("Verificando IDs existentes...")
    ids_existentes = set()
    try:
        cur = conn.cursor()
        cur.execute("SELECT CAST(id_chamada AS VARCHAR) FROM deltalaketrusted.cobranca.bitrix_chamadas")
        ids_existentes = set([row[0] for row in cur.fetchall()])
        cur.close()
    except Exception as e:
        print(f"Processando todos os registros.")

    df_incremental = df_chamadas[~df_chamadas['id_chamada'].astype(str).isin(ids_existentes)].copy()
    print(f"{len(df_incremental)} novos registros identificados.")
    
    if df_incremental.empty:
        return print("Finalizado: Nenhum dado novo para inserir.")

    df_incremental = df_incremental.reset_index(drop=True)
    
    # Exportando dados para a camada trusted
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "cobranca"
    FOLDER_DESTINATION_TRUSTED = "bitrix_chamadas"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        df_incremental,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="append"
    )

    print("Processamento e escrita concluídos com sucesso.")