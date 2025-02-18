import logging, base64, requests
from jira import JIRA
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd
from minio import Minio
from io import BytesIO

def aplicar_politica(access_params=None,  **kwargs):

    def conectar_banco():
        conn = connect(
            host=access_params['trino_endpoint'],
            port=access_params['trino_port'],
            user=access_params['trino_user'],
            auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
            http_scheme="https",
        )
        return conn
    
    def executar_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)

    ### Validando se a raiz do CNPJ foi analisada a menos de 60 DIAS
    # Configurações da API do Jira
    jira_url = f"{access_params['jira_url']}/rest/api/2/search"
    # Credenciais de acesso
    email = access_params['jira_api_user']
    api_token = access_params['jira_api_token']
    # Gerando o header de autenticação em Base64
    auth = base64.b64encode(f"{email}:{api_token}".encode()).decode()

    jira_url_transition = f"{access_params['jira_url']}"
    jira_connection = JIRA(basic_auth=(email, api_token), server=jira_url_transition)

    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }

    max_results = access_params['jira_max_result']  # Defina o número máximo de resultados por página (até 1000 conforme a configuração do Jira)
    start_at = 0                                    # Inicie na primeira página de resultados
    jira_project = access_params['jira_project']

    query = {
        "jql": f'project = {jira_project} AND status = "Open" ORDER BY created ASC',
        "fields": [
            "key",  # ISSUE_JIRA
            "customfield_13729", # CNPJ do Sacados
            "customfield_13739", # pgid do cedente 
            "customfield_13793"  # política
        ],
        "maxResults": max_results,
        "startAt": start_at
    }

    try:   
        response = requests.get(jira_url, headers=headers, params=query)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logging.error(f"HTTP Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"General Error: {err}")
    else:
        logging.info("Request was successful.")

    if response.status_code != 200:
        return
    tickets = response.json().get('issues', [])
    # Se não houver mais tickets, parar a paginação
    if not tickets:
        logging.info("Não encontrou propostas em aberto !!")
        return

    for ticket in tickets:
        # Acessando o assignee corretamente dentro de fields
        key_jira = ticket['key']
        cnpj_sacado = ticket['fields'].get('customfield_13729')
        pgid_cedente = ticket['fields'].get('customfield_13739')
        politica = ticket['fields'].get('customfield_13793', {}).get('value') if ticket['fields'].get('customfield_13793') else None

        print(f'A issue:{key_jira}, do CNPJ:{cnpj_sacado} está cadastrada com o pgid:{pgid_cedente} e política: {politica}')
        
        issue = jira_connection.issue(key_jira)

        # Query para regra politica_v3
        query_politica_v3 = f"""
        select 
            bi.cnpj_sacado, sum(valor_face) as valor_vencido, 
            coalesce(max(lim.limite_atribuido), 0) as limite_atribuido
        from 
            deltalaketrusted.payments.boletos_internos bi
        left join deltalaketrusted.limites.limite lim 
            on regexp_replace(bi.cnpj_sacado, '[.-]', '') = lim.cnpj_sacado
        where 
            status_titulo = 'VENCIDO' 
            and data_vencimento <= current_date - interval '30' day 
            and regexp_replace(bi.cnpj_sacado, '[.-]', '') = '{cnpj_sacado}'
        group by 
            bi.cnpj_sacado
        """

        # Query para regra politica_v4
        query_politica_v4 = f"""
        with 
        is_matriz as (
            select 
            documento_sem_formatacao, is_matriz 
            from deltalaketrusted.receita_federal.estabelecimentos 
            where cnpj_raiz = substring('{cnpj_sacado}', 1, 8) and is_matriz = true),
        sit_especial as (
            select 
                cnpj_raiz as cnpj_sacado_raiz,
                documento_sem_formatacao as cnpj_completo,
                razao_social as razao_social_sacado,
                coalesce(situacao_especial, 'ATIVA') as situacao_cadastral,
                tem_pep
            from 
                deltalakerefined.motor.pre_filtro
            where 
                cnpj_raiz = substring('{cnpj_sacado}', 1, 8)
                and (situacao_cadastral <> 'ATIVA' or situacao_especial = 'RECUPERACAO JUDICIAL' or tem_pep = true))
        select 
            *
        from 
            sit_especial se
            inner join is_matriz im on se.cnpj_completo = im.documento_sem_formatacao
        """


        def listar_arquivos_minio(bucket_name):
            # Função para listar os arquivos no bucket do MinIO
            client = Minio(
                        access_params['endpoint_url_raw'],
                        access_key= access_params['aws_access_key_id_raw'],
                        secret_key=access_params['aws_secret_access_key_raw'],
                        secure=True
                    )

            # Lista os objetos (arquivos) no bucket
            arquivos = []
            objects = client.list_objects(bucket_name, recursive=True)
            for obj in objects:
                arquivos.append(obj.object_name)
            
            return arquivos

        def verificar_cnpj_em_excel(bucket_name, arquivo, cnpj_sacado):
            # Função para verificar se o CNPJ está dentro de um arquivo Excel no bucket
            client = Minio(
                        access_params['endpoint_url_raw'],
                        access_key= access_params['aws_access_key_id_raw'],
                        secret_key=access_params['aws_secret_access_key_raw'],
                        secure=True
                    )
            
            # Baixar o arquivo Excel do MinIO
            response = client.get_object(bucket_name, arquivo)
            excel_data = response.read()
        
            # Ler o Excel usando Pandas
            df = pd.read_excel(BytesIO(excel_data), dtype=str)
          
            # Verificar se o CNPJ está no arquivo
            if 'CNPJ' not in df.columns:
                return False  # Se não existir, retorna False (não encontrou)
            
            cnpj_sacado = str(cnpj_sacado)

            # Verificar se o CNPJ está na coluna 'CNPJ'
            if cnpj_sacado in df['CNPJ'].values:
                return True
            
            return False

        # Função para atribuir política com base nas regras
        def atribuir_politica(cnpj_sacado=cnpj_sacado, politica=None, pgid=None):
            # Se a política já estiver preenchida com 'mesa', mantenha
            if politica == 'Mesa':
                return 'Mesa'
            
            # Conexão com o banco
            conn = conectar_banco()

            # Se não for 'mesa', verifica as regras subsequentes
            if politica != 'Mesa':
                # Regra politica_v3: executa a query e verifica o resultado
                resultado_v3 = executar_query(conn, query_politica_v3)
                if not resultado_v3.empty:
                    return 'V3'
                
                # Regra politica_v4: executa a query e verifica o resultado
                resultado_v4 = executar_query(conn, query_politica_v4)
                if not resultado_v4.empty:
                    return 'V4'

                # (V5): Verifica se o CNPJ está em algum arquivo Excel no bucket "pre-aprovado-lote"
                arquivos_excel = listar_arquivos_minio('pre-aprovado-lote')

                for arquivo in arquivos_excel:
                    if verificar_cnpj_em_excel('pre-aprovado-lote', arquivo, cnpj_sacado):
                        return 'V5'
                
                
                # (Desafiante): Verifica o 4º dígito do CNPJ
                # if len(cnpj_sacado) > 3:
                #     quarto_digito = cnpj_sacado[3]  # Pega o 4º dígito do CNPJ (índice 3)
                #     if quarto_digito in ['2', '3', '4']:
                #         return 'Desafiante'
                
                # Regra para fornecedores específicos
                if pgid in ['arcelor', 'belgo']:
                    return 'V2'
                elif pgid == 'cadubo':
                    return 'V1'
            
            # Se nenhuma regra for aplicada, retorna 'mesa' como padrão
            return 'Mesa'

        # Aplicando a função para atribuir política a este CNPJ
        politica_atualizada = atribuir_politica(cnpj_sacado, politica, pgid_cedente)

        print(f'A partir do conjunto de regras, a política definida para a issue {key_jira} foi:{politica_atualizada}')
        
        # Mapeamento das políticas para os respectivos IDs
        politica_para_id = {
            'Mesa': 12699,
            'V1': 12697,
            'V2': 12698,
            'V3': 12709,
            'V4': 12710,
            'V5': 12711,
            'Desafiante': 12712
        }

        # Função para atualizar a política no Jira
        def atualizar_politica_jira(issue, politica_atualizada):
            # Verifica se a política atualizada está no mapeamento
            if politica_atualizada in politica_para_id:
                # Obtém o ID da política atualizada
                id_politica = politica_para_id[politica_atualizada]

                # Atualiza a issue no Jira com o novo ID da política
                issue.update(fields={'customfield_13793': {'id': str(id_politica)}})

                print(f"Atualizando a issue {issue}. De Política:{politica} -- Para:{politica_atualizada}.")
                print("Atualizado com sucesso!!")
            else:
                print(f"Política {politica_atualizada} não encontrada no mapeamento.")

        # Exemplo de uso:
        atualizar_politica_jira(issue, politica_atualizada)
        print('---------------------------------------------------------------------------------------------------')

        # Armazenando resultados
        issues = []
        # Itera sobre cada ticket e captura o valor de 'key_jira'
        for ticket in tickets:
            key_jira = ticket['key']  
            issues.append(key_jira)

    # Fora do loop, cria um DataFrame com todas as 'key_jira' acumuladas
    dados = {
        'issue': issues  # Aqui vai a lista com todas as 'key_jira'
    }

    df_resultado = pd.DataFrame(dados)

    # Retorna o DataFrame convertido para dicionário
    return df_resultado.to_dict(orient='records')