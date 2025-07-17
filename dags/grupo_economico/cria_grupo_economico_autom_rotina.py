# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
import os
import numpy as np
import json
from confluent_kafka import Producer
from datetime import datetime, timedelta
from airflow.models import Variable


def cria_grupo_economico_automatico_rotina(access_params=None,  **kwargs):

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
    
    # Definindo parametro da query
    agora = datetime.now()

    # Ultimas 3 horas
    tres_horas_atras = agora - timedelta(hours=3)

    data_input = tres_horas_atras.strftime('%Y-%m-%d %H:%M:%S')

    print('Data considerada no select:')
    print(data_input)

    print('VARIAVEL SERVIDOR:')
    print(Variable.get("KAFKA_TRANSACIONAL_ENDPOINT"))

    print('VARIAVEL TÓPICO:')
    print(Variable.get('KAFKA_TOPIC_PARTIES_ECONOMIC_IN'))

    # Base de CNPJs
    query_cnpjs = f"""
                    select 
                        sub.raiz_cnpj as raiz_cnpj,
                        max(sub.pgid) as pgid
                    from (
                        select
                            distinct raiz_cnpj, pgid
                        from deltalaketrusted.jira.propostas
                        where TRY_CAST(data_criado AS TIMESTAMP) >= TIMESTAMP '{data_input}' ) as sub
                    group by sub.raiz_cnpj
                """

    df_cnpjs = execute_query(conn, query_cnpjs)

    print('Quantidade de Sacados:')
    print(len(df_cnpjs))
    
    # Garantindo raiz_cnpj 8
    df_cnpjs['raiz_cnpj'] = df_cnpjs['raiz_cnpj'].astype(str).str.zfill(8)
    df_cnpjs['pgid'] = df_cnpjs['pgid'].str.lower()


    for _, row_cnpj in df_cnpjs.iterrows():

        cnpj = row_cnpj['raiz_cnpj']
        pgid = row_cnpj['pgid']

        print(f'Analisando raiz_cnpj: {cnpj}')
        
        query1 = f"""
            SELECT 
                p_participante.legal_name AS razao_social,
                p_participante."name" AS nome_fantasia,
                pi_participantes.value AS raiz_cnpj,
                pi_participantes."type" AS tipo_identificacao_sacado,
                pr_participantes."role" AS role_sacado,
                prr_participantes."type" AS tipo_vinculo,
                pr_grupo."role" AS role_grupo,
                pi_grupo.value AS pgid,
                pi_grupo."type" AS tipo_identificacao_grupo
            FROM postgres.prts_schema_prd_default.party_identification pi_sacado
            INNER JOIN postgres.prts_schema_prd_default.party_role pr_sacado 
                ON pr_sacado.party_id = pi_sacado.party_id 
                AND pr_sacado."role" = 'EG_MEMBER'
            INNER JOIN postgres.prts_schema_prd_default.party_role_relationship prr 
                ON prr.to_party_role_id = pr_sacado.id 
                AND prr."type" = 'ECONOMIC_GROUP_TO_EG_MEMBER' 
                AND prr.date_thru IS NULL
            INNER JOIN postgres.prts_schema_prd_default.party_role pr_grupo 
                ON pr_grupo.id = prr.from_party_role_id 
                AND pr_grupo."role" = 'ECONOMIC_GROUP'
            INNER JOIN postgres.prts_schema_prd_default.party_identification pi_grupo 
                ON pi_grupo.party_id = pr_grupo.party_id 
                AND pi_grupo."type" = 'PGID'
            INNER JOIN postgres.prts_schema_prd_default.party p_sacado 
                ON p_sacado.id = pi_sacado.party_id
            LEFT JOIN postgres.prts_schema_prd_default.party_role_relationship prr_participantes 
                ON prr_participantes.from_party_role_id = pr_grupo.id 
                AND prr_participantes."type" = 'ECONOMIC_GROUP_TO_EG_MEMBER' 
                AND prr_participantes.date_thru IS NULL
            LEFT JOIN postgres.prts_schema_prd_default.party_role pr_participantes 
                ON pr_participantes.id = prr_participantes.to_party_role_id 
                AND pr_participantes."role" = 'EG_MEMBER'
            LEFT JOIN postgres.prts_schema_prd_default.party_identification pi_participantes 
                ON pi_participantes.party_id = pr_participantes.party_id 
                AND pi_participantes."type" = 'PGAS'
            LEFT JOIN postgres.prts_schema_prd_default.party p_participante 
                ON p_participante.id = pi_participantes.party_id 
            WHERE 
                pi_sacado."type" = 'PGAS'
                AND pi_sacado.value = '{cnpj}'
        """

        possui_grupo = execute_query(conn, query1)

        if possui_grupo.empty:

            query2 = f"""
            select  distinct
                    substring(A.document_number,1,8) as cnpj_base
                    , A.document_number as cnpj_completo
                    , A.company_name as nome
                    , SUBSTRING(C.participated_document_id,1,8) as cnpj_base_coligada
                    , C.participated_document_id as cnpj_completo_coligada
                    , C.participated_name as nome_coligada
                    , D.document_id as cpf_cnpj_socio
                    , D.participant_name as nome_socio
                    , D.participation_percentage_capital as part_social
                    , IF(C.restriction_sign, 1, 0) as anotacoes
            from postgres.exrp_prd_default.identification_report as A
            inner join postgres.exrp_prd_default.optional_features as B on A.ID = B.id 
            inner join postgres.exrp_prd_default.participated as C on B.company_participations_report_id = C.company_participations_report_id 
            inner join postgres.exrp_prd_default.participant as D on C.resume_id = D.participated_id
            inner join (select document_number, max(id) as max_id from postgres.exrp_prd_default.identification_report group by document_number) ir2 on ir2.max_id = A.id
            where substring(A.document_number,1,8) = '{cnpj}'
            and coalesce(D.participation_percentage_capital,0) >= 20  
            """

            cria_grupo = execute_query(conn, query2)

            if not cria_grupo.empty:

                # Definindo as coligadas
                df_json = cria_grupo[['cnpj_base_coligada', 'nome_coligada']].copy()

                df_json = df_json.drop_duplicates().reset_index(drop=True)

                # Dando um MAX no nome para garantir que todos terão o mesmo nome/identificação
                nome_grupo = cria_grupo["nome"].max().upper()

                # Normlaizando os dados
                df_json['cnpj_base_coligada'] = df_json['cnpj_base_coligada'].astype(str).str.zfill(8)
                df_json['nome_coligada'] = df_json['nome_coligada'].str.upper()

                # Inserindo valores do cnpj analisado
                cnpj_analisado = cria_grupo["cnpj_base"].iloc[0]
                
                df_json = pd.concat([
                    df_json,
                    pd.DataFrame([{
                        'cnpj_base_coligada': cnpj_analisado,
                        'nome_coligada': nome_grupo
                    }])
                ], ignore_index=True)

                # Criando nome do grupo e identificador do grupo
                df_json['nome'] = nome_grupo # Mesmo nome da empresa analisada
                df_json['identificacao_grupo_econ'] = df_json['nome'].str.replace(r'\s+', '_', regex=True) + '_ECON'

                json_list = []

                for _, row in df_json.iterrows():
                    json_payload = {
                        "partyRoleRelationship": {
                            "toPartyRole": {
                                "party": {
                                    "objectType": "PARTY_GROUP",
                                    "name": row['nome_coligada'],
                                    "identifications": [
                                        {
                                            "value": row['cnpj_base_coligada'],
                                            "type": "PGAS"
                                        }
                                    ]
                                },
                                "role": "EG_MEMBER"
                            },
                            "fromPartyRole": {
                                "party": {
                                    "objectType": "PARTY_GROUP",
                                    "name": row['nome'],
                                    "identifications": [
                                        {
                                            "value": row['identificacao_grupo_econ'],
                                            "type": "PGID"
                                        }
                                    ]
                                },
                                "role": "ECONOMIC_GROUP"
                            },
                            "type": "ECONOMIC_GROUP_TO_EG_MEMBER"
                        }
                    }
                
                    json_list.append((json_payload, row['cnpj_base_coligada']))


                # Configuração do Kafka
                kafka_config = {'bootstrap.servers': Variable.get("KAFKA_TRANSACIONAL_ENDPOINT")}

                # Tópico Kafka
                topic = Variable.get("KAFKA_TOPIC_PARTIES_ECONOMIC_IN")

                producer = Producer(kafka_config)

                def delivery_report(err, msg, cnpj):
                    if err is not None:
                        print(f"❌ Erro ao enviar mensagem para CNPJ {cnpj}: {err}")
                    else:
                        print(f"✅ Mensagem enviada para CNPJ {cnpj} → {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


                # Loop para enviar todos os JSONs
                for json_payload, cnpj_coligada in json_list:
                    try:
                        producer.produce(
                            topic=topic,
                            key=pgid.encode("utf-8"),
                            value=json.dumps(json_payload).encode("utf-8"),
                            callback=lambda err, msg, cnpj=cnpj_coligada: delivery_report(err, msg, cnpj)
                        )
                    except BufferError as e:
                        print(f"Buffer cheio, aguardando: {e}")
                        producer.poll(1)  # Aguarda espaço no buffer
                
                    # Processa callbacks pendentes
                    producer.poll(0)
                
                producer.flush()

                print('-' * 80)
                        

            else:
                print('Não possui grupo, porém não retorna dados na consulta de criação temporária.')
                pass
        
        else:
            print('Já possui grupo econômico.')
            pass

    # Fecha conexão
    conn.close()