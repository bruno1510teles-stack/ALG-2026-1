import requests
from datetime import datetime, timedelta

def enviar_mensagem_teams():

    data_execucao = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
    data_execucao_dt = datetime.strptime(data_execucao, "%d/%m/%Y")
    dia_semana = datetime.now().weekday()  # 0 = segunda, 6 = domingo
    year = data_execucao_dt.year
    month = f"{data_execucao_dt.month:02d}"
    day = f"{data_execucao_dt.day:02d}"

    webhook_url = "https://webhookbot.c-toss.com/api/bot/webhooks/b8eef0ac-7822-4a45-8b07-43fe6ed85991"

    payload_padrao = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "📄 **Base de propostas decididas pelo motor**",
                            "weight": "Bolder",
                            "size": "Large",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Prezados(as),",
                            "wrap": True,
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"O relatório de propostas decididas pelo motor do dia **{data_execucao}** já está disponível!!",
                            "wrap": True,
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": "Qualquer dúvida ou ajuste, estou à disposição!",
                            "wrap": True,
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"[🔗 Clique aqui para acessar o relatório](https://minio-datalake.alpe.com.br/raw/browser/proposta-motor/base_propostas_decididas_motor/year={year}/month={month}/day={day}/)",
                            "wrap": True,
                            "spacing": "Large"
                        }
                    ]
                }
            }
        ]
    }

    payload_seg_dom = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "**📄 Base de Propostas Decididas pelo Motor**",
                            "weight": "Bolder",
                            "size": "Large",
                            "wrap": True
                        },
                        {
                            "type": "TextBlock",
                            "text": "Prezados(as),",
                            "wrap": True,
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Não há propostas decididas pelo motor para o dia anterior.",
                            "wrap": True,
                            "spacing": "Medium"
                        },
                        {
                            "type": "TextBlock",
                            "text": "Qualquer dúvida ou ajuste, estou à disposição!",
                            "wrap": True,
                            "spacing": "Medium"
                        }
                    ]
                }
            }
        ]
    }

    payload = payload_seg_dom if dia_semana in [6, 0] else payload_padrao

    response = requests.post(webhook_url, json=payload)

    if response.status_code in [200, 204]:
        print("✅ Mensagem enviada com sucesso!")
    else:
        print(f"❌ Erro ao enviar mensagem. Código: {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    enviar_mensagem_teams()