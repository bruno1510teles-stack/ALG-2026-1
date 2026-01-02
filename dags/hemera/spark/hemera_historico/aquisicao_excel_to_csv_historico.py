# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
import os
import tempfile
import msoffcrypto
import time

def aquisicao_excel_to_csv_historico (access_params=None, **kwargs):

    minio_raw = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
    )

    # Connection validation
    try:
        # Try to list the buckets
        buckets = minio_raw.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")
    

    # Configurações
    BUCKET_SOURCE = "hemera"
    BUCKET_DESTINATION = "hemera-csv"
    BASE_FOLDER = "aquisicao"
    YEARS = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
    MONTHS = [f"{m:02}" for m in range(1, 13)]
    EXCEL_PASSWORD = "Alpe38"

    erro_count = 0

    # Loop por anos e meses
    for year in YEARS:
        for month in MONTHS:
            folder_path = f"{BASE_FOLDER}/year={year}/month={month}/"
            print(f"🔍 Procurando arquivos em: {folder_path}")

            # Lista arquivos .xlsx na pasta
            objects = minio_raw.list_objects(BUCKET_SOURCE, prefix=folder_path, recursive=True)

            for obj in objects:
                file_key = obj.object_name
                if file_key.endswith(".xlsx"):
                    print(f"📥 Baixando arquivo: {file_key}")

                    try:
                        # Baixa o arquivo como bytes
                        response = minio_raw.get_object(BUCKET_SOURCE, file_key)
                        raw_data = BytesIO(response.read())

                        decrypted_data = BytesIO()

                        try:
                            # Tenta descriptografar
                            office_file = msoffcrypto.OfficeFile(raw_data)
                            office_file.load_key(password=EXCEL_PASSWORD)
                            office_file.decrypt(decrypted_data)
                            decrypted_data.seek(0)
                            df = pd.read_excel(decrypted_data)
                        except Exception as decrypt_error:
                            # Se não estiver criptografado, tenta ler direto
                            raw_data.seek(0)
                            df = pd.read_excel(raw_data)

                        # Converte para CSV
                        csv_buffer = BytesIO()
                        df.to_csv(csv_buffer, index=False)
                        csv_buffer.seek(0)

                        # Caminho do novo arquivo
                        csv_key = file_key.replace(".xlsx", ".csv")

                        # Upload para bucket destino
                        minio_raw.put_object(
                            BUCKET_DESTINATION,
                            csv_key,
                            csv_buffer,
                            length=csv_buffer.getbuffer().nbytes,
                            content_type='text/csv'
                        )

                        print(f"✅ CSV enviado: {csv_key}")

                    except Exception as e:
                        erro_count += 1
                        print(f"⚠️ Erro ao processar {file_key}: {e}")

    print("✔️ Processo finalizado.")
    print(f"❌ Total de arquivos com erro: {erro_count}")

