# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
import os
import tempfile
import msoffcrypto
import re


def recompra_excel_to_csv_diario (access_params=None, **kwargs):

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
    BASE_FOLDER = "recompra"
    EXCEL_PASSWORD = "Alpe38"

    erro_count = 0
    arquivos_processados = []

    # Define a data de hoje (UTC)
    data_hoje = (datetime.now(timezone.utc) - timedelta(hours=3)).date()
    print(f"📅 Processando arquivos modificados em: {data_hoje}")

    # 🔍 Lista e processa arquivos do bucket origem
    objects = minio_raw.list_objects(BUCKET_SOURCE, prefix=BASE_FOLDER, recursive=True)

    for obj in objects:
        file_key = obj.object_name
        modified_date = (obj.last_modified - timedelta(hours=3)).date()

        if modified_date != data_hoje:
            print(f"⏭️ Ignorando {file_key} (modificado em: {modified_date})")
            continue

        print(f"📥 Processando arquivo novo: {file_key} (modificado em: {modified_date})")

        try:
            response = minio_raw.get_object(BUCKET_SOURCE, file_key)
            raw_data = BytesIO(response.read())
            decrypted_data = BytesIO()

            try:
                office_file = msoffcrypto.OfficeFile(raw_data)
                office_file.load_key(password=EXCEL_PASSWORD)
                office_file.decrypt(decrypted_data)
                decrypted_data.seek(0)
                df = pd.read_excel(decrypted_data)
            except Exception:
                raw_data.seek(0)
                df = pd.read_excel(raw_data)

            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            csv_key = file_key.replace(".xlsx", ".csv")

            minio_raw.put_object(
                BUCKET_DESTINATION,
                csv_key,
                csv_buffer,
                length=csv_buffer.getbuffer().nbytes,
                content_type='text/csv'
            )

            arquivos_processados.append(file_key)
            print(f"✅ CSV enviado: {csv_key}")

        except Exception as e:
            erro_count += 1
            print(f"⚠️ Erro ao processar {file_key}: {e}")

    # ✅ Resumo final
    print("\n✔️ Processo finalizado.")
    print(f"❌ Total de arquivos com erro: {erro_count}")
    print(f"📂 Arquivos processados com sucesso ({len(arquivos_processados)}):")
    for arq in arquivos_processados:
        print(f"   - {arq}")