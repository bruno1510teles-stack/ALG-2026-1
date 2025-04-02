from pyspark.sql import SparkSession
from minio import Minio
from concurrent.futures import ThreadPoolExecutor
import io 
import re
from datetime import datetime, timedelta
import pandas as pd
import os
import msoffcrypto

print("Bibliotecas importadas")

def hemera_raw_to_trusted(access_params=None, spark=None, **kwargs):
    if not spark:
        print("Erro: Instância do Spark não foi inicializada.")
        return

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_RAW_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_RAW_SECRET_KEY"))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv("MINIO_RAW_ENDPOINT"))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "10000")
    hadoop_conf.set("fs.s3a.connection.timeout", "20000")
    hadoop_conf.set("hadoop.security.authentication", "simple")
    hadoop_conf.set("hadoop.security.authorization", "false")

    print("MinIO Acessado e iniciando processo de leitura")

    client = Minio(
        os.getenv("MINIO_RAW_ENDPOINT"),
        access_key=os.getenv("MINIO_RAW_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_RAW_SECRET_KEY"),
        secure=True
    )

    try:
        client.list_buckets()
        print("Conexão com MinIO bem-sucedida!")
    except Exception as e:
        print(f"Erro ao conectar no MinIO: {e}")
        return

    # Buckets e subpastas
    SOURCE_BUCKET = "hemera"
    DEST_BUCKET = "hemera-csv"
    SUBFOLDERS = {
        "estoque": "estoque-csv",
        "retorno": "retorno-csv",
        "aquisicao": "aquisicao-csv",
        "recompra": "recompra-csv"
    }

    # Expressão regular para capturar a data no nome do arquivo
    DATE_PATTERN = re.compile(r"(\d{2}\.\d{2}\.\d{2})")

    def extract_date_from_filename(filename):
        """ Extrai e converte a data do formato DD.MM.YY para datetime. """
        match = DATE_PATTERN.search(filename)
        if match:
            return datetime.strptime(match.group(1), "%d.%m.%y")
        return None

    def process_subfolder(subfolder):
        print(f"Processando subpasta: {subfolder}")

        objects = client.list_objects(SOURCE_BUCKET, prefix=subfolder + "/", recursive=True)

        latest_obj = None
        latest_date = None

        for obj in objects:
            if obj.object_name.lower().endswith(".xlsx"):
                file_date = extract_date_from_filename(obj.object_name)  

                if file_date and (latest_date is None or file_date > latest_date):
                    latest_date = file_date
                    latest_obj = obj

        if latest_obj is None:
            print(f"Não foram encontrados arquivos XLSX na subpasta {subfolder}.")
            return

        # Verificar se o arquivo encontrado é de D-1
        yesterday = (datetime.now() - timedelta(days=1)).date()
        if latest_date.date() != yesterday:
            print(f"O arquivo {latest_obj.object_name} não é da data de ontem ({yesterday.strftime('%d/%m/%Y')}). Ignorando o processamento.")
            return

        print(f"Arquivo selecionado: {latest_obj.object_name} (Data no nome: {latest_date.strftime('%d/%m/%Y')})")

        # Baixar o arquivo do bucket
        response = client.get_object(SOURCE_BUCKET, latest_obj.object_name)
        file_data = response.read()
        response.close()
        response.release_conn()

        # Descriptografar o arquivo
        encrypted_file = io.BytesIO(file_data)
        decrypted_file = io.BytesIO()
        try:
            office_file = msoffcrypto.OfficeFile(encrypted_file)
            office_file.load_key(password="Alpe38")
            office_file.decrypt(decrypted_file)
        except Exception as e:
            print(f"Erro ao descriptografar {latest_obj.object_name}: {e}")
            return
        decrypted_file.seek(0)

        # Ler XLSX
        try:
            if subfolder == "retorno":
                df = pd.read_excel(decrypted_file, sheet_name=2)  # 3ª aba
            else:
                df = pd.read_excel(decrypted_file)  # Primeira aba padrão
        except Exception as e:
            print(f"Erro ao ler o XLSX {latest_obj.object_name}: {e}")
            return

        # Converter para CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        # Extrair caminho do arquivo original
        path_parts = latest_obj.object_name.split("/")

        # Construir o caminho no bucket de destino mantendo "year=YYYY/month=MM"
        year_month_path = "/".join([p for p in path_parts if p.startswith("year=") or p.startswith("month=")])

        # Mapear a subpasta para a estrutura correta no bucket de destino
        dest_subfolder = SUBFOLDERS[subfolder]
        dest_object_name = f"{dest_subfolder}/{year_month_path}/{latest_obj.object_name.rsplit('.', 1)[0]}.csv"

        # Fazer upload do CSV para o destino correto
        try:
            client.put_object(
                DEST_BUCKET,
                dest_object_name,
                io.BytesIO(csv_bytes),
                length=len(csv_bytes),
                content_type="text/csv"
            )
            print(f"Arquivo {dest_object_name} enviado para o bucket {DEST_BUCKET}.")
        except Exception as e:
            print(f"Erro ao enviar o CSV para o bucket {DEST_BUCKET}: {e}")

    # Processar cada subpasta
    for subfolder in SUBFOLDERS.keys():
        process_subfolder(subfolder)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("execel_to_csv") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()

    print("SparkSession criado com sucesso:", spark)
    hemera_raw_to_trusted(spark=spark)
