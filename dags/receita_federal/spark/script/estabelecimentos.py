from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, col, when
from minio import Minio
import os
from itertools import chain
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import argparse


def estabelecimentos_to_trusted(spark):
    # Função para gerar cliente MinIO
    def generate_client_minio():
        parsedurl = os.getenv('MINIO_RAW_ENDPOINT')
        return Minio(
            parsedurl,
            os.getenv('MINIO_RAW_ACCESS_KEY'),
            os.getenv('MINIO_RAW_SECRET_KEY')
        )

    # Função para recuperar o arquivo mais recente no bucket
    def get_newest_file_path(client, bucket, path): 
        objects = client.list_objects(bucket, f'{path}/')
        max_ano = max(obj.object_name for obj in objects)
        
        objects = client.list_objects(bucket, max_ano)
        max_particao = max(obj.object_name for obj in objects)
        
        return max_particao

    # Configuração das credenciais do MinIO
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_RAW_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_RAW_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_RAW_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "10000")
    hadoop_conf.set("fs.s3a.connection.timeout", "20000")
    hadoop_conf.set("hadoop.security.authentication", "simple")
    hadoop_conf.set("hadoop.security.authorization", "false")


    # Criando o cliente MinIO
    client = generate_client_minio()

    # Pastas MiniO
    BUCKET_SOURCE = "receita-federal"
    PATH = "estabelecimentos"
    
    print("Lendo Pasta mais recente...")
    file_path = get_newest_file_path(client, BUCKET_SOURCE, PATH)
    print("Pasta mais recente: {}".format(file_path))

    objects = client.list_objects("receita-federal", prefix=file_path)

    # Extract file names
    file_names = [obj.object_name for obj in objects]

    print("Arquivos a serem processados:")
    for file_name in file_names:
        print(file_name)
    

    # Inicializando o DataFrame vazio para armazenar todos os dados
    trusted_estabelecimentos = None

    # Lendo todos os arquivos encontrados no file_path
    for file_name in file_names:
        print(f"Lendo arquivo: {file_name}")
        df_temp = spark.read \
            .option("delimiter", ";") \
            .option("header", False) \
            .option("encoding", 'latin1') \
            .csv(f"s3a://{BUCKET_SOURCE}/{file_name}")
        
        # Concatenando os DataFrames
        if trusted_estabelecimentos is None:
            trusted_estabelecimentos = df_temp
        else:
            trusted_estabelecimentos = trusted_estabelecimentos.union(df_temp)

    print("Todos os arquivos foram lidos e combinados.")

    header_inicial = ['CNPJ BÁSICO',
        'CNPJ ORDEM',
        'CNPJ DV',
        'IDENTIFICADOR MATRIZ/FILIAL',
        'NOME FANTASIA',
        'SITUAÇÃO CADASTRAL',
        'DATA SITUAÇÃO CADASTRAL',
        'MOTIVO SITUAÇÃO CADASTRAL',
        'NOME DA CIDADE NO EXTERIOR', 
        'PAIS',
        'DATA DE INÍCIO ATIVIDADE',
        'CNAE FISCAL PRINCIPAL',
        'CNAE FISCAL SECUNDÁRIA',
        'TIPO DE LOGRADOURO',
        'LOGRADOURO',
        'NÚMERO',
        'COMPLEMENTO',
        'BAIRRO',
        'CEP',
        'UF',
        'MUNICÍPIO',
        'DDD 1',
        'TELEFONE 1',
        'DDD 2',
        'TELEFONE 2',
        'DDD DO FAX',
        'FAX',
        'CORREIO ELETRÔNICO',
        'SITUAÇÃO ESPECIAL',
        'DATA DA SITUAÇÃO ESPECIAL']
    
    trusted_estabelecimentos = trusted_estabelecimentos \
        .withColumnRenamed("_c0", header_inicial[0]) \
        .withColumnRenamed("_c1", header_inicial[1]) \
        .withColumnRenamed("_c2", header_inicial[2]) \
        .withColumnRenamed("_c3", header_inicial[3]) \
        .withColumnRenamed("_c4", header_inicial[4]) \
        .withColumnRenamed("_c5", header_inicial[5]) \
        .withColumnRenamed("_c6", header_inicial[6]) \
        .withColumnRenamed("_c7", header_inicial[7]) \
        .withColumnRenamed("_c8", header_inicial[8]) \
        .withColumnRenamed("_c9", header_inicial[9]) \
        .withColumnRenamed("_c10", header_inicial[10]) \
        .withColumnRenamed("_c11", header_inicial[11]) \
        .withColumnRenamed("_c12", header_inicial[12]) \
        .withColumnRenamed("_c13", header_inicial[13]) \
        .withColumnRenamed("_c14", header_inicial[14]) \
        .withColumnRenamed("_c15", header_inicial[15]) \
        .withColumnRenamed("_c16", header_inicial[16]) \
        .withColumnRenamed("_c17", header_inicial[17]) \
        .withColumnRenamed("_c18", header_inicial[18]) \
        .withColumnRenamed("_c19", header_inicial[19]) \
        .withColumnRenamed("_c20", header_inicial[20]) \
        .withColumnRenamed("_c21", header_inicial[21]) \
        .withColumnRenamed("_c22", header_inicial[22]) \
        .withColumnRenamed("_c23", header_inicial[23]) \
        .withColumnRenamed("_c24", header_inicial[24]) \
        .withColumnRenamed("_c25", header_inicial[25]) \
        .withColumnRenamed("_c26", header_inicial[26]) \
        .withColumnRenamed("_c27", header_inicial[27]) \
        .withColumnRenamed("_c28", header_inicial[28]) \
        .withColumnRenamed("_c29", header_inicial[29]) 
        
        
    # Concatenar e formatar o documento (CNPJ)
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('documento_sem_formatacao', 
                    concat(col('CNPJ BÁSICO'), col('CNPJ ORDEM'), col('CNPJ DV')))

    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('documento_formatado', 
                    concat(col('documento_sem_formatacao').substr(1, 2), lit('.'),
                            col('documento_sem_formatacao').substr(3, 3), lit('.'),
                            col('documento_sem_formatacao').substr(6, 3), lit('/'),
                            col('documento_sem_formatacao').substr(9, 4), lit('-'),
                            col('documento_sem_formatacao').substr(13, 2)))

    # Criar a coluna de logradouro completa
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('logradouro', 
                    concat(col('TIPO DE LOGRADOURO'), lit(' '), col('LOGRADOURO')))

    # Criar as colunas de telefone formatadas
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('telefone_1', 
                    concat(lit('('), col('DDD 1'), lit(')'), col('TELEFONE 1')))

    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('telefone_2', 
                    concat(lit('('), col('DDD 2'), lit(')'), col('TELEFONE 2')))

    # Converter coluna categorica em dummy
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('IDENTIFICADOR MATRIZ/FILIAL', 
                    when(col('IDENTIFICADOR MATRIZ/FILIAL') == "1", True).otherwise(False))
    
    # Drop das colunas indesejadas
    colunas_para_dropar = ['CNPJ ORDEM', 'CNPJ DV', 'TIPO DE LOGRADOURO', 
                        'DDD 1', 'TELEFONE 1', 'DDD 2', 'TELEFONE 2', 'DDD DO FAX', 'FAX']

    trusted_estabelecimentos = trusted_estabelecimentos.drop(*colunas_para_dropar)
    
    header_final = {'IDENTIFICADOR MATRIZ/FILIAL': 'is_matriz',
        'CNPJ BÁSICO': 'cnpj_raiz', 
        'NOME FANTASIA': 'nome_fantasia',
        'SITUAÇÃO CADASTRAL': 'codigo_situacao_cadastral',
        'DATA SITUAÇÃO CADASTRAL': 'data_situacao_cadastral',
        'MOTIVO SITUAÇÃO CADASTRAL': 'motivo_situacao_cadastral',
        'NOME DA CIDADE NO EXTERIOR': 'nome_cidade_exterior', 
        'PAIS': 'pais',
        'DATA DE INÍCIO ATIVIDADE': 'data_inicio_atividade',
        'CNAE FISCAL PRINCIPAL': 'cnae_principal',
        'CNAE FISCAL SECUNDÁRIA': 'cnae_secundaria',
        'NÚMERO': 'numero',
        'COMPLEMENTO': 'complemento',
        'BAIRRO': 'bairro',
        'CEP': 'cep',
        'UF': 'uf',
        'MUNICÍPIO': 'municipio',
        'CORREIO ELETRÔNICO': 'email',
        'SITUAÇÃO ESPECIAL': 'situacao_especial',
        'DATA DA SITUAÇÃO ESPECIAL': 'data_sitaucao_especial'}
    
    trusted_estabelecimentos = trusted_estabelecimentos \
        .withColumnRenamed('IDENTIFICADOR MATRIZ/FILIAL', header_final['IDENTIFICADOR MATRIZ/FILIAL']) \
        .withColumnRenamed('CNPJ BÁSICO', header_final['CNPJ BÁSICO']) \
        .withColumnRenamed('NOME FANTASIA', header_final['NOME FANTASIA']) \
        .withColumnRenamed('SITUAÇÃO CADASTRAL', header_final['SITUAÇÃO CADASTRAL']) \
        .withColumnRenamed('DATA SITUAÇÃO CADASTRAL', header_final['DATA SITUAÇÃO CADASTRAL']) \
        .withColumnRenamed('MOTIVO SITUAÇÃO CADASTRAL', header_final['MOTIVO SITUAÇÃO CADASTRAL']) \
        .withColumnRenamed('NOME DA CIDADE NO EXTERIOR', header_final['NOME DA CIDADE NO EXTERIOR']) \
        .withColumnRenamed('PAIS', header_final['PAIS']) \
        .withColumnRenamed('DATA DE INÍCIO ATIVIDADE', header_final['DATA DE INÍCIO ATIVIDADE']) \
        .withColumnRenamed('CNAE FISCAL PRINCIPAL', header_final['CNAE FISCAL PRINCIPAL']) \
        .withColumnRenamed('CNAE FISCAL SECUNDÁRIA', header_final['CNAE FISCAL SECUNDÁRIA']) \
        .withColumnRenamed('NÚMERO', header_final['NÚMERO']) \
        .withColumnRenamed('COMPLEMENTO', header_final['COMPLEMENTO']) \
        .withColumnRenamed('BAIRRO', header_final['BAIRRO']) \
        .withColumnRenamed('CEP', header_final['CEP']) \
        .withColumnRenamed('UF', header_final['UF']) \
        .withColumnRenamed('MUNICÍPIO', header_final['MUNICÍPIO']) \
        .withColumnRenamed('CORREIO ELETRÔNICO', header_final['CORREIO ELETRÔNICO']) \
        .withColumnRenamed('SITUAÇÃO ESPECIAL', header_final['SITUAÇÃO ESPECIAL']) \
        .withColumnRenamed('DATA DA SITUAÇÃO ESPECIAL', header_final['DATA DA SITUAÇÃO ESPECIAL'])
        
    # Dicionário de mapeamento
    situacao_cadastral_dict = {
        '01': 'NULA',
        '02': 'ATIVA',
        '03': 'SUSPENSA',
        '04': 'INAPTA',
        '08': 'BAIXADA'
    }

    # Cria um mapa (coluna de mapeamento)
    mapping_expr = F.create_map([F.lit(x) for x in chain(*situacao_cadastral_dict.items())])

    # Aplica o mapeamento e usa 'UNKNOWN' como valor padrão
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn(
        "situacao_cadastral",
        F.coalesce(mapping_expr[F.col("codigo_situacao_cadastral")], F.lit("UNKNOWN"))
    )
    
    # Formatar a coluna 'data_situacao_cadastral'
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('data_situacao_cadastral', 
                    concat(col('data_situacao_cadastral').substr(1, 4), lit('-'),
                            col('data_situacao_cadastral').substr(5, 2), lit('-'),
                            col('data_situacao_cadastral').substr(7, 2)))

    # Formatar a coluna 'data_inicio_atividade'
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('data_inicio_atividade', 
                    concat(col('data_inicio_atividade').substr(1, 4), lit('-'),
                            col('data_inicio_atividade').substr(5, 2), lit('-'),
                            col('data_inicio_atividade').substr(7, 2)))

    # Formatar a coluna 'data_sitaucao_especial'
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn('data_sitaucao_especial', 
                    concat(col('data_sitaucao_especial').substr(1, 4), lit('-'),
                            col('data_sitaucao_especial').substr(5, 2), lit('-'),
                            col('data_sitaucao_especial').substr(7, 2)))
    
    colunas_ordem = [
            'cnpj_raiz',
            'documento_sem_formatacao',
            'documento_formatado',
            'is_matriz',
            'nome_fantasia',
            'codigo_situacao_cadastral',
            'situacao_cadastral',
            'data_situacao_cadastral',
            'motivo_situacao_cadastral',
            'nome_cidade_exterior', 
            'pais',
            'data_inicio_atividade',
            'cnae_principal',
            'cnae_secundaria',
            'logradouro',
            'numero',
            'complemento',
            'bairro',
            'cep',
            'uf',
            'municipio',
            'telefone_1',
            'telefone_2',
            'email',
            'situacao_especial',
            'data_sitaucao_especial']

    # Reordenar as colunas usando select
    trusted_estabelecimentos = trusted_estabelecimentos.select(*colunas_ordem)
    
  # Pegando nome do arquivo para criar a coluna data_ref    
    file_name = file_names[0]
    
    data_ref = str(datetime.now().year)[0:3] + file_name[-14:-13] + file_name[-13:-11]
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn("data_ref", lit(data_ref))
    
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    trusted_estabelecimentos = trusted_estabelecimentos.withColumn("atualizado_em", lit(atualizado_em))

    print(f"DataFrame carregado. Esquema: {trusted_estabelecimentos.printSchema()}")
    print(f"Primeiras linhas do DataFrame: {trusted_estabelecimentos.show(5)}") 
    row_count = trusted_estabelecimentos.count()
    print(f"Número de linhas no DataFrame: {row_count}")
    
    # Reconfigurar a sessão Spark para escrita no segundo MinIO
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    # Escrevendo os dados com o schema definido
    trusted_estabelecimentos.write \
        .partitionBy("data_ref") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .save("s3a://teste-felipe/receita-federal/estabelecimentos")

    print("Arquivos Salvos")    
    # Fechar a sessão Spark
    spark.stop()

        
# Criando sessão Spark
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EstabelecimentosToTrusted") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    estabelecimentos_to_trusted(spark)