# Importando bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, lpad, substring, coalesce, col, to_date, when, trim, regexp_extract, input_file_name
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from minio import Minio
from io import BytesIO
from datetime import datetime, timezone, timedelta
import re
import os
import pandas as pd


def estoque_consolidado_trusted (access_params=None, **kwargs):

    spark.sparkContext.setLogLevel("ERROR")

    # Configurações do Hadoop para acesso ao MinIO (S3 compatível)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", 'B7q0avvSIpSdyGPXWnEC') 
    hadoop_conf.set("fs.s3a.secret.key", 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU') 
    hadoop_conf.set("fs.s3a.endpoint", 'api-raw.alpe.com.br') 
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # ESSENCIAL
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "10000")
    hadoop_conf.set("fs.s3a.connection.timeout", "20000")
    hadoop_conf.set("hadoop.security.authentication", "simple")
    hadoop_conf.set("hadoop.security.authorization", "false")


    minio_raw = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
        secure = True
    )


    # Expressão para extrair a data no formato 11.11.20
    regex = r"(\d{2}\.\d{2}\.\d{2})"

    bucket_name = "hemera-csv"
    caminho = 'estoque'

    anos = ['2025', '2024', '2023', '2022', '2021', '2020']

    meses = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    df_list = []

    for ano in anos:
        for mes in meses:
            caminho_csv = f"s3a://{bucket_name}/{caminho}/year={ano}/month={mes}/*.csv"

            try:
                df_ano = spark.read \
                    .option("delimiter", ",") \
                    .option("header", True) \
                    .option("allowMissingColumns", True) \
                    .csv(caminho_csv)

                # Adiciona a coluna data_arquivo extraída do nome do arquivo
                df_ano = df_ano.withColumn("data_arquivo", regexp_extract(input_file_name(), regex, 1))

                df_list.append(df_ano)
                print(f"Arquivos lidos para {ano}/{mes}")
            
            except Exception as e:
                print(f"Erro ao ler arquivos para {ano}/{mes}: {e}")

    if df_list:
        df = df_list[0]
        for df_atual in df_list[1:]:
            df = df.unionByName(df_atual, allowMissingColumns=True)
    else:
        df = None
        print("Nenhum arquivo foi encontrado para os anos e meses fornecidos.")

    print("Arquivos Lidos")


    print("Iniciando Tratamento Dados")


    # Tratando tipo_cedente
    if "CedenteTipoInscricao" in df.columns and "PES_TIPO_PESSOA" in df.columns:
        df = df.withColumn("tipo_cedente", coalesce(col("CedenteTipoInscricao"), col("PES_TIPO_PESSOA")))
    elif "CedenteTipoInscricao" in df.columns:
        df = df.withColumn("tipo_cedente", col("CedenteTipoInscricao"))
    elif "PES_TIPO_PESSOA" in df.columns:
        df = df.withColumn("tipo_cedente", col("PES_TIPO_PESSOA"))


    # Tratando nota_pdd
    if "NotaPDD" in df.columns and "NotaPdd" in df.columns:
        df = df.withColumn("nota_pdd", coalesce(col("NotaPDD"), col("NotaPdd")))
    elif "NotaPDD" in df.columns:
        df = df.withColumn("nota_pdd", col("NotaPDD"))
    elif "NotaPdd" in df.columns:
        df = df.withColumn("nota_pdd", col("NotaPdd"))


    # Tratando tipo_sacado
    if "SacadoTipoInscricao" in df.columns and "SAC_TIPO_PESSOA" in df.columns:
        df = df.withColumn("tipo_sacado", coalesce(col("SacadoTipoInscricao"), col("SAC_TIPO_PESSOA")))
    elif "SacadoTipoInscricao" in df.columns:
        df = df.withColumn("tipo_sacado", col("SacadoTipoInscricao"))
    elif "SAC_TIPO_PESSOA" in df.columns:
        df = df.withColumn("tipo_sacado", col("SAC_TIPO_PESSOA"))


    # Tratando valor_nominal
    if "ValorNominalOriginal" in df.columns and "ValorNominal" in df.columns:
        df = df.withColumn("valor_nominal", coalesce(col("ValorNominalOriginal"), col("ValorNominal")))
    elif "ValorNominalOriginal" in df.columns:
        df = df.withColumn("valor_nominal", col("ValorNominalOriginal"))
    elif "ValorNominal" in df.columns:
        df = df.withColumn("valor_nominal", col("ValorNominal"))


    # Tratando pdd_nota
    if "PDDNota" in df.columns and "PDD NOTA" in df.columns:
        df = df.withColumn("pdd_nota", coalesce(col("PDDNota"), col("PDD NOTA")))
    elif "PDDNota" in df.columns:
        df = df.withColumn("pdd_nota", col("PDDNota"))
    elif "PDD NOTA" in df.columns:
        df = df.withColumn("pdd_nota", col("PDD NOTA"))


    # Tratando pdd_vencido
    if "PDDVencido" in df.columns and "PDD REGULAMENTO" in df.columns:
        df = df.withColumn("pdd_vencido", coalesce(col("PDDVencido"), col("PDD REGULAMENTO")))
    elif "PDDVencido" in df.columns:
        df = df.withColumn("pdd_vencido", col("PDDVencido"))
    elif "PDD REGULAMENTO" in df.columns:
        df = df.withColumn("pdd_vencido", col("PDD REGULAMENTO"))


    # Renomeando colunas

    # Dicionário com renomeações
    renomeacoes = {
        "Situacao": "situacao",
        "CedenteCnpjCpf": "cnpj_cedente",
        "CedenteNome": "nome_cedente",
        "SacadoCnpjCpf": "cnpj_sacado",
        "SacadoNome": "nome_sacado",
        "IdTituloVx": "id_titulo",
        "TipoAtivo": "tipo_ativo",
        "DataEmissao": "data_emissao",
        "DataAquisicao": "data_aquisicao",
        "DataVencimento": "data_vencimento",
        "NumeroBoletoBanco": "numero_boleto_banco",
        "NumeroTitulo": "numero_titulo",
        "CampoChave": "campo_chave",
        "CMC7": "cmc7",
        "ValorAquisicao": "valor_aquisicao",
        "DataPosicao": "data_posicao",
        "DataProrrogacao": "data_prorrogacao",
        "DataOcorrenciaProrrogacao": "data_ocorrencia_prorrogacao",
        "Coobrigacao": "coobrigacao",
        "OriginadorCpfCnpj": "cpf_cnpj_originador",
        "EmpresaConveniadaCnpj": "cnpj_empresa_conveniada",
        "SubTipoAtivo": "sub_tipo_ativo",
        "Cnae": "cnae",
        "ValorPresenteMTM": "valor_presente_mtm",
        "IdRegistro": "id_registro",
        "ValorPresente": "valor_presente",
        "NomeRegistradora": "nome_registradora"
    }

    # Aplicar as renomeações
    for antiga, nova in renomeacoes.items():
        df = df.withColumnRenamed(antiga, nova)


    df = df.filter((col("data_posicao").isNotNull()) & (col("data_posicao") != ""))
    padrao_data = r'^\d{4}-\d{2}-\d{2}$'
    df = df.filter(F.col("data_posicao").rlike(padrao_data))

    # Adicionando coluna 'data_ref'
    df = df.withColumn("data_referencia", 
                    concat(substring(df['data_posicao'], 1, 4), lit('-'), 
                            lpad(substring(df['data_posicao'], 6, 2), 2, '0')))



    # Carregando Depara Grupo

    minio_raw = Minio(
    "api-raw.alpe.com.br",
    access_key = 'B7q0avvSIpSdyGPXWnEC',
    secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
    )


    try:
        buckets = minio_raw.list_buckets()

        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            print(f"Erro ao conectar ao MinIO: {e}")


    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)


    # Gerando nome do arquivo para importacao
    BUCKET_SOURCE_RAW = "arquivos-python"
    FOLDER_DESTINATION_RAW = 'depara_grupo_hemera'
    file_name = 'DEPARA_GRUPO.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Carregando Excel
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    df_grupo = pd.read_excel(file_data)


    # Convertendo Pandas para Spark
    df_grupo_spark = spark.createDataFrame(df_grupo)

    # Fazendo o left join
    df_join = df.join(
        df_grupo_spark,
        on="nome_cedente",
        how="left"
    )

    # Tratando nome_sacado para validação.
    df_join = df_join.withColumn("nome_sacado", trim(col("nome_sacado")))

    # Primeiro atualize grupo_temp com base em nome_sacado
    df_join = df_join.withColumn(
        "grupo_temp",
        when(trim(col("Grupo")) == "OLHAR SACADO", 
            when(col("nome_sacado") == "TRANSTECH TRANSPORTES E LOGISTICA S.A.", "Conglomerado")
            .when(col("nome_sacado") == "TRANSTECH TRANSPORTE E LOGÍSTICA", "Conglomerado")
            .when(col("nome_sacado") == "G&B AUTO PECAS ALTERNATIVAS LTDA - EM RECUPERACAO JUDICIAL", "VenderMais")
            .when(col("nome_sacado") == "G E B AUTO PECAS ALTERNATIVAS LTDA", "VenderMais")
            .when(col("nome_sacado") == "MIXTEL DISTRIBUIDORA LTDA.", "Tradicional")
            .when(col("nome_sacado") == "MIXTEL DISTRIBUIDORA LTDA", "Tradicional")
            .when(col("nome_sacado") == "VEX LOGISTICA E TRANSPORTES LTDA", "Conglomerado")
            .otherwise(col("Grupo"))
        ).otherwise(col("Grupo"))
    )

    # Depois atualize detalhe_temp com base em nome_sacado
    df_join = df_join.withColumn(
        "detalhe_temp",
        when(trim(col("Grupo")) == "OLHAR SACADO", 
            when(col("nome_sacado") == "TRANSTECH TRANSPORTES E LOGISTICA S.A.", "Conglomerado")
            .when(col("nome_sacado") == "TRANSTECH TRANSPORTE E LOGÍSTICA", "Conglomerado")
            .when(col("nome_sacado") == "G&B AUTO PECAS ALTERNATIVAS LTDA - EM RECUPERACAO JUDICIAL", "VenderMais")
            .when(col("nome_sacado") == "G E B AUTO PECAS ALTERNATIVAS LTDA", "VenderMais")
            .when(col("nome_sacado") == "MIXTEL DISTRIBUIDORA LTDA.", "Tradicional")
            .when(col("nome_sacado") == "MIXTEL DISTRIBUIDORA LTDA", "Tradicional")
            .when(col("nome_sacado") == "VEX LOGISTICA E TRANSPORTES LTDA", "Conglomerado")
            .otherwise(col("Detalhe"))
        ).otherwise(col("Detalhe"))
    )

    # Substituir e renomear
    df_join = df_join.withColumn("Grupo", coalesce(col("grupo_temp"), lit("Tradicional"))) \
        .withColumn("Detalhe", coalesce(col("detalhe_temp"), lit("Tradicional"))) \
        .drop("grupo_temp", "detalhe_temp")

    # Renomear para minúsculo
    df_join = df_join.withColumnRenamed("Grupo", "grupo").withColumnRenamed("Detalhe", "detalhe")


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df_join = df_join.withColumn("atualizado_em", lit(atualizado_em))

    # Formatando colunas

    df_join = df_join.withColumn("data_emissao", to_date(substring(col("data_emissao"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_aquisicao", to_date(substring(col("data_aquisicao"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_vencimento", to_date(substring(col("data_vencimento"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_prorrogacao", to_date(substring(col("data_prorrogacao"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_ocorrencia_prorrogacao", to_date(substring(col("data_ocorrencia_prorrogacao"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_arquivo", to_date(col("data_arquivo"), "dd.MM.yy"))

    # Formatar as colunas de valores numéricos como DoubleType
    df_join = df_join.withColumn("valor_aquisicao", col("valor_aquisicao").cast(DoubleType())) \
        .withColumn("valor_nominal", col("valor_nominal").cast(DoubleType())) \
        .withColumn("valor_presente", col("valor_presente").cast(DoubleType())) \
        .withColumn("pdd_nota", col("pdd_nota").cast(DoubleType())) \
        .withColumn("pdd_vencido", col("pdd_vencido").cast(DoubleType()))


    # Lista de colunas desejadas
    colunas_desejadas = ["cpf_cnpj_originador", "coobrigacao", "cnpj_cedente", "nome_cedente", "tipo_cedente", 
                        "cnpj_sacado", "nome_sacado", "tipo_sacado", "id_titulo", "numero_titulo", 
                        "numero_boleto_banco", "campo_chave", "tipo_ativo", "situacao", "data_emissao", 
                        "data_aquisicao", "data_vencimento", "data_prorrogacao", "data_ocorrencia_prorrogacao", 
                        "valor_aquisicao", "valor_nominal", "valor_presente", "pdd_nota", "pdd_vencido", 
                        "data_arquivo", "data_referencia", "grupo", "detalhe", "atualizado_em"]

    # Selecionar as colunas
    df_join = df_join.select(*colunas_desejadas)


    print("Tratamento Concluído")

    # Tirando configs da raw
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    # Colocando configs trusted
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    df_join.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://hemera-trusted/estoque")

    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("estoque_diario_hemera") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    estoque_consolidado_trusted(spark)
