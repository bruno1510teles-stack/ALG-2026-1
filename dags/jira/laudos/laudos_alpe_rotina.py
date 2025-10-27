from minio import Minio
from io import BytesIO, StringIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake, DeltaTable
import csv
from decimal import Decimal, ROUND_DOWN

def trusted_incremental(access_params=None, **kwargs):
    """
    Lê novos arquivos CSV do RAW (df_issues_novas),
    compara pela chave_unica com a base existente no Delta (df_laudo_historico),
    aplica todos os tratamentos da carga full nos registros novos,
    e insere apenas os registros ainda não existentes.
    """

    print("Iniciando carga incremental da base Trusted...")

    # 1 Conexão com MinIO RAW
    minio_raw = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
    )

    BUCKET_RAW = "laudo"
    PREFIX_RAW = "saida/esteira=alpe/"
    ALL_DATA = []

    print("📥 Lendo novos arquivos CSV do RAW (df_issues_novas)...")
    objects = minio_raw.list_objects(BUCKET_RAW, prefix=PREFIX_RAW, recursive=True)
    for obj in objects:
        file_key = obj.object_name
        if file_key.endswith(".csv"):
            try:
                response = minio_raw.get_object(BUCKET_RAW, file_key)
                csv_content = response.read().decode('utf-8')

                # Limpeza de quebras de linha internas
                input_file = StringIO(csv_content)
                output_file = StringIO()
                reader = csv.reader(input_file)
                writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                for row in reader:
                    cleaned_row = [field.replace('\n', ' ').replace('\r', ' ') for field in row]
                    writer.writerow(cleaned_row)

                csv_data_buffer = BytesIO(output_file.getvalue().encode('utf-8'))
                df = pd.read_csv(csv_data_buffer, sep=',', dtype=str, low_memory=False, header=0)
                df["source_file"] = file_key
                ALL_DATA.append(df)

            except Exception as e:
                print(f"⚠️ Erro ao ler {file_key}: {e}")

    if not ALL_DATA:
        print("⚠️ Nenhum novo arquivo CSV encontrado no RAW.")
        return

    df_issues_novas = pd.concat(ALL_DATA, ignore_index=True)
    print(f"✅ Arquivos novos lidos: {len(df_issues_novas)} linhas totais")

    # 2 Lê a base existente do Delta Lake
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    BUCKET_TRUSTED = "laudo-trusted"
    FOLDER_TRUSTED = "laudo"
    delta_path = f"s3a://{BUCKET_TRUSTED}/{FOLDER_TRUSTED}"

    df_laudo_historico = DeltaTable(delta_path, storage_options=storage_options).to_pandas()
    print(f"Base existente (df_laudo_historico): {len(df_laudo_historico)} linhas")

    # 3 Filtra apenas registros novos pela chave_unica
    if 'chave_unica' not in df_issues_novas.columns:
        raise ValueError("❌ Coluna 'chave_unica' não encontrada nos arquivos novos.")

    chaves_existentes = set(df_laudo_historico['chave_unica'].unique())
    df_incremental = df_issues_novas[~df_issues_novas['chave_unica'].isin(chaves_existentes)].copy()
    print(f"Registros novos identificados: {len(df_incremental)}")

    if df_incremental.empty:
        print("✅ Nenhum novo registro para inserir. Encerrando carga incremental.")
        return

    # Lista de novas chaves_unicas
    novas_chaves = df_incremental['chave_unica'].tolist()
    print(f"Novas chaves_unicas que serão inseridas ({len(novas_chaves)}):")
    print(novas_chaves)

    # 4 Aplicando todos os tratamentos da carga full
    print("Aplicando tratamentos da carga full nos registros novos...")

    linhas_antes = len(df_incremental)

    # Campo CNPJ
    if 'cnpj_ec' in df_incremental.columns:
        df_incremental.rename(columns={'cnpj_ec': 'cnpj'}, inplace=True)
    df_incremental['cnpj'] = df_incremental['cnpj'].astype(str).str.slice(0, 14).str.zfill(14)

    # Criação campo cnpj_raiz
    df_incremental['cnpj_raiz'] = df_incremental['cnpj'].str[:8].str.zfill(8)

    # Função para ajustar valores decimais
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None
        valor_str = str(valor).strip().replace(',', '.')
        if valor_str == '':
            return None
        try:
            return Decimal(valor_str).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        except Exception:
            return None

    colunas_decimais = ['valor_aprovado', 'restritivo_pj', 'restritivo_pf', 'total_restritivo']
    for col in colunas_decimais:
        if col in df_incremental.columns:
            df_incremental[col] = df_incremental[col].apply(ajustar_decimal)
            df_incremental[col] = df_incremental[col].astype(float).round(2)

    # Colunas inteiras com suporte a nulos
    colunas_int = ['score']
    for col in colunas_int:
        if col in df_incremental.columns:
            df_incremental[col] = (
                pd.to_numeric(df_incremental[col], errors='coerce')
                .round(0)
                .astype('Int64')
            )

    # Garantir tipo string
    colunas_string = [
        "data_hora", "chave_unica", "nome_filtro", "ticket_jira", "cnpj",
        "cnpj_raiz", "resolucao", "politica", "parecer", "ramificacao"
    ]
    df_incremental[colunas_string] = (
        df_incremental[colunas_string].astype(str)
        .replace(["nan", "NaT", "None"], "")
        .fillna("")
    )

    # Remove registros sem data_hora ou chave_unica
    linhas_antes_filtro = len(df_incremental)
    df_incremental = df_incremental[
        df_incremental['data_hora'].notna() & df_incremental['chave_unica'].notna()
    ]
    print(f"Removidas {linhas_antes_filtro - len(df_incremental)} linhas sem data_hora ou chave_unica")

    # Remove CNPJs inválidos
    linhas_antes_filtro = len(df_incremental)
    df_incremental = df_incremental[
        df_incremental['cnpj'].str.match(r'^\d{14}$', na=False)
    ]
    print(f"Removidas {linhas_antes_filtro - len(df_incremental)} linhas com CNPJ inválido")

    # Remove linhas totalmente nulas
    linhas_antes_filtro = len(df_incremental)
    df_incremental.dropna(how='all', inplace=True)
    print(f"Removidas {linhas_antes_filtro - len(df_incremental)} linhas totalmente nulas")

    # Verifica se sobrou algum registro válido
    if df_incremental.empty:
        print("⚠️ Nenhum registro válido após tratamento. Encerrando carga incremental.")
        return

    print(f"✅ Total de registros após tratamento: {len(df_incremental)}")

    # Reordenar colunas
    ordem_colunas = [
        'data_hora', 'chave_unica', 'nome_filtro', 'ticket_jira', 'cnpj', 'cnpj_raiz',
        'resolucao', 'valor_aprovado', 'politica', 'parecer', 'ramificacao', 'score',
        'restritivo_pj', 'restritivo_pf', 'total_restritivo'
    ]
    df_incremental = df_incremental[ordem_colunas].reset_index(drop=True)

    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_incremental["atualizado_em"] = now.strftime("%Y-%m-%d %X")
    df_incremental["year"], df_incremental["month"], df_incremental["day"] = now.year, now.month, now.day

    # 5 Grava no Delta Lake
    write_deltalake(
        delta_path,
        df_incremental,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="append"
    )
    print(f"✅ Carga incremental concluída com sucesso! {len(df_incremental)} novas linhas inseridas.")
