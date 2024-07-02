# Carregando libs
import pandas as pd
import pyarrow as pa
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable
import psutil
import requests

# Criando conexão

def download_and_extract_zip(url_path, extract_to, file_name):
    
    # Step 0: Creates path if dont exist
    os.makedirs(extract_to, exist_ok=True)
    
    # Step 1: Download the ZIP file
    
    zip_path = os.path.join(extract_to, file_name)
    url = os.path.join(url_path, file_name)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    # Step 2: Extract the contents of the ZIP file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    # Step 3: Cleanup - remove the downloaded ZIP file
    os.remove(zip_path)
    
class TqdmProgress:
    def __init__(self):
        self.progress_bar = None

    def set_meta(self, object_name, total_length):
        self.progress_bar = tqdm(total=total_length, unit='B', unit_scale=True, desc=object_name)

    def update(self, bytes_transferred):
        if self.progress_bar:
            self.progress_bar.update(bytes_transferred)

    def close(self):
        if self.progress_bar:
            self.progress_bar.close()

def upload_with_progress(client, bucket_name, object_name, file_path):
    progress = TqdmProgress()
    file_size = os.path.getsize(file_path)
    client.fput_object(bucket_name, object_name, file_path, progress=progress)
    progress.close()
    
pastas = ['cnaes',
          'empresas',
          'estabelecimentos',
          'motivos',
          'municipios',
          'naturezas',
          'paises',
          'qualificacoes',
          'simples',
          'socios']

fonte_receita = 'https://dadosabertos.rfb.gov.br/CNPJ/'

zip_arquivos = {
    'cnaes': ['Cnaes.zip'],
    'empresas': [
        'Empresas0.zip', 'Empresas1.zip', 'Empresas2.zip', 'Empresas3.zip', 
        'Empresas4.zip', 'Empresas5.zip', 'Empresas6.zip', 'Empresas7.zip', 
        'Empresas8.zip', 'Empresas9.zip'
    ],
    'estabelecimentos': [
        'Estabelecimentos0.zip', 'Estabelecimentos1.zip', 'Estabelecimentos2.zip', 
        'Estabelecimentos3.zip', 'Estabelecimentos4.zip', 'Estabelecimentos5.zip', 
        'Estabelecimentos6.zip', 'Estabelecimentos7.zip', 'Estabelecimentos8.zip', 
        'Estabelecimentos9.zip'
    ],
    'motivos': ['Motivos.zip'],
    'naturezas': ['Naturezas.zip'],
    'paises': ['Paises.zip'],
    'qualificacoes': ['Qualificacoes.zip'],
    'simples': ['Simples.zip'],
    'socios': [
        'Socios0.zip', 'Socios1.zip', 'Socios2.zip', 'Socios3.zip', 'Socios4.zip', 
        'Socios5.zip', 'Socios6.zip', 'Socios7.zip', 'Socios8.zip', 'Socios9.zip'
    ]
}

def simples_to_trusted():

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    TRUSTED_FOLDER = "simples/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
 
