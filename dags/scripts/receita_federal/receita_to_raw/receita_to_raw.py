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
import zipfile
from tqdm import tqdm

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


def receita_to_raw():
    
    print('AAAAAAA COMEÇOU AAAAA')
    fonte_receita = 'https://dadosabertos.rfb.gov.br/CNPJ/'
    print(os.getcwd())
    print(os.listdir(os.getcwd()))
    
    download_and_extract_zip(fonte_receita, '.cnae', 'Cnaes.zip')
    
    
    print(os.getcwd())
    print(os.listdir(os.getcwd()))
    print('ZZZZZZZ TERMINOU ZZZZZ')