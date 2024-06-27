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

def receita_to_raw():
    
    print('AAAAAAA COMEÇOU AAAAA')
    print(os.getcwd())
    print(os.listdir(os.getcwd()))
    print('ZZZZZZZ TERMINOU ZZZZZ')