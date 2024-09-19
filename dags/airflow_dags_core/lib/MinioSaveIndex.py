from scripts.utils import get_trino_connection, execute_query
import base64

from airflow_dags_core.lib.MinioWriteFile import MinioWriteFile

class MinioSaveIndex:

    def toBase64(self, file):
        file.seek(0)
        
        file_content = file.read()
        
        return base64.b64encode(file_content).decode('utf-8')

    def insert(self, base64file, issueKey, cnpj, path, type):
        try:
            connection = get_trino_connection()

            insert_query = f"""
                INSERT INTO minioraw.analise_credito_pcc."index"
                ("issue_key", "documento", "path", "arquivo", "tipo_arquivo")
                VALUES (\'{issueKey}\', \'{cnpj}\', \'{path}\', \'{base64file}\', \'{type}\')"""

            execute_query(conn= connection, query= insert_query)
        finally:
            connection.close()

    
    def save(self, key, identification, path, fileType, file = None):

        if file is not None:
            base64_encoded = self.toBase64(file)
        else:
            base64_encoded = None

        self.insert(base64file= base64_encoded, issueKey= key, cnpj= identification, path= path, type= fileType)