from scripts.utils import get_trino_connection, execute_query
import base64

class MinioSaveIndex:

    def toBase64(self, file):
        file.seek(0)
        
        file_content = file.read()
        
        return base64.b64encode(file_content).decode('utf-8')

    def insert(self, base64file, issueKey, cnpj, path, type):
        try:
            connection = get_trino_connection()

            insert_query = f"""
                INSERT INTO minioraw.analise_credito_pcc."index"(
                    "issue_key", 
                    "documento", 
                    "path", 
                    "arquivo", 
                    "tipo_arquivo")
                VALUES (
                    {f"'{issueKey}'" if issueKey is not None else 'NULL'}, 
                    {f"'{cnpj}'" if cnpj is not None else 'NULL'}, 
                    {f"'{path}'" if path is not None else 'NULL'}, 
                    {f"'{base64file}'" if base64file is not None else 'NULL'}, 
                    {f"'{type}'" if type is not None else 'NULL'})
                """
            queryCheckIndex = f"""
                    SELECT 
                        1 
                    FROM 
                        minioraw.analise_credito_pcc."index" 
                    WHERE 
                        documento = '{cnpj}' AND issue_key = '{issueKey}'
                    """
            primeiraConsulta = execute_query(conn=connection, query=queryCheckIndex)
            if not primeiraConsulta:
                execute_query(conn= connection, query= insert_query)
        finally:
            connection.close()

    
    def save(self, key, identification, path, fileType, file = None):

        if file is not None:
            base64_encoded = self.toBase64(file)
        else:
            base64_encoded = None

        self.insert(base64file= base64_encoded, issueKey= key, cnpj= identification, path= path, type= fileType)