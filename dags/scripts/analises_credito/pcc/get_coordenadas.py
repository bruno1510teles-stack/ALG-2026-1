import json, requests
from airflow.models import Variable
from scripts.utils import execute_query

def get_coordenadas(contentId, connection):
    try:
        getEndereco = f"SELECT logradouro || ' - ' || bairro || ', ' || municipio || ' - ' || uf || ', ' || cep FROM deltalaketrusted.serasa.endereco where id = '{contentId}'"
        
        endereco = execute_query(conn=connection, query=getEndereco)

        if not endereco:
            print("Endereço não fornecido")
            return None
        
        google_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={endereco[0][0]}&key={Variable.get('GOOGLE_API_KEY')}"
            
        result = json.loads(requests.post(url=google_url).content)

        coordenadas = f"{result['results'][0]['geometry']['location']['lat']};{result['results'][0]['geometry']['location']['lng']}"

        return coordenadas
    except Exception as e:
        print(e)
        return None
    