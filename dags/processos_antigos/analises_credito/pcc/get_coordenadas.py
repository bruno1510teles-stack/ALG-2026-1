import json, requests
from airflow.models import Variable
from processos_antigos.scripts_diversos.utils import execute_query

def get_coordenadas(contentId, connection):
    try:
        getEndereco = f"""
        SELECT logradouro || ' - ' || bairro || ', ' || municipio || ' - ' || uf || ', ' || cep FROM deltalaketrusted.serasa.endereco where id = '{contentId}'
        union
        SELECT distinct
            a.address_line || ' - ' || a.district  || ', ' ||  a.city || ' - ' || a.state ||', ' || a.zip_code  endereco_completo
        FROM  postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
            inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = re.reports_id
            inner join postgres.exrp_{Variable.get('STAGE')}_default.report r on rs.id = r.reports_id
            left join postgres.exrp_{Variable.get('STAGE')}_default.identification_report ir on ir.id = r.identification_report_id
            left join postgres.exrp_{Variable.get('STAGE')}_default.address a on a.id = ir.address_id
        WHERE rc.json_content = '{contentId}' limit 1"""
        
        endereco = execute_query(conn=connection, query=getEndereco)

        print(endereco)
        if endereco[0][0]:
            print("Endereço não fornecido")
            return None
        
        google_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={endereco[0][0]}&key={Variable.get('GOOGLE_API_KEY')}"
            
        result = json.loads(requests.post(url=google_url).content)

        coordenadas = f"{result['results'][0]['geometry']['location']['lat']};{result['results'][0]['geometry']['location']['lng']}"

        return coordenadas
    except Exception as e:
        print(f"Erro ao gerar coordenadas: {e}")
        return None
    