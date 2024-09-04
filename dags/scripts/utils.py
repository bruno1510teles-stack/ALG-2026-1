from urllib.parse import urlparse

def extract_path_from_url(url):
    url = "https://minio-datalake.alpe.com.br/raw/browser/analise-credito/47074079/2024-09-03-arcelor-CMGT-26864/arquivos/"
    parsed_url = urlparse(url)
    extracted_path = parsed_url.path.replace("/raw/browser/", "")
    return extracted_path