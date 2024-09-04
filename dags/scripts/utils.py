from urllib.parse import urlparse

def extract_path_from_url(url):
    parsed_url = urlparse(url)
    extracted_path = parsed_url.path.replace("/raw/browser/", "")
    return extracted_path