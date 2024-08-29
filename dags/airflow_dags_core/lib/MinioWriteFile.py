from airflow.models import Variable
from minio import Minio
from urllib.parse import urlparse

class MinioWriteFile:

    def __init__(self):
        self.access_params = self.get_access_params()

    def get_access_params(self):
        return { 
            "endpoint_url_raw": Variable.get("MINIO_RAW_ENDPOINT"), 
            "aws_access_key_id_raw": Variable.get("MINIO_RAW_ACCESS_KEY"), 
            "aws_secret_access_key_raw": Variable.get("MINIO_RAW_SECRET_KEY"),      
        }
    
    def make_path_name_file(self, path, file_name):
        path = "" if path == None else "/{}".format(path)
        return "{}/{}".format(path, file_name)
        
    def extract_bucket_name(self, bucket):
        print(bucket.split('/')[0])
    
    def extract_path(self, bucket):
        print('/'.join(bucket.split('/')[1:]))

    def write_file(self, file, file_name, bucket, path=None):  
        print("init...")
        try:
            parsed_url = urlparse(f"https://{self.access_params['endpoint_url_raw']}")
            
            minio_client = Minio(
                parsed_url.netloc,
                access_key = self.access_params["aws_access_key_id_raw"],
                secret_key = self.access_params["aws_secret_access_key_raw"],
            )

            if '/' in bucket:
                bucket_name = self.extract_bucket_name(self, bucket=bucket)
                path = self.extract_path(self, bucket=bucket)
                bucket = bucket_name

            print("Uploading: {}".format(file_name))    

            path = self.make_path_name_file(path, file_name)

            if minio_client.bucket_exists(bucket):
                minio_client.put_object(bucket_name=bucket, object_name=path, data=file, length=file.getbuffer().nbytes)
            else :
                print("Bucket '{}' does not exist".format(bucket))

            print("{} uploaded!!".format(file_name))
        except Exception as e:
            print(f"ERRO: {e}")