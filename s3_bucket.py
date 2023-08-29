import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ParamValidationError
import os
import os
import logging

class S3Bucket:

    def __init__(self, bucket_name=None):
        # Check if running in a local environment
        run_local = os.getenv('RUN_LOCAL')

        if run_local:
            logging.info("Running in a local environment.")
            
            # Get keys from environment variables
            env_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
            env_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
            
            # Check environment variables
            if not env_access_key_id or not env_secret_access_key:
                logging.error("AWS configuration missing in environment variables!")
                raise EnvironmentError("Missing AWS configuration in environment variables.")
            
            # Initialize S3 client with credentials
            self.client = boto3.client('s3', aws_access_key_id=env_access_key_id, aws_secret_access_key=env_secret_access_key)
        else:
            logging.info("Running in a cloud environment.")
            
            # Initialize S3 client using IAM role
            self.client = boto3.client('s3')
        
        # Use provided bucket name or fall back to environment variable
        self.bucket_name = bucket_name if bucket_name else os.environ.get('AWS_BUCKET_NAME')
        
        if not self.bucket_name:
            logging.error("AWS bucket name missing!")
            raise EnvironmentError("Missing AWS bucket name.")

    def download_from_s3(self, s3_path, local_path):
        try:
            self.client.download_file(self.bucket_name, s3_path, local_path)
            logging.info(f"Downloaded {s3_path} to {local_path}")
        except S3UploadFailedError as e:
            logging.error(f"Upload failed for {s3_path} to {local_path}: {e}")
        except ParamValidationError as e:
            logging.error(f"Parameter validation failed for {s3_path} to {local_path}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while downloading {s3_path} to {local_path}: {e}")

