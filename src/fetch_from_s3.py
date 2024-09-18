import boto3
import os


def fetch_csv_from_s3(bucket_name, file_key, local_file_path):
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    s3.download_file(bucket_name, file_key, local_file_path)
    print(f"Downloaded {file_key} from {bucket_name} to {local_file_path}")


if __name__ == "__main__":
    BUCKET_NAME = 'your-s3-bucket'
    FILE_KEY = 'path/to/your/file.csv'
    LOCAL_FILE_PATH = 'data/output.csv'

    fetch_csv_from_s3(BUCKET_NAME, FILE_KEY, LOCAL_FILE_PATH)
