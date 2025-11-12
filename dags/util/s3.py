import boto3
import pandas as pd
from botocore.exceptions import ClientError
from io import BytesIO


class S3Client:
    def __init__(self, region_name='ap-southeast-2'):
        self.s3 = boto3.client('s3', region_name=region_name)

    def upload(self, local_file, bucket_name, s3_key):
        self.s3.upload_file(local_file, bucket_name, s3_key)
        print(f"Uploaded to s3://{bucket_name}/{s3_key}")

    def download(self, bucket_name, s3_key, local_file):
        self.s3.download_file(bucket_name, s3_key, local_file)
        print(f"Downloaded s3://{bucket_name}/{s3_key} to {local_file}")

    def exists(self, bucket_name, s3_key):
        try:
            self.s3.head_object(Bucket=bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise

    def read_csv(self, bucket_name, s3_key, **kwargs):
        try:
            obj = self.s3.get_object(Bucket=bucket_name, Key=s3_key)
            df = pd.read_csv(BytesIO(obj['Body'].read()), **kwargs)
            return df
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                print(f"File {s3_key} not found in bucket {bucket_name}")
                return None
            else:
                raise

    def read_csv_from_folder(self, bucket_name, prefix, **kwargs):
        try:
            resp = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            if "Contents" not in resp:
                return None

            csv_keys = [item["Key"] for item in resp["Contents"] if item["Key"].endswith(".csv")]

            if not csv_keys:
                return None

            for k in csv_keys:
                print("-", k)

            dfs = []
            for key in csv_keys:
                df = self.read_csv(bucket_name, key)
                if df is not None:
                    dfs.append(df)

            if not dfs:
                print("No dataframes to concatenate.")
                return None

            return pd.concat(dfs, ignore_index=True)
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                print(f"File {prefix} not found in bucket {bucket_name}")
                return None
            else:
                raise
