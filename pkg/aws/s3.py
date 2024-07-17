import logging
import re

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from internal.db import Db
from pkg.aws import Aws
from smart_open import open

db = Db().dbm


class S3(Aws):
    def __init__(self):
        super().__init__()
        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )
        self.session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def is_valid_bucket_name(self, bucket_name):
        pattern = r"^[a-z0-9.-]{3,63}$"
        if re.match(pattern, bucket_name) and not re.match(
            r"^\d+\.\d+\.\d+\.\d+$", bucket_name
        ):
            return True
        return False

    def check_bucket_exists(self, bucket_name, is_create=False):
        if not self.is_valid_bucket_name(bucket_name):
            raise ValueError(f"The specified bucket name '{bucket_name}' is not valid.")
        try:
            self.client.head_bucket(Bucket=bucket_name)
            # print(f"Bucket {bucket_name} exists")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                if is_create:
                    print(f"Bucket '{bucket_name}' does not exist. Creating it...")
                    self.client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": self.region},
                    )
                    print(f"Bucket '{bucket_name}' created successfully.")
                return False
            else:
                raise e

    def list_bucket_contents(self, bucket, prefix=None):
        try:
            kwargs = {"Bucket": bucket}
            if prefix:
                kwargs["Prefix"] = prefix
            response = self.client.list_objects_v2(**kwargs)
            if "Contents" in response:
                return [content["Key"] for content in response["Contents"]]
        except self.client.exceptions.NoSuchBucket:
            raise ValueError(f"Bucket {bucket} does not exist.")

    def read_file(self, bucket, key):
        s3_url = f"s3://{bucket}/{key}"
        with open(s3_url, transport_params={"client": self.client}) as fin:
            return fin.read()

    def steam_query(self, db_target, s3_url):
        with open(s3_url, transport_params={"client": self.client}) as fin:
            buffer = ""
            for line in fin:
                if db.conn is None:
                    db.init_connection(db_target)
                buffer += line
                if line.strip().endswith(";"):
                    if not (
                        "DROP DATABASE" in buffer.upper()
                        or "CREATE DATABASE" in buffer.upper()
                        or "\\connect" in buffer.lower()
                    ):
                        with db.conn.cursor() as cur:
                            cur.execute(buffer)
                            db.conn.commit()
                    buffer = ""

    def upload_file(self, file_name, key, bucket):
        try:
            self.client.upload_file(Filename=file_name, Bucket=bucket, Key=key)
        except ClientError as e:
            logging.error(e)
            return False
        return True
