from pkg.aws import Aws
import boto3
import botocore.exceptions
import re


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

    def check_bucket_exists(self, bucket_name):
        if not self.is_valid_bucket_name(bucket_name):
            raise ValueError(f"The specified bucket name '{bucket_name}' is not valid.")
        try:
            self.client.head_bucket(Bucket=bucket_name)
            # print(f"Bucket {bucket_name} exists")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"Bucket '{bucket_name}' does not exist. Creating it...")
                self.client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )
                print(f"Bucket '{bucket_name}' created successfully.")
                return False
            else:
                raise e
