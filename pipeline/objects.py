from io import BytesIO
import json
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pandas as pd


class ObjectStore:
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
            region_name="us-east-1",
            verify=os.environ["PIPELINE_CA"],
            config=Config(s3={"addressing_style": "path"}, connect_timeout=8, read_timeout=30,
                          retries={"total_max_attempts": 1}),
        )

    def ensure_bucket(self):
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            try:
                self.client.create_bucket(Bucket=self.bucket)
            except ClientError as creation_error:
                if creation_error.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise

    def metadata(self, key: str) -> dict | None:
        try:
            return self.client.head_object(Bucket=self.bucket, Key=key)["Metadata"]
        except ClientError as error:
            if error.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def put(self, key: str, content: bytes, *, metadata=None, immutable=False):
        condition = {"IfNoneMatch": "*"} if immutable else {}
        try:
            self.client.put_object(
                Bucket=self.bucket, Key=key, Body=content, Metadata=metadata or {}, **condition
            )
        except ClientError as error:
            if not immutable or error.response["Error"]["Code"] not in {"412", "PreconditionFailed"}:
                raise

    def get(self, key: str) -> bytes:
        result = self.client.get_object(Bucket=self.bucket, Key=key)
        with result["Body"] as body:
            return body.read()

    def write_frame(self, key: str, frame: pd.DataFrame, **options):
        buffer = BytesIO()
        frame.to_parquet(buffer, engine="pyarrow")
        self.put(key, buffer.getvalue(), **options)

    def read_frame(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(BytesIO(self.get(key)), engine="pyarrow")

    def write_json(self, key: str, data: dict):
        self.put(key, json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def key(self, uri: str) -> str:
        prefix = f"s3://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("Ссылка на объект относится к другому бакету.")
        return uri[len(prefix):]
