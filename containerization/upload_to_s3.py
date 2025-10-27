import os
import boto3
from botocore.client import Config

def upload_outputs_to_s3(bucket_name, endpoint_url, access_key, secret_key):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )

    base_output_dir = "outputs"
    uploaded = []

    print("[☁️] Uploading outputs to S3...")

    for root, _, files in os.walk(base_output_dir):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = file  # hanya nama file saja
            s3.upload_file(local_path, bucket_name, s3_key)
            print(f"[☁️] Uploaded {local_path} → s3://{bucket_name}/{s3_key}")
            uploaded.append(s3_key)

    print(f"[✔] Uploaded {len(uploaded)} files to S3. Now cleaning up local files...")
    return uploaded