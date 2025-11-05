"""Download and extract the e0_2_location_example Evaluation dataset from S3."""
from typing import Union
import botocore.session
from botocore import UNSIGNED
from botocore.config import Config
import tarfile
from pathlib import Path
import os

session = botocore.session.Session()


def download_e0_2_example(temp_dir: Union[str, Path]):
    """Download and extract the e0_2_location_example Evaluation dataset from S3."""
    if not Path(temp_dir).is_dir():
        os.makedirs(temp_dir, exist_ok=True)

    s3_client = session.create_client(
        's3',
        config=Config(signature_version=UNSIGNED)
    )

    bucket_name = "ciroh-rti-public-data"  # Replace with actual bucket
    key = "teehr-data-warehouse/v0_4_evaluations/e0_2_location_example.tar.gz"  # Replace with actual key
    local_path = Path(temp_dir, "e0_2_location_example.tar.gz")



    # Use get_object instead of download_file
    response = s3_client.get_object(Bucket=bucket_name, Key=key)

    # Write the response body to file
    with open(local_path, 'wb') as f:
        f.write(response['Body'].read())

    print(f"✅ Downloaded to {local_path}")

    print("Extracting archive...")
    with tarfile.open(local_path, 'r:gz') as tar:
        tar.extractall(path=temp_dir)
    print("✅ Extraction complete")

    os.remove(local_path)
    print(f"✅ Removed archive {local_path}")


if __name__ == "__main__":
    download_e0_2_example(temp_dir="/home/slamont/temp")