from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import boto3
from airflow.decorators import dag, task


@dag(
    dag_id="download_dataset_from_s3",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["s3", "dataset"],
)
def download_dataset_from_s3() -> None:
    @task
    def download() -> str:
        bucket = os.environ["DATA_BUCKET"]
        object_key = os.environ["DATA_OBJECT_KEY"]
        target_dir = Path("/opt/airflow/data/input")
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / Path(object_key).name

        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
            region_name=os.environ.get("S3_REGION") or None,
        )
        client.download_file(bucket, object_key, str(target))
        return str(target)

    download()


download_dataset_from_s3()

