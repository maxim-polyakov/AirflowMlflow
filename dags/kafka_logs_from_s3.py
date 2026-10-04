from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import boto3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable


@dag(
    dag_id="ingest_galaxy_kafka_logs",
    start_date=datetime(2024, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "s3", "galaxy-map"],
)
def ingest_galaxy_kafka_logs() -> None:
    @task
    def download_new_objects() -> list[str]:
        bucket = os.environ["KAFKA_LOGS_BUCKET"]
        prefix = os.environ.get(
            "KAFKA_LOGS_PREFIX",
            "storm-training/galaxy.storms/",
        )
        max_files = int(os.environ.get("KAFKA_LOGS_MAX_FILES_PER_RUN", "100"))
        target_root = Path("/opt/airflow/data/kafka-logs")
        target_root.mkdir(parents=True, exist_ok=True)

        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
            region_name=os.environ.get("S3_REGION") or None,
        )
        keys = sorted(
            item["Key"]
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket,
                Prefix=prefix,
            )
            for item in page.get("Contents", [])
            if not item["Key"].endswith("/")
        )

        checkpoint_name = "galaxy_kafka_last_s3_key"
        last_key = Variable.get(checkpoint_name, default_var="")
        pending = [key for key in keys if key > last_key][:max_files]
        if not pending:
            raise AirflowSkipException("No new Kafka log objects in S3")

        downloaded: list[str] = []
        for key in pending:
            relative_path = Path(key.removeprefix(prefix))
            target = target_root / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(target))
            downloaded.append(str(target))

        Variable.set(checkpoint_name, pending[-1])
        return downloaded

    download_new_objects()


ingest_galaxy_kafka_logs()

