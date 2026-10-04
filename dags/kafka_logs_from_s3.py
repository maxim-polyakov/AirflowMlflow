from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import boto3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context


@dag(
    dag_id="prepare_galaxy_logs_for_jupyter",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "s3", "jupyter", "galaxy-map"],
)
def prepare_galaxy_logs_for_jupyter() -> None:
    @task
    def create_download_manifest() -> dict[str, object]:
        """List new Kafka log objects and return short-lived presigned URLs.

        Airflow holds S3 credentials. Jupyter only downloads via temporary URLs.
        """
        context = get_current_context()
        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        bucket = os.environ["KAFKA_LOGS_BUCKET"]
        prefix = os.environ.get(
            "KAFKA_LOGS_PREFIX",
            "storm-training/galaxy.storms/",
        )
        # Small training dumps (~MB), not big data — allow large one-shot manifests.
        max_files = min(
            int(conf.get("max_files", os.environ.get("KAFKA_LOGS_MAX_FILES_PER_RUN", "20000"))),
            50000,
        )
        last_key = str(conf.get("last_key", ""))
        url_ttl = int(
            os.environ.get("KAFKA_LOGS_PRESIGNED_URL_TTL_SECONDS", "3600")
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
            region_name=os.environ.get("S3_REGION") or None,
        )
        keys = sorted(
            item["Key"]
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket,
                Prefix=prefix,
            )
            for item in page.get("Contents", [])
            if not item["Key"].endswith("/")
        )

        pending = [key for key in keys if key > last_key][:max_files]
        if not pending:
            raise AirflowSkipException("No new Kafka log objects in S3")

        files = [
            {
                "key": key,
                "url": s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket, "Key": key},
                    ExpiresIn=url_ttl,
                ),
            }
            for key in pending
        ]
        return {
            "bucket": bucket,
            "prefix": prefix,
            "files": files,
            "keys": pending,
            "last_key": pending[-1],
            "file_count": len(files),
            "expires_at": (
                datetime.now(UTC) + timedelta(seconds=url_ttl)
            ).isoformat(),
            "prepared_at": datetime.now(UTC).isoformat(),
        }

    create_download_manifest()


prepare_galaxy_logs_for_jupyter()
