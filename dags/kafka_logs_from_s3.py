from __future__ import annotations

import os
import posixpath
import shutil
from datetime import datetime
from pathlib import PurePosixPath

import boto3
import paramiko
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable


def _ensure_remote_directory(sftp: paramiko.SFTPClient, directory: str) -> None:
    missing: list[str] = []
    current = directory
    while current not in ("", "/"):
        try:
            sftp.stat(current)
            break
        except OSError:
            missing.append(current)
            current = posixpath.dirname(current)

    for path in reversed(missing):
        sftp.mkdir(path)


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
    def transfer_new_objects_to_vast() -> list[str]:
        bucket = os.environ["KAFKA_LOGS_BUCKET"]
        prefix = os.environ.get(
            "KAFKA_LOGS_PREFIX",
            "storm-training/galaxy.storms/",
        )
        max_files = int(os.environ.get("KAFKA_LOGS_MAX_FILES_PER_RUN", "100"))
        vast_host = os.environ["VAST_SSH_HOST"]
        vast_port = int(os.environ["VAST_SSH_PORT"])
        vast_user = os.environ.get("VAST_SSH_USER", "root")
        vast_key_path = os.environ.get(
            "VAST_SSH_KEY_PATH",
            "/opt/airflow/keys/vast_ai",
        )
        vast_logs_path = os.environ.get(
            "VAST_LOGS_PATH",
            "/workspace/data/kafka-logs",
        )
        strict_host_key = (
            os.environ.get("VAST_SSH_STRICT_HOST_KEY", "false").lower() == "true"
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

        checkpoint_name = "galaxy_kafka_last_s3_key"
        last_key = Variable.get(checkpoint_name, default_var="")
        pending = [key for key in keys if key > last_key][:max_files]
        if not pending:
            raise AirflowSkipException("No new Kafka log objects in S3")

        ssh = paramiko.SSHClient()
        if strict_host_key:
            ssh.load_host_keys(
                os.environ.get(
                    "VAST_SSH_KNOWN_HOSTS_PATH",
                    "/opt/airflow/keys/known_hosts",
                )
            )
            ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        transferred: list[str] = []
        try:
            ssh.connect(
                hostname=vast_host,
                port=vast_port,
                username=vast_user,
                key_filename=vast_key_path,
                look_for_keys=False,
                allow_agent=False,
                timeout=30,
                banner_timeout=30,
                auth_timeout=30,
            )
            with ssh.open_sftp() as sftp:
                for key in pending:
                    relative = key.removeprefix(prefix).lstrip("/")
                    parts = [
                        part
                        for part in PurePosixPath(relative).parts
                        if part not in ("", ".", "..")
                    ]
                    if not parts:
                        continue

                    remote_path = posixpath.join(vast_logs_path, *parts)
                    _ensure_remote_directory(sftp, posixpath.dirname(remote_path))

                    try:
                        sftp.stat(remote_path)
                    except OSError:
                        temporary_path = f"{remote_path}.part"
                        try:
                            sftp.remove(temporary_path)
                        except OSError:
                            pass

                        response = s3.get_object(Bucket=bucket, Key=key)
                        body = response["Body"]
                        try:
                            with sftp.open(temporary_path, "wb") as remote_file:
                                remote_file.set_pipelined(True)
                                shutil.copyfileobj(body, remote_file, length=1024 * 1024)
                            sftp.rename(temporary_path, remote_path)
                            sftp.chmod(remote_path, 0o644)
                        finally:
                            body.close()

                    transferred.append(remote_path)
                    Variable.set(checkpoint_name, key)
        finally:
            ssh.close()

        return transferred

    transfer_new_objects_to_vast()


ingest_galaxy_kafka_logs()

