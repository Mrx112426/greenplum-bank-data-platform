from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def check_postgres_source() -> None:
    hook = PostgresHook(postgres_conn_id="source_postgres")
    result = hook.get_first("SELECT current_database(), current_user, 1")
    if not result or result[2] != 1:
        raise RuntimeError("PostgreSQL source check failed")


def check_minio() -> None:
    bucket_name = os.environ["PLATFORM_MINIO_BUCKET"]
    hook = S3Hook(aws_conn_id="minio")
    if not hook.check_for_bucket(bucket_name):
        raise RuntimeError(f"MinIO bucket '{bucket_name}' was not found")


def check_greenplum() -> None:
    hook = PostgresHook(postgres_conn_id="greenplum_dwh")
    result = hook.get_first(
        """
        SELECT count(*)
        FROM gp_segment_configuration
        WHERE role = 'p'
          AND content >= 0
          AND status = 'u'
        """
    )
    if not result or result[0] < 1:
        raise RuntimeError("Greenplum primary segments are not available")


with DAG(
    dag_id="platform_healthcheck",
    description="Checks connectivity from Airflow to PostgreSQL, MinIO, and Greenplum",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["platform", "healthcheck"],
) as dag:
    postgres_check = PythonOperator(
        task_id="check_postgres_source",
        python_callable=check_postgres_source,
    )

    minio_check = PythonOperator(
        task_id="check_minio_bucket",
        python_callable=check_minio,
    )

    greenplum_check = PythonOperator(
        task_id="check_greenplum_dwh",
        python_callable=check_greenplum,
    )

    postgres_check >> minio_check >> greenplum_check
