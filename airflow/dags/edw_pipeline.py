from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from edw.cbr import extract_cbr, load_cbr
from edw.flights import build_core, load_flights
from edw.quality import audit_data, build_dm
from edw.runtime import initialize

COMMON = dict(
    start_date=pendulum.datetime(2026, 10, 15, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["edw", "stage-3"],
)


def pipeline_config(**context):
    incoming = context["dag_run"].conf or {}
    return {
        "batch_id": incoming.get("batch_id") or context["dag_run"].run_id,
        "cbr_start_date": incoming.get("cbr_start_date"),
        "cbr_end_date": incoming.get("cbr_end_date") or (
            context["data_interval_end"].in_timezone("Europe/Moscow").date() - timedelta(days=1)
        ).isoformat(),
    }


with DAG(dag_id="edw_staging", schedule=None, description="Full PostgreSQL snapshot and incremental CBR -> S3 -> Staging", **COMMON) as staging_dag:
    init = PythonOperator(task_id="initialize_edw", python_callable=initialize)
    flights = PythonOperator(task_id="load_postgres_full", python_callable=load_flights,
                             execution_timeout=timedelta(hours=2))
    extract = PythonOperator(task_id="extract_cbr_to_s3", python_callable=extract_cbr,
                             execution_timeout=timedelta(hours=1))
    rates = PythonOperator(task_id="load_cbr_to_staging", python_callable=load_cbr)
    init >> [flights, extract]
    extract >> rates

with DAG(dag_id="edw_core", schedule=None, description="Validate, quarantine, and atomically publish Core", **COMMON) as core_dag:
    core = PythonOperator(task_id="validate_and_publish_core", python_callable=build_core,
                          execution_timeout=timedelta(hours=2))

with DAG(dag_id="edw_dm", schedule=None, description="Publish four analytical marts in one transaction", **COMMON) as dm_dag:
    dm = PythonOperator(task_id="build_four_marts", python_callable=build_dm,
                        execution_timeout=timedelta(hours=2))

with DAG(dag_id="edw_data_quality", schedule="0 6 * * *", description="Daily DQ audit, persisted results and log notifications", **COMMON) as dq_dag:
    audit = PythonOperator(task_id="audit_published_data", python_callable=audit_data,
                           execution_timeout=timedelta(hours=2))

with DAG(dag_id="edw_pipeline", schedule="0 3 * * *", render_template_as_native_obj=True,
         description="Master: Staging -> Core -> DM -> Data Quality", **COMMON) as pipeline_dag:
    config = PythonOperator(task_id="prepare_batch", python_callable=pipeline_config)
    previous = config
    for layer in ("staging", "core", "dm", "data_quality"):
        trigger = TriggerDagRunOperator(
            task_id="run_" + layer,
            trigger_dag_id="edw_" + layer,
            trigger_run_id="pipeline__{{ run_id }}",
            conf="{{ ti.xcom_pull(task_ids='prepare_batch') }}",
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=10,
            execution_timeout=timedelta(hours=3),
        )
        previous >> trigger
        previous = trigger
