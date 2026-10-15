import hashlib
import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

from edw.model import create_flight_tables

LOG = logging.getLogger(__name__)


def gp_connection():
    override = os.environ.get("EDW_TEST_DATABASE")
    if override and not override.endswith("_test"):
        raise ValueError("Test database name must end with _test")
    hook = PostgresHook(postgres_conn_id="greenplum_dwh", database=override)
    conn = hook.get_conn()
    if override and conn.info.dbname != override:
        conn.close()
        raise RuntimeError("Refusing test writes: connected database differs from EDW_TEST_DATABASE")
    return conn


@contextmanager
def transaction():
    conn = gp_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute("SET statement_timeout = '30min'")
            cur.execute("SET lock_timeout = '5min'")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialize():
    sql_path = Path(os.environ.get("EDW_SQL_DIR", "/opt/airflow/project-sql")) / "edw" / "001_control.sql"
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(sql_path.read_text(encoding="utf-8"))
        create_flight_tables(cur)


def batch_id(context):
    conf = context["dag_run"].conf or {}
    value = conf.get("batch_id") or context["dag_run"].run_id
    return hashlib.sha256(str(value).encode()).hexdigest()[:32]


def get_state(cur, layer):
    cur.execute("SELECT batch_id FROM dq.layer_state WHERE layer = %s", (layer,))
    row = cur.fetchone()
    return row[0] if row else None


def require_state(cur, layer, expected=None):
    actual = get_state(cur, layer)
    if actual is None or (expected is not None and actual != expected):
        raise RuntimeError(f"Layer {layer}: expected batch {expected}, published batch {actual}")
    return actual


def publish_state(cur, layer, batch):
    cur.execute("DELETE FROM dq.layer_state WHERE layer = %s", (layer,))
    cur.execute("INSERT INTO dq.layer_state (layer,batch_id,published_at) VALUES (%s,%s,clock_timestamp())", (layer, batch))


def record_run(cur, batch, layer, status, details=None):
    cur.execute("DELETE FROM dq.pipeline_runs WHERE batch_id=%s AND layer=%s", (batch, layer))
    cur.execute(
        "INSERT INTO dq.pipeline_runs (batch_id,layer,status,finished_at,details) "
        "VALUES (%s,%s,%s,clock_timestamp(),%s)",
        (batch, layer, status, Json(details or {})),
    )


def record_check(cur, audit, batch, layer, table, name, failed, severity="ERROR", details=None):
    status = "PASS" if failed == 0 else ("WARN" if severity == "WARNING" else "FAIL")
    cur.execute(
        "INSERT INTO dq.check_results "
        "(audit_id,batch_id,layer,table_name,check_name,severity,status,failed_rows,details,checked_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp())",
        (audit, batch, layer, table, name, severity, status, failed, Json(details or {})),
    )
    level = logging.INFO if failed == 0 else logging.WARNING if severity == "WARNING" else logging.ERROR
    LOG.log(level, "DQ %s layer=%s table=%s check=%s failed_rows=%s details=%s",
            status, layer, table, name, failed, json.dumps(details or {}, ensure_ascii=False, default=str))


def reset_checks(cur, audit, layer):
    cur.execute("DELETE FROM dq.check_results WHERE audit_id=%s AND layer=%s", (audit, layer))


def execute_layer(fn, context, layer):
    batch = batch_id(context)
    try:
        with transaction() as conn, conn.cursor() as cur:
            # All layer jobs take this lock first, including manually triggered jobs.
            cur.execute("LOCK TABLE dq.layer_state IN EXCLUSIVE MODE")
            details = fn(cur, batch, context)
            status = "WARNING" if details.get("rejected_rows", 0) or details.get("warning_checks", 0) else "SUCCESS"
            record_run(cur, batch, layer, status, details)
        LOG.info("Published layer=%s batch=%s details=%s", layer, batch, details)
        return details
    except Exception as exc:
        LOG.exception("Layer %s failed; its publication was rolled back", layer)
        with transaction() as conn, conn.cursor() as cur:
            record_run(cur, batch, layer, "FAILED")
            cur.execute(
                "UPDATE dq.pipeline_runs SET error_message=%s WHERE batch_id=%s AND layer=%s",
                (str(exc)[:4000], batch, layer),
            )
        raise


def layer_batch(cur, context, upstream):
    if (context["dag_run"].conf or {}).get("batch_id"):
        return require_state(cur, upstream, batch_id(context))
    return require_state(cur, upstream)


def table_columns(table):
    return ",".join(f'"{c}"' for c in table.column_names)
