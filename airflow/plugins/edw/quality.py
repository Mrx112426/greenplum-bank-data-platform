import os
from datetime import date, timedelta
from pathlib import Path

from edw.flights import validation_select
from edw.model import MART_KEYS, TABLES
from edw.runtime import (LOG, batch_id, execute_layer, layer_batch,
                         publish_state, record_check, record_run, require_state,
                         reset_checks, transaction)


def build_dm(**context):
    return execute_layer(_build_dm, context, "dm")


def _build_dm(cur, batch, context):
    cur.execute("LOCK TABLE " + ",".join(f"core.{t.name}" for t in TABLES)
                + ",core.exchange_rates IN SHARE MODE")
    upstream = layer_batch(cur, context, "core")
    path = Path(os.environ.get("EDW_SQL_DIR", "/opt/airflow/project-sql")) / "edw" / "002_marts.sql"
    cur.execute(path.read_text(encoding="utf-8"), {"batch_id": batch})
    counts = {}
    for name in MART_KEYS:
        cur.execute(f"ANALYZE dm.{name}")
        cur.execute(f"SELECT count(*) FROM dm.{name}")
        counts[name] = cur.fetchone()[0]
    # A lost row in a supposedly one-to-one mart is a publication error, not source quarantine.
    cur.execute("SELECT count(*) FROM core.ticket_flights")
    if cur.fetchone()[0] != counts["ticket_flight_details"]:
        raise RuntimeError("DM detail row count differs from accepted Core ticket-flight rows")
    publish_state(cur, "dm", batch)
    return {"core_batch": upstream, "tables": counts}


def audit_data(**context):
    audit = batch_id(context)
    warnings = 0
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur.execute("LOCK TABLE dq.layer_state IN EXCLUSIVE MODE")
        batch = layer_batch(cur, context, "dm")
        core_batch = require_state(cur, "core")
        cur.execute("SELECT details FROM dq.pipeline_runs WHERE batch_id=%s AND layer='dm'", (batch,))
        published = cur.fetchone()
        if not published or published[0]["core_batch"] != core_batch:
            raise RuntimeError("DM is not built from the currently published Core batch; run edw_dm")
        reset_checks(cur, audit, "audit")

        def check(table, name, failed, details=None):
            nonlocal warnings
            warnings += int(failed > 0)
            record_check(cur, audit, batch, "audit", table, name, failed, "WARNING", details)

        cur.execute("SELECT details FROM dq.pipeline_runs WHERE batch_id=%s AND layer='core'", (core_batch,))
        core_run = cur.fetchone()[0]
        for table in TABLES:
            cur.execute("SELECT count(*) FROM (" + validation_select(table, "core", "core") + ") q WHERE dq_reason <> ''")
            check(table.name, "core_row_quality", cur.fetchone()[0])
            cur.execute(f"SELECT count(*) FROM core.{table.name}")
            accepted = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM dq.rejected_records WHERE batch_id=%s AND layer='core' AND table_name=%s",
                        (core_batch, table.name))
            rejected = cur.fetchone()[0]
            expected = core_run["tables"][table.name]["source"]
            check(table.name, "source_accepted_rejected_balance", abs(expected - accepted - rejected),
                  {"source": expected, "accepted": accepted, "rejected": rejected})
            check(table.name, "quarantine_warning", rejected)

        for name, keys in MART_KEYS.items():
            fields = ",".join(keys)
            cur.execute(f"SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM dm.{name} GROUP BY {fields} HAVING count(*)>1) d")
            check(name, "mart_unique_key", int(cur.fetchone()[0]))

        metrics = (
            ("ticket_flight_details", "row_count", "SELECT count(*) FROM core.ticket_flights", "SELECT count(*) FROM dm.ticket_flight_details"),
            ("ticket_flight_details", "amount_rub", "SELECT coalesce(sum(amount),0) FROM core.ticket_flights", "SELECT coalesce(sum(amount_rub),0) FROM dm.ticket_flight_details"),
            ("flight_airport_daily", "flight_count", "SELECT count(*) FROM core.flights", "SELECT coalesce(sum(flight_count),0) FROM dm.flight_airport_daily"),
            ("flight_airport_daily", "ticket_count", "SELECT count(*) FROM core.ticket_flights", "SELECT coalesce(sum(ticket_count),0) FROM dm.flight_airport_daily"),
            ("flight_airport_daily", "amount_rub", "SELECT coalesce(sum(amount),0) FROM core.ticket_flights", "SELECT coalesce(sum(ticket_amount_rub),0) FROM dm.flight_airport_daily"),
            ("booking_sales_daily", "booking_amount_rub", "SELECT coalesce(sum(total_amount),0) FROM core.bookings", "SELECT coalesce(sum(booking_amount_rub),0) FROM dm.booking_sales_daily"),
            ("booking_sales_daily", "booking_count", "SELECT count(*) FROM core.bookings", "SELECT coalesce(sum(booking_count),0) FROM dm.booking_sales_daily"),
            ("booking_sales_daily", "ticket_count", "SELECT count(*) FROM core.tickets", "SELECT coalesce(sum(ticket_count),0) FROM dm.booking_sales_daily"),
            ("booking_sales_daily", "ticket_flight_amount_rub", "SELECT coalesce(sum(amount),0) FROM core.ticket_flights", "SELECT coalesce(sum(ticket_flight_amount_rub),0) FROM dm.booking_sales_daily"),
            ("exchange_rates_daily", "row_count", "SELECT count(*) FROM core.exchange_rates", "SELECT count(*) FROM dm.exchange_rates_daily"),
        )
        for table, name, source_sql, target_sql in metrics:
            cur.execute(source_sql)
            source = cur.fetchone()[0]
            cur.execute(target_sql)
            target = cur.fetchone()[0]
            check(table, "reconcile_" + name, int(source != target), {"source": str(source), "target": str(target)})

        cur.execute("""
            SELECT count(*) FROM core.exchange_rates
            WHERE rate_date IS NULL OR rate_date>request_date OR nominal IS NULL OR nominal<=0
               OR rate_value IS NULL OR rate_value<=0 OR source_value IS NULL OR source_value<=0
               OR abs(rate_value-source_value/nullif(nominal,0))>0.000000000001
        """)
        check("exchange_rates", "rate_normalization", cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM (SELECT request_date FROM core.exchange_rates GROUP BY request_date HAVING count(DISTINCT currency_code)<>3) r")
        check("exchange_rates", "three_currencies_per_date", cur.fetchone()[0])
        cur.execute("""
            SELECT count(*) FROM (
                SELECT d.day FROM
                (SELECT generate_series(min(request_date)::timestamp, max(request_date)::timestamp, interval '1 day') AS day
                 FROM core.exchange_rates) d
                LEFT JOIN core.exchange_rates r ON r.request_date=d.day::date
                GROUP BY d.day HAVING count(DISTINCT r.currency_code)<>3
            ) dates
        """)
        check("exchange_rates", "calendar_completeness", cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM dq.rejected_records WHERE batch_id=%s AND layer='core' AND table_name='exchange_rates'",
                    (core_batch,))
        check("exchange_rates", "quarantine_warning", cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM dm.exchange_rates_daily WHERE abs(change_pct)>%s",
                    (int(os.environ.get("CBR_CHANGE_WARNING_PCT", "20")),))
        check("exchange_rates_daily", "large_rate_change", cur.fetchone()[0])
        cur.execute("SELECT max(request_date) FROM core.exchange_rates")
        newest = cur.fetchone()[0]
        conf = context["dag_run"].conf or {}
        expected = date.fromisoformat(conf["cbr_end_date"]) if conf.get("cbr_end_date") else (
            context["data_interval_end"].in_timezone("Europe/Moscow").date() - timedelta(days=1)
        )
        check("exchange_rates", "freshness", int(newest is None or newest < expected),
              {"expected": str(expected), "actual": str(newest)})
        details = {"warning_checks": warnings, "published_dm_batch": batch}
        record_run(cur, audit, "audit", "WARNING" if warnings else "SUCCESS", details)
    LOG.info("Daily DQ summary: %s", details)
    return details
