import tempfile

from airflow.providers.postgres.hooks.postgres import PostgresHook

from edw.model import TABLES
from edw.runtime import (LOG, execute_layer, layer_batch, publish_state,
                         record_check, reset_checks, table_columns)


def load_flights(**context):
    return execute_layer(_load_flights, context, "staging_flights")


def _load_flights(cur, batch, context):
    source = PostgresHook(postgres_conn_id="demo_postgres").get_conn()
    counts = {}
    try:
        source.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with source.cursor() as src:
            src.execute("SET TIME ZONE 'UTC'")
            # All eight COPY operations use the same source snapshot and GP transaction.
            cur.execute("TRUNCATE " + ",".join(f"staging.{t.name}" for t in TABLES))
            reset_checks(cur, batch, "staging_flights")
            for table in TABLES:
                columns = table_columns(table)
                src.execute(f"SELECT count(*) FROM bookings.{table.name}")
                expected = src.fetchone()[0]
                if expected == 0:
                    raise RuntimeError(f"Unexpected empty source bookings.{table.name}; publication cancelled")
                query = src.mogrify(
                    f"COPY (SELECT {columns}, %s::text FROM bookings.{table.name}) "
                    "TO STDOUT WITH (FORMAT CSV, NULL '\\N')", (batch,)
                ).decode()
                with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b") as stream:
                    src.copy_expert(query, stream)
                    stream.seek(0)
                    cur.copy_expert(
                        f"COPY staging.{table.name} ({columns},batch_id) "
                        "FROM STDIN WITH (FORMAT CSV, NULL '\\N')", stream
                    )
                cur.execute(f"SELECT count(*) FROM staging.{table.name}")
                actual = cur.fetchone()[0]
                record_check(cur, batch, batch, "staging_flights", table.name,
                             "source_row_count", abs(expected - actual),
                             details={"source": expected, "staging": actual})
                if expected != actual:
                    raise RuntimeError(f"COPY row count mismatch for {table.name}")
                cur.execute(f"ANALYZE staging.{table.name}")
                counts[table.name] = actual
                LOG.info("COPY bookings.%s -> staging.%s: %s rows", table.name, table.name, actual)
        source.rollback()
    finally:
        source.close()
    publish_state(cur, "staging_flights", batch)
    return {"tables": counts, "source_rows": sum(counts.values())}


def validation_select(table, schema="staging", parents="core"):
    clauses = []
    joins = []
    types = dict(table.columns)
    missing = [f's."{name}" IS NULL' + (
        f' OR btrim(s."{name}") = \'\'' if types[name] == "text" else ""
    ) for name in table.required]
    clauses.append(("missing_required", " OR ".join(missing)))
    for index, keys in enumerate((table.keys,) + table.other_unique):
        fields = ",".join(f'"{k}"' for k in keys)
        match = " AND ".join(f's."{k}" = d{index}."{k}"' for k in keys)
        joins.append(
            f"LEFT JOIN (SELECT {fields}, count(*) AS duplicates FROM {schema}.{table.name} "
            f"GROUP BY {fields} HAVING count(*) > 1) d{index} ON {match}"
        )
        clauses.append(("duplicate_" + "_".join(keys), f"d{index}.duplicates IS NOT NULL"))
    clauses.extend(table.rules)
    for index, (local, parent, remote) in enumerate(table.references):
        match = " AND ".join(f's."{a}"=r{index}."{b}"' for a, b in zip(local, remote))
        joins.append(f"LEFT JOIN {parents}.{parent} r{index} ON {match}")
        clauses.append(("missing_parent_" + "_".join(local), f'r{index}."{remote[0]}" IS NULL'))
    errors = ",".join(f"CASE WHEN ({condition}) THEN '{name}' END" for name, condition in clauses)
    return f"SELECT s.*, concat_ws(';', {errors}) AS dq_reason FROM {schema}.{table.name} s " + " ".join(joins)


def build_core(**context):
    return execute_layer(_build_core, context, "core")


def _build_core(cur, batch, context):
    from edw.cbr import publish_rates

    cur.execute("LOCK TABLE " + ",".join(f"staging.{t.name}" for t in TABLES)
                + ",staging.exchange_rates IN SHARE MODE")
    flight_batch = layer_batch(cur, context, "staging_flights")
    rate_batch = layer_batch(cur, context, "staging_cbr")
    reset_checks(cur, batch, "core")
    cur.execute("DELETE FROM dq.rejected_records WHERE batch_id=%s AND layer='core'", (batch,))
    # TRUNCATE, filtering, quarantine, and state publication commit as one layer.
    cur.execute("TRUNCATE " + ",".join(f"core.{t.name}" for t in TABLES))
    totals = {}
    rejected_total = 0
    for table in TABLES:
        cur.execute(
            "CREATE TEMP TABLE validated_rows ON COMMIT DROP AS " + validation_select(table)
            + " DISTRIBUTED " + table.distribution
        )
        cur.execute("SELECT count(*), count(*) FILTER (WHERE dq_reason <> '') FROM validated_rows")
        total, rejected = cur.fetchone()
        columns = table_columns(table)
        cur.execute(
            "INSERT INTO dq.rejected_records (batch_id,layer,table_name,reason,record_data) "
            "SELECT %s,'core',%s,dq_reason,to_jsonb(v) - 'dq_reason' "
            "FROM validated_rows v WHERE dq_reason <> ''", (batch, table.name)
        )
        cur.execute(
            f"INSERT INTO core.{table.name} ({columns},batch_id) "
            f"SELECT {columns},%s FROM validated_rows WHERE dq_reason = ''", (batch,)
        )
        cur.execute(f"SELECT count(*) FROM core.{table.name}")
        accepted = cur.fetchone()[0]
        if accepted + rejected != total:
            raise RuntimeError(f"Quarantine reconciliation failed for {table.name}")
        record_check(cur, batch, batch, "core", table.name, "quarantined_rows", rejected,
                     "WARNING", {"source": total, "accepted": accepted, "rejected": rejected})
        cur.execute("DROP TABLE validated_rows")
        cur.execute(f"ANALYZE core.{table.name}")
        totals[table.name] = {"source": total, "accepted": accepted, "rejected": rejected}
        rejected_total += rejected
    rate_summary = publish_rates(cur, batch, rate_batch)
    rejected_total += rate_summary["rejected"]
    publish_state(cur, "core", batch)
    return {"flight_batch": flight_batch, "rate_batch": rate_batch, "tables": totals,
            "exchange_rates": rate_summary, "rejected_rows": rejected_total}
