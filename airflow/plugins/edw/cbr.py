import hashlib
import os
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from xml.etree import ElementTree

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from edw.runtime import (LOG, batch_id, execute_layer, publish_state,
                         record_check, reset_checks, transaction)

CURRENCIES = ("USD", "EUR", "CNY")


def decimal_value(value):
    try:
        result = Decimal((value or "").replace(" ", "").replace("\u00a0", "").replace(",", "."))
        return result if result.is_finite() else None
    except InvalidOperation:
        return None


def parse_rates(payload, request_date):
    root = ElementTree.fromstring(payload)
    if root.tag != "ValCurs":
        raise ValueError("CBR returned an unexpected XML document")
    effective_date = datetime.strptime(root.attrib["Date"], "%d.%m.%Y").date()
    rows = []
    for item in root.findall("Valute"):
        code = item.findtext("CharCode")
        if code not in CURRENCIES:
            continue
        nominal_value = decimal_value(item.findtext("Nominal"))
        nominal = int(nominal_value) if nominal_value is not None and nominal_value == nominal_value.to_integral_value() else None
        value = decimal_value(item.findtext("Value"))
        with localcontext() as ctx:
            ctx.prec = 40
            rate = (value / nominal).quantize(Decimal("0.000000000001")) if value is not None and nominal and nominal > 0 else None
        rows.append((request_date, effective_date, code, nominal, value, rate))
    if not rows:
        raise ValueError("CBR response has no configured currencies; existing data will be preserved")
    return rows


def extraction_dates(context, last_date):
    conf = context["dag_run"].conf or {}
    end = date.fromisoformat(conf["cbr_end_date"]) if conf.get("cbr_end_date") else (
        context["data_interval_end"].in_timezone("Europe/Moscow").date() - timedelta(days=1)
    )
    if end > datetime.now().date():
        raise ValueError("Future CBR request dates are not allowed")
    if conf.get("cbr_start_date"):
        start = date.fromisoformat(conf["cbr_start_date"])
    elif last_date is None:
        start = end - timedelta(days=int(os.environ.get("CBR_INITIAL_DAYS", "30")) - 1)
    else:
        lookback = int(os.environ.get("CBR_LOOKBACK_DAYS", "7"))
        start = min(last_date, end) - timedelta(days=lookback - 1)
    if start > end or (end - start).days >= 366:
        raise ValueError("CBR interval must contain between 1 and 366 days")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def extract_cbr(**context):
    batch = batch_id(context)
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT max(request_date) FROM staging.exchange_rates")
        dates = extraction_dates(context, cur.fetchone()[0])
    s3 = S3Hook(aws_conn_id="minio")
    client = s3.get_conn()
    bucket = os.environ["PLATFORM_MINIO_BUCKET"]
    manifest = []
    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=Retry(
            total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",)
        )))
        for day in dates:
            key = f"raw/cbr/request_date={day.isoformat()}/batch_id={batch}/response.xml"
            if s3.check_for_key(key, bucket_name=bucket):
                payload = client.get_object(Bucket=bucket, Key=key)["Body"].read()
            else:
                response = session.get("https://www.cbr.ru/scripts/XML_daily.asp",
                                       params={"date_req": day.strftime("%d/%m/%Y")}, timeout=(10, 60))
                response.raise_for_status()
                payload = response.content
                client.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="application/xml",
                                  Metadata={"request-date": day.isoformat(), "sha256": hashlib.sha256(payload).hexdigest()})
            parse_rates(payload, day)
            manifest.append({"request_date": day.isoformat(), "key": key,
                             "sha256": hashlib.sha256(payload).hexdigest()})
            LOG.info("CBR raw archived: s3://%s/%s", bucket, key)
    return manifest


def load_cbr(**context):
    return execute_layer(_load_cbr, context, "staging_cbr")


def _load_cbr(cur, batch, context):
    manifest = context["ti"].xcom_pull(task_ids="extract_cbr_to_s3")
    if not manifest:
        raise RuntimeError("CBR manifest is empty")
    client = S3Hook(aws_conn_id="minio").get_conn()
    bucket = os.environ["PLATFORM_MINIO_BUCKET"]
    total = 0
    missing_total = 0
    reset_checks(cur, batch, "staging_cbr")
    for item in manifest:
        day = date.fromisoformat(item["request_date"])
        payload = client.get_object(Bucket=bucket, Key=item["key"])["Body"].read()
        if hashlib.sha256(payload).hexdigest() != item["sha256"]:
            raise RuntimeError("CBR object checksum changed after extraction")
        rows = [row + (item["key"], item["sha256"], batch) for row in parse_rates(payload, day)]
        cur.execute("DELETE FROM staging.exchange_rates WHERE request_date=%s", (day,))
        execute_values(cur,
            "INSERT INTO staging.exchange_rates "
            "(request_date,rate_date,currency_code,nominal,source_value,rate_value,source_key,source_sha256,batch_id) VALUES %s", rows)
        missing = len(set(CURRENCIES) - {row[2] for row in rows})
        missing_total += missing
        record_check(cur, batch, batch, "staging_cbr", "exchange_rates", "expected_currencies", missing,
                     "WARNING", {"request_date": day.isoformat(), "received_rows": len(rows)})
        total += len(rows)
    publish_state(cur, "staging_cbr", batch)
    return {"request_dates": len(manifest), "source_rows": total, "warning_checks": missing_total}


def publish_rates(cur, batch, source_batch):
    cur.execute("""
        CREATE TEMP TABLE validated_rates ON COMMIT DROP AS
        SELECT s.*, concat_ws(';',
            CASE WHEN currency_code IS NULL OR currency_code NOT IN ('USD','EUR','CNY') THEN 'invalid_currency' END,
            CASE WHEN rate_date IS NULL OR rate_date > request_date THEN 'invalid_rate_date' END,
            CASE WHEN nominal IS NULL OR nominal <= 0 THEN 'invalid_nominal' END,
            CASE WHEN source_value IS NULL OR source_value <= 0 OR rate_value IS NULL OR rate_value <= 0
                THEN 'invalid_rate_value' END,
            CASE WHEN nominal > 0 AND abs(rate_value - source_value / nominal) > 0.000000000001
                THEN 'normalization_mismatch' END,
            CASE WHEN count(*) OVER (PARTITION BY request_date,currency_code) > 1 THEN 'duplicate_rate' END
        ) AS dq_reason
        FROM staging.exchange_rates s WHERE batch_id=%s
        DISTRIBUTED BY (currency_code)
    """, (source_batch,))
    cur.execute("SELECT count(*), count(*) FILTER (WHERE dq_reason <> '') FROM validated_rates")
    total, rejected = cur.fetchone()
    if total == 0:
        raise RuntimeError("No rates available for the requested staging batch")
    cur.execute("""
        INSERT INTO dq.rejected_records (batch_id,layer,table_name,reason,record_data)
        SELECT %s,'core','exchange_rates',dq_reason,to_jsonb(v) - 'dq_reason'
        FROM validated_rates v WHERE dq_reason <> ''
    """, (batch,))
    cur.execute("""
        DELETE FROM core.exchange_rates c USING
        (SELECT DISTINCT request_date FROM validated_rates) d WHERE c.request_date=d.request_date
    """)
    cur.execute("""
        INSERT INTO core.exchange_rates
        SELECT request_date,rate_date,currency_code,nominal,source_value,rate_value,
               source_key,source_sha256,%s,current_timestamp
        FROM validated_rates WHERE dq_reason = ''
    """, (batch,))
    record_check(cur, batch, batch, "core", "exchange_rates", "quarantined_rows", rejected,
                 "WARNING", {"source": total, "accepted": total - rejected, "rejected": rejected})
    cur.execute("DROP TABLE validated_rates")
    return {"source": total, "accepted": total - rejected, "rejected": rejected}
