"""Stage 5: bounded, sequential Greenplum experiments. Never writes to dwh.

Executed in the existing Airflow container by run.sh. Pure metric helpers can
also be imported by host tests without installing Airflow or psycopg2.
"""
import argparse
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import statistics
import tempfile
import time
from datetime import datetime, timezone

DATABASE = 'sharding_stage5_test'
OWNER = 'greenplum-bank-stage5-v1'
FILTERS = {}
OUT = Path('/tmp/stage5-evidence')
SQL_DIR = Path(os.environ.get('EDW_SQL_DIR', '/opt/airflow/project-sql')) / 'sharding'
AO = 'WITH (appendoptimized=true,orientation=column,compresstype=zstd,compresslevel=1)'
# All projections exclude passenger names, contacts and other personal data.
TABLES = {
 'tickets': ('core.tickets', 'ticket_no,book_ref'),
 'bookings': ('core.bookings', 'book_ref,book_date,total_amount'),
 'ticket_flights': ('core.ticket_flights', 'ticket_no,flight_id,fare_conditions,amount'),
 'boarding_passes': ('core.boarding_passes', 'ticket_no,flight_id,boarding_no,seat_no'),
 'flights': ('core.flights', 'flight_id,scheduled_departure,departure_airport,arrival_airport,aircraft_code'),
 'airports': ('core.airports', 'airport_code,city'),
 'exchange_rates': ('core.exchange_rates', 'request_date,rate_date,currency_code,rate_value'),
 'flight_airport_daily': ('dm.flight_airport_daily', 'flight_date,departure_airport,arrival_airport,flight_count,ticket_amount_rub'),
 'booking_sales_daily': ('dm.booking_sales_daily', 'booking_date,booking_count,booking_amount_rub'),
}
DETAILS = "SELECT ticket_no,flight_id,fare_conditions,amount_rub AS amount,scheduled_departure::date AS flight_date FROM dm.ticket_flight_details"
DERIVED = "SELECT tf.ticket_no,tf.flight_id,tf.fare_conditions,tf.amount,f.scheduled_departure::date AS flight_date FROM stage5_base.ticket_flights tf JOIN stage5_base.flights f USING (flight_id)"
DETAIL_COLUMNS = 'ticket_no,flight_id,fare_conditions,amount,flight_date'
GROUPS = {
 'fact': ('tf','ticket_flights', [
  ('ticket','BY (ticket_no)'),('flight','BY (flight_id)'),
  ('composite','BY (ticket_no,flight_id)'),('random','RANDOMLY'),
  ('bad_fare','BY (fare_conditions)')],
  ['ticket_join','flight_join','boarding_join','ticket_filter','flight_filter']),
 'tickets': ('tickets','tickets', [('ticket','BY (ticket_no)'),('booking','BY (book_ref)'),('random','RANDOMLY')], ['ticket_join','booking_join']),
 'reference': ('airports','airports', [('replicated','REPLICATED'),('hash','BY (airport_code)'),('random','RANDOMLY')], ['airport_join']),
 'rates': ('rates','exchange_rates', [('currency','BY (currency_code)'),('composite','BY (request_date,currency_code)'),('random','RANDOMLY'),('replicated','REPLICATED')], ['rates_window']),
 'daily': ('daily','flight_airport_daily', [('airport','BY (departure_airport)'),('date','BY (flight_date)'),('composite','BY (flight_date,departure_airport,arrival_airport)'),('random','RANDOMLY')], ['daily_group']),
}


def emit(event, **values):
    print(json.dumps({'event': event, **values}, ensure_ascii=False, default=str), flush=True)


def save(name, value):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if name.startswith('plans/'):
        # Preserve every field and repetition without thousands of indent-only lines.
        path.write_text('[\n'+',\n'.join(json.dumps(plan,ensure_ascii=False,separators=(',',':'),default=str) for plan in value)+'\n]\n')
    else:
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n')


def skew_metrics(counts):
    if not counts:
        raise ValueError('Need one count for each active content')
    values = list(counts.values())
    mean = statistics.mean(values)
    return {'counts': counts, 'min': min(values), 'max': max(values), 'mean': mean,
            'max_avg': max(values)/mean if mean else None,
            'cv_population': statistics.pstdev(values)/mean if mean else None,
            'cv_sample_pct': statistics.stdev(values)/mean*100 if mean and len(values)>1 else None}


def result_digest(rows):
    # Queries have a deterministic ORDER BY; Decimal/date are canonical strings.
    payload = json.dumps(rows, ensure_ascii=False, separators=(',', ':'), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def summarize(values):
    return {'median_ms': statistics.median(values), 'min_ms': min(values),
            'max_ms': max(values), 'stdev_ms': statistics.pstdev(values),
            'samples_ms': values}


def walk(node):
    yield node
    for child in node.get('Plans', []):
        yield from walk(child)


def motion_summary(plan):
    result = []
    for node in walk(plan['Plan']):
        kind = node.get('Node Type', '')
        if 'Motion' not in kind:
            continue
        # GP JSON semantics are retained in the raw plan. This is NOT measured
        # wire traffic: row widths are planner estimates and compression differs.
        rows = node.get('Actual Rows', 0)
        result.append({'type': kind, 'senders': node.get('Senders'),
                       'receivers': node.get('Receivers'), 'actual_rows_reported': rows,
                       'actual_loops': node.get('Actual Loops'),
                       'plan_width': node.get('Plan Width'),
                       'reported_rows_times_plan_width': rows * node.get('Plan Width', 0),
                       'hash_key': node.get('Hash Key')})
    return result


def validate_database(name):
    if name != DATABASE or not name.endswith('_test'):
        raise ValueError('Only sharding_stage5_test is writable')


def credentials():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    conn = PostgresHook.get_connection('greenplum_dwh')
    return dict(host=conn.host, port=conn.port or 5432, user=conn.login,
                password=conn.password, connect_timeout=10)


def connect(name, readonly=False):
    if name not in ('dwh', DATABASE, 'postgres'):
        raise ValueError('Unknown database')
    if name == 'dwh':
        readonly = True  # enforce protection even if a caller omits the flag
    import psycopg2
    params = credentials()
    conn = psycopg2.connect(dbname=name, **params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("SET statement_timeout='10min'")
        cur.execute("SET lock_timeout='15s'")
        cur.execute("SET application_name='stage5_sharding'")
        if readonly:
            cur.execute('SET default_transaction_read_only=on')
        elif name == DATABASE:
            cur.execute("SET statement_mem='64MB'")
            cur.execute('SET optimizer=on')
    return conn


def execute(conn, query, params=None):
    # Every non-admin write connection is checked at the call site by target().
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description:
            return [dict(zip([d.name for d in cur.description], row)) for row in cur.fetchall()]
        return []


def target():
    validate_database(DATABASE)
    conn = connect(DATABASE)
    if conn.info.dbname != DATABASE or execute(conn, 'SELECT current_database() AS db')[0]['db'] != DATABASE:
        conn.close()
        raise RuntimeError('Actual target database differs from required test database')
    return conn


def assert_owned(conn):
    rows = execute(conn, 'SELECT document FROM stage5_meta.manifest WHERE id=1')
    if len(rows)!=1 or rows[0]['document'].get('owner') != OWNER:
        raise RuntimeError('No completed, owned stage5 snapshot; run prepare')
    return rows[0]['document']


def sql_text(name):
    return (SQL_DIR / name).read_text()


def queries(filename='004_workload.sql'):
    result = {}
    for section in sql_text(filename).split('-- name: ')[1:]:
        name, query = section.split('\n', 1)
        result[name.strip()] = query.strip().rstrip(';')
    return result


def fingerprint(conn, table, columns):
    query = sql_text('005_verify.sql').format(table=table, columns=columns)
    return execute(conn, query)[0]


def source_state(conn):
    return {
      'layers': execute(conn, 'SELECT layer,batch_id FROM dq.layer_state ORDER BY layer'),
      'counts_and_sums': execute(conn, "SELECT 'core' AS layer,count(*) AS rows,sum(amount)::text AS amount FROM core.ticket_flights UNION ALL SELECT 'dm',count(*),sum(amount_rub)::text FROM dm.ticket_flight_details ORDER BY layer"),
      'schemas': execute(conn, "SELECT nspname FROM pg_namespace WHERE nspname IN ('staging','core','dm','dq') ORDER BY nspname")}


def check_cluster(conn):
    rows = execute(conn, 'SELECT content,dbid,role,preferred_role,mode,status,port FROM gp_segment_configuration WHERE content>=0 ORDER BY content,dbid')
    assert len(rows)==4 and {r['content'] for r in rows}=={0,1}, rows
    for content in (0,1):
        pair = [r for r in rows if r['content']==content]
        assert {r['role'] for r in pair}=={'p','m'} and all(r['mode']=='s' and r['status']=='u' and r['role']==r['preferred_role'] for r in pair), pair
    return rows


def no_active_dags():
    from airflow import settings
    with settings.engine.connect() as conn:
        count = conn.exec_driver_sql("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')").scalar()
    if count:
        raise RuntimeError(f'{count} active DAG runs; benchmark requires an idle ETL platform')


def preflight(source):
    no_active_dags()
    segments = check_cluster(source)
    size = execute(source, 'SELECT pg_database_size(current_database()) AS bytes')[0]['bytes']
    free = shutil.disk_usage('/tmp').free
    required = max(6*1024**3, size*6)
    if free < required:
        raise RuntimeError(f'Need {required} free VM bytes for bounded copies and mirrors; have {free}')
    memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    if memory < 6*1024**3:
        raise RuntimeError('At least 6 GiB of VM physical memory required for this sequential run')
    report = {'utc': datetime.now(timezone.utc).isoformat(), 'source_bytes': size,
              'vm_free_bytes': free, 'required_free_bytes': required,
              'vm_physical_memory_bytes': memory, 'segments': segments,
              'repeats': 5, 'warmups': 1, 'target_statement_mem': '64MB',
              'execution_mode': 'one runner, one materialized work variant',
              'platform': os.uname().machine}
    save('environment.json', report)
    return report


def inventory(source):
    data = []
    for stmt in sql_text('001_inventory.sql').split(';'):
        if stmt.strip():
            data.append({'sql': stmt.strip(), 'rows': execute(source, stmt)})
    save('inventory.json', data)
    return data


def prepare():
    source = connect('dwh', readonly=True)
    try:
        preflight(source)
        inventory(source)
        admin = connect('postgres')
        try:
            exists = execute(admin, 'SELECT 1 FROM pg_database WHERE datname=%s', (DATABASE,))
            if not exists:
                # The sole administrative write creates this fixed test DB.
                validate_database(DATABASE)
                execute(admin, 'CREATE DATABASE sharding_stage5_test')
        finally:
            admin.close()
        dest = target()
        try:
            present = execute(dest, "SELECT to_regclass('stage5_meta.manifest') AS rel")[0]['rel']
            if present:
                manifest = assert_owned(dest)
                verify_snapshot(dest, manifest)
                save('snapshot.json', manifest)
                emit('snapshot_reused', tables=len(TABLES))
                return
            unexpected = execute(dest, "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema','gp_toolkit','pg_aoseg','pg_bitmapindex') AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','f','S')")
            if unexpected:
                raise RuntimeError('Existing database has unowned tables; refusing initialization')
            source.autocommit = False
            source.set_session(isolation_level='REPEATABLE READ', readonly=True)
            dest.autocommit = False
            manifest = {'owner': OWNER, 'utc': datetime.now(timezone.utc).isoformat(),
                        'source_database': 'dwh', 'tables': {}, 'source_state': source_state(source)}
            execute(dest, sql_text('002_schema.sql'))
            for name, (relation, columns) in TABLES.items():
                emit('snapshot_copy', table=name)
                src_query = f'SELECT {columns} FROM {relation}'
                expected = fingerprint(source, f'({src_query}) s', columns)
                with tempfile.TemporaryFile(mode='w+b') as stream:
                    with source.cursor() as cur:
                        cur.copy_expert(f'COPY ({src_query}) TO STDOUT WITH (FORMAT CSV)', stream)
                    stream.seek(0)
                    with dest.cursor() as cur:
                        cur.copy_expert(f'COPY stage5_base.{name} FROM STDIN WITH (FORMAT CSV)', stream)
                actual = fingerprint(dest, f'stage5_base.{name}', columns)
                assert expected == actual, (name, expected, actual)
                manifest['tables'][name] = {'source': relation, 'columns': columns, 'fingerprint': expected}
                execute(dest, f'ANALYZE stage5_base.{name}')
                emit('snapshot_verified', table=name, rows=expected['rows'])
            manifest['details_fingerprint'] = fingerprint(source, f'({DETAILS}) s', DETAIL_COLUMNS)
            assert fingerprint(dest, f'({DERIVED}) s', DETAIL_COLUMNS) == manifest['details_fingerprint']
            execute(dest, 'INSERT INTO stage5_meta.manifest VALUES (1,%s::jsonb)', (json.dumps(manifest, default=str),))
            dest.commit()
            source.rollback()
            save('snapshot.json', manifest)
            emit('snapshot_complete')
        except BaseException:
            dest.rollback()
            raise
        finally:
            dest.close()
    finally:
        source.close()


def verify_snapshot(conn, manifest):
    for name, item in manifest['tables'].items():
        actual = fingerprint(conn, f'stage5_base.{name}', item['columns'])
        assert actual == item['fingerprint'], (name, actual, item['fingerprint'])
        emit('snapshot_verified', table=name, rows=actual['rows'])


def reset_work(conn):
    assert_owned(conn)
    execute(conn, 'DROP SCHEMA stage5_work CASCADE; CREATE SCHEMA stage5_work')
    for alias, base in [('tf','ticket_flights'),('tickets','tickets'),('airports','airports'),('rates','exchange_rates'),('daily','flight_airport_daily')]:
        execute(conn, f'CREATE VIEW stage5_work.{alias} AS SELECT * FROM stage5_base.{base}')


def physical_counts(conn, name, replicated=False):
    if replicated:
        # GP7 replicated tables have no gp_segment_id system column.
        # gp_dist_random explicitly scans physical copies on all contents.
        contents = execute(conn, "SELECT content FROM gp_segment_configuration WHERE content>=0 AND role='p' ORDER BY content")
        counts = {r['content']: 0 for r in contents}
        for row in execute(conn, 'SELECT gp_execution_segment() AS content,count(*) AS rows FROM gp_dist_random(%s) GROUP BY 1', (name,)):
            counts[row['content']] = row['rows']
        return counts
    return {r['content']: r['rows'] for r in execute(conn, sql_text('003_skew.sql').format(table=name))}


def measure(conn, group, variant, names, expected, repeats=5):
    workload = queries()
    checks = {}
    # Establish equivalence outside timed EXPLAIN ANALYZE statements.
    for name in names:
        rows = execute(conn, workload[name], FILTERS)
        digest = result_digest(rows)
        if name in expected:
            assert digest == expected[name], (group, variant, name, 'different query results')
        else:
            expected[name] = digest
        checks[name] = {'rows': len(rows), 'sha256': digest}
    for name in names:
        execute(conn, workload[name], FILTERS)  # exactly one untimed warmup per query
    values = {name: [] for name in names}
    walls = {name: [] for name in names}
    all_plans = {name: [] for name in names}
    rng = random.Random(20261027)
    for repetition in range(repeats):
        order = list(names)
        rng.shuffle(order)
        for name in order:
            start = time.perf_counter()
            document = execute(conn, 'EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) '+workload[name], FILTERS)[0]['QUERY PLAN']
            walls[name].append((time.perf_counter()-start)*1000)
            plan = document[0]
            values[name].append(plan['Execution Time'])
            all_plans[name].append(plan)
        emit('measurement', group=group, variant=variant, repeat=repetition+1)
    result = []
    for name in names:
        save(f'plans/{group}__{variant}__{name}.json', all_plans[name])
        result.append({'group': group, 'variant': variant, 'query': name,
                       **summarize(values[name]), 'client_wall': summarize(walls[name]),
                       'equivalence': checks[name], 'motion': motion_summary(all_plans[name][0])})
    return result


def variant_table(conn, alias, base, policy):
    execute(conn, f'DROP VIEW stage5_work.{alias}')
    storage = AO if base in ('tickets','ticket_flights') else ''
    start = time.perf_counter()
    execute(conn, f'CREATE TABLE stage5_work.{alias} (LIKE stage5_base.{base}) {storage} DISTRIBUTED {policy}')
    execute(conn, f'INSERT INTO stage5_work.{alias} SELECT * FROM stage5_base.{base}')
    load_ms = (time.perf_counter()-start)*1000
    execute(conn, f'ANALYZE stage5_work.{alias}')
    return load_ms


def benchmark():
    source = connect('dwh', readonly=True)
    conn = target()
    records, layouts = [], []
    save('run-status.json', {'state': 'running', 'utc': datetime.now(timezone.utc).isoformat()})
    (OUT/'completion.json').unlink(missing_ok=True)
    (OUT/'verification.json').unlink(missing_ok=True)
    try:
        preflight(source)
        manifest = assert_owned(conn)
        runtime_source_state = source_state(source)
        save('dwh-before.json', runtime_source_state)
        if runtime_source_state != manifest['source_state']:
            emit('pinned_snapshot', snapshot_utc=manifest['utc'], note='dwh has newer publications; every variant still uses the same immutable snapshot')
        # A second runner cannot enter due to the process-wide flock in main().
        FILTERS.update(execute(conn, 'SELECT min(ticket_no) AS ticket_no,min(flight_id) AS flight_id FROM stage5_base.ticket_flights')[0])
        save('filters.json', FILTERS)
        expected = {}
        for group, (alias, base, variants, names) in GROUPS.items():
            order = list(variants)
            random.Random(20261027).shuffle(order)
            for variant, policy in order:
                no_active_dags()
                check_cluster(source)
                reset_work(conn)
                emit('variant_start', group=group, variant=variant, distribution=policy)
                load_ms = variant_table(conn, alias, base, policy)
                actual = fingerprint(conn, f'stage5_work.{alias}', manifest['tables'][base]['columns'])
                assert actual == manifest['tables'][base]['fingerprint']
                counts = physical_counts(conn, f'stage5_work.{alias}', policy=='REPLICATED')
                layouts.append({'group': group, 'variant': variant, 'policy': policy,
                                'load_ms': load_ms, 'fingerprint': actual,
                                'physical_rows_include_replicas': policy=='REPLICATED',
                                **skew_metrics(counts)})
                records.extend(measure(conn, group, variant, names, expected))
                save('layouts.json', layouts)
                save('measurements.json', records)
                emit('variant_complete', group=group, variant=variant)
        partition_experiment(conn, source, manifest, records, layouts)
        empty_segment_probe(conn)
        reset_work(conn)  # release final work copy; retain only immutable snapshot
        save('measurements.json', records)
        save('layouts.json', layouts)
        write_summary(records, layouts)
        assert source_state(source)==runtime_source_state, 'dwh publication changed during the run; retry outside ETL'
        check_cluster(source)
        save('completion.json', {'complete': True, 'utc': datetime.now(timezone.utc).isoformat(),
              'query_variants': len(records), 'layouts': len(layouts), 'dwh_unchanged': True,
              'snapshot_utc': manifest['utc'], 'dwh_state_before': runtime_source_state,
              'segments': check_cluster(source)})
        save('run-status.json', {'state': 'complete', 'snapshot_utc': manifest['utc'], 'utc': datetime.now(timezone.utc).isoformat()})
        emit('benchmark_complete', comparisons=len(records))
    finally:
        conn.close()
        source.close()


def partition_experiment(conn, source, manifest, records, layouts):
    bounds = execute(conn, 'SELECT min(scheduled_departure)::date AS lo,max(scheduled_departure)::date AS hi FROM stage5_base.flights')[0]
    start = bounds['lo'].replace(day=1)
    after = bounds['hi'].replace(day=1)
    def next_month(day):
        return day.replace(year=day.year+1, month=1) if day.month==12 else day.replace(month=day.month+1)
    stop = next_month(after)
    expected = {}
    for variant in ('flat','monthly'):
        no_active_dags()
        reset_work(conn)
        partition = ''
        if variant=='monthly':
            partition = f"PARTITION BY RANGE (flight_date) (START (DATE '{start}') END (DATE '{stop}') EVERY (INTERVAL '1 month'))"
        emit('variant_start', group='partition', variant=variant)
        before = time.perf_counter()
        execute(conn, f'CREATE TABLE stage5_work.fact (ticket_no text,flight_id integer,fare_conditions text,amount numeric(18,2),flight_date date) {AO} DISTRIBUTED BY (ticket_no) {partition}')
        execute(conn, 'INSERT INTO stage5_work.fact '+DERIVED)
        load_ms = (time.perf_counter()-before)*1000
        execute(conn, 'ANALYZE stage5_work.fact')
        actual = fingerprint(conn, 'stage5_work.fact', DETAIL_COLUMNS)
        assert actual==manifest['details_fingerprint']
        layouts.append({'group': 'partition', 'variant': variant, 'policy': 'BY (ticket_no)',
                        'load_ms': load_ms, 'fingerprint': actual,
                        **skew_metrics(physical_counts(conn, 'stage5_work.fact'))})
        records.extend(measure(conn, 'partition', variant, ['partition_filter','partition_all'], expected))
        partitions = execute(conn, "SELECT c.relname,pg_get_expr(c.relpartbound,c.oid) AS bound FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid WHERE i.inhparent='stage5_work.fact'::regclass ORDER BY c.relname")
        save(f'partition_{variant}.json', {'partitions': partitions, 'count': len(partitions)})
        save('measurements.json', records)
        save('layouts.json', layouts)
        check_cluster(source)


def empty_segment_probe(conn):
    execute(conn, 'CREATE TABLE stage5_work.empty_segment_probe (id integer,k text) DISTRIBUTED BY (k)')
    execute(conn, "INSERT INTO stage5_work.empty_segment_probe SELECT id,'same' FROM generate_series(1,1000) id")
    metrics = skew_metrics(physical_counts(conn, 'stage5_work.empty_segment_probe'))
    assert sorted(metrics['counts'].values()) == [0,1000]
    assert metrics['max_avg']==2
    save('empty-segment.json', metrics)


def write_summary(records, layouts):
    with (OUT/'measurements.csv').open('w') as stream:
        fields=['group','variant','query','median_ms','min_ms','max_ms','stdev_ms']
        writer=csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore', lineterminator='\n')
        writer.writeheader(); writer.writerows(records)
    with (OUT/'layouts.csv').open('w') as stream:
        fields=['group','variant','policy','min','max','mean','max_avg','cv_population','cv_sample_pct','load_ms']
        writer=csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore', lineterminator='\n')
        writer.writeheader(); writer.writerows(layouts)


def capabilities():
    import psycopg2
    conn = target()
    probes = []
    try:
        assert_owned(conn)
        for name, statement in queries('006_capabilities.sql').items():
            conn.autocommit = False
            try:
                execute(conn, statement)
                probes.append({'probe': name, 'accepted': True, 'sql': statement})
            except psycopg2.Error as exc:
                probes.append({'probe': name, 'accepted': False, 'sql': statement,
                               'sqlstate': exc.pgcode, 'error': exc.diag.message_primary})
            finally:
                conn.rollback()
                conn.autocommit = True
        result = {r['probe']: r['accepted'] for r in probes}
        assert result['compatible_primary_key'] and not result['incompatible_primary_key']
        assert result['random_heap'] and result['replicated_heap']
        save('capabilities.json', probes)
        emit('capabilities_checked', results=result)
    finally:
        conn.close()


def verify():
    conn=target(); source=connect('dwh', readonly=True)
    try:
        manifest=assert_owned(conn)
        if (OUT/'run-status.json').exists():
            state=json.loads((OUT/'run-status.json').read_text())
            assert state['state'] in ('complete','verified'), 'Latest benchmark is incomplete; old evidence cannot certify it'
        completion=json.loads((OUT/'completion.json').read_text())
        if 'snapshot_utc' in completion:
            assert completion['snapshot_utc']==manifest['utc'], 'Evidence belongs to another snapshot'
        verify_snapshot(conn, manifest)
        assert source_state(source)==completion.get('dwh_state_before',manifest['source_state']), 'dwh publication changed since the benchmark; rerun outside ETL'
        segments=check_cluster(source)
        completion=json.loads((OUT/'completion.json').read_text())
        assert completion['complete'] and completion['dwh_unchanged']
        records=json.loads((OUT/'measurements.json').read_text())
        required = {(g,v,q) for g,(_,_,variants,names) in GROUPS.items() for v,_ in variants for q in names}
        required.update(('partition',v,q) for v in ('flat','monthly') for q in ('partition_filter','partition_all'))
        assert {(r['group'],r['variant'],r['query']) for r in records} == required
        assert len(records)==completion['query_variants']==len(required)
        equivalent = {}
        for record in records:
            assert len(record['samples_ms'])>=5
            digest = record['equivalence']['sha256']
            assert digest
            equivalent.setdefault(record['query'], digest)
            assert equivalent[record['query']] == digest, 'Different results for the same workload query'
            plans = json.loads((OUT/f"plans/{record['group']}__{record['variant']}__{record['query']}.json").read_text())
            assert [p['Execution Time'] for p in plans] == record['samples_ms'], 'Samples differ from raw server plans'
            for key, value in summarize(record['samples_ms']).items():
                assert record[key] == value, f'Incorrect saved statistic: {key}'
        save('verification.json', {'passed': True,
                                  'source_read_only': execute(source, 'SHOW transaction_read_only')[0]['transaction_read_only'], 'query_variants': len(records), 'segments': segments,
                                  'snapshot_tables': len(TABLES), 'dwh_unchanged': True})
        save('run-status.json', {'state': 'verified', 'snapshot_utc': manifest['utc'], 'utc': datetime.now(timezone.utc).isoformat()})
        emit('verification_passed')
    finally:
        conn.close(); source.close()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('action', choices=('status','prepare','capabilities','benchmark','verify','all'))
    args=parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    with open('/tmp/stage5-sharding.lock','w') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another stage5 runner is active')
        if args.action=='status':
            conn=connect('dwh', readonly=True)
            try:
                inventory(conn); emit('status', segments=check_cluster(conn), source=source_state(conn))
            finally: conn.close()
        if args.action in ('prepare','all'): prepare()
        if args.action in ('capabilities','all'): capabilities()
        if args.action in ('benchmark','all'): benchmark()
        if args.action in ('verify','all'): verify()


if __name__=='__main__':
    main()
