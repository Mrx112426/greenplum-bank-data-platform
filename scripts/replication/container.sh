#!/usr/bin/env bash
set -eo pipefail
source /usr/local/gpdb/greenplum_path.sh
export COORDINATOR_DATA_DIRECTORY=${COORDINATOR_DATA_DIRECTORY:-/data/coordinator/gpsne-1}
export PGCONNECT_TIMEOUT=5 PGOPTIONS='-c statement_timeout=30000'
set -u
action=${1:?action required}
target=${2:?target required}
state=/data/replication-lab
mkdir -p "$state"
# Serialize mutations; do not allow concurrent setup / failure / recovery.
exec 9>"$state/operation.lock"
flock -n 9 || { echo 'Another replication operation is running.' >&2; exit 1; }
sql() { psql -X -v ON_ERROR_STOP=1 -h localhost -U gpadmin -d postgres -At -c "$1"; }
probe() { psql -X -v ON_ERROR_STOP=1 -h localhost -U gpadmin -d replication_test -At -c "$1"; }
segments="SELECT content,dbid,role,preferred_role,mode,status,hostname,port,datadir FROM gp_segment_configuration WHERE content>=0 ORDER BY content,dbid"
status() {
    sql 'SELECT version()'
    sql 'SHOW data_directory'
    sql "$segments"
    gpstate -s
}
healthy() {
    [[ $(sql "SELECT count(*)=4 AND count(DISTINCT content)=2 AND count(*) FILTER (WHERE role='p')=2 AND count(*) FILTER (WHERE role='m')=2 AND bool_and(status='u' AND mode='s') FROM gp_segment_configuration WHERE content>=0") == t ]] &&
    [[ $(sql "SELECT count(*)=2 FROM (SELECT content FROM gp_segment_configuration WHERE content>=0 GROUP BY content HAVING count(*)=2 AND count(*) FILTER (WHERE role='p')=1 AND count(*) FILTER (WHERE role='m')=1 AND count(*) FILTER (WHERE preferred_role='p')=1 AND count(*) FILTER (WHERE preferred_role='m')=1) pairs") == t ]]
}
wait_sync() {
    local i
    for i in $(seq 1 120); do
        if healthy; then return; fi
        sleep 2
    done
    status
    echo 'Replication did not synchronize within 240 seconds.' >&2
    return 1
}
verify_probe() {
    [[ $(probe 'SELECT current_database()') == replication_test ]] || return 1
    [[ $(probe "SELECT count(*)=10000 AND count(DISTINCT id)=10000 AND bool_and(payload=md5(id::text)) FROM public.replication_probe WHERE id BETWEEN 1 AND 10000") == t ]] || return 1
    [[ $(probe 'SELECT count(*)=0 FROM public.replication_probe WHERE id<1 OR id>10001 OR payload IS DISTINCT FROM md5(id::text)') == t ]] || return 1
    probe "SELECT count(*),sum(id),md5(string_agg(id::text || ':' || payload,',' ORDER BY id)) FROM public.replication_probe" || return 1
    probe 'SELECT gp_segment_id,count(*) FROM public.replication_probe GROUP BY gp_segment_id ORDER BY gp_segment_id'
}
recover() {
    gprecoverseg -a || return 1
    wait_sync || return 1
    if [[ $(sql "SELECT count(*) FROM gp_segment_configuration WHERE content>=0 AND role<>preferred_role") != 0 ]]; then
        gprecoverseg -a -r || return 1
        wait_sync || return 1
    fi
    [[ $(sql "SELECT count(*) FROM gp_segment_configuration WHERE content>=0 AND role<>preferred_role") == 0 ]] || return 1
    if [[ "$target" == lab ]]; then verify_probe || return 1; fi
    sql "$segments"
}
case "$action" in
 status) status ;;
 setup)
    sql "$segments" | tee "$state/before-setup-$(date +%Y%m%dT%H%M%S).txt"
    [[ $(sql "SELECT count(*) FROM gp_segment_configuration WHERE content>=0 AND preferred_role='p'") == 2 ]]
    mirrors=$(sql "SELECT count(*) FROM gp_segment_configuration WHERE content>=0 AND preferred_role='m'")
    if [[ "$mirrors" == 2 ]]; then
        wait_sync
        echo 'Two synchronized mirrors already exist; no configuration changes.'
        exit 0
    fi
    [[ "$mirrors" == 0 ]] || { echo 'Partial mirror configuration: inspect and recover manually.' >&2; exit 1; }
    [[ $(sql "SELECT bool_and(status='u' AND role='p' AND hostname='localhost' AND datadir LIKE '/data/%') FROM gp_segment_configuration WHERE content>=0") == t ]] || {
        echo 'Unsupported topology; expected healthy single-host image with data in /data.' >&2; exit 1;
    }
    ssh -o BatchMode=yes -o ConnectTimeout=5 localhost true
    # Let this GP version choose matching ports and content IDs. Persist config in /data.
    mkdir -p /data/mirror1 /data/mirror2
    printf '%s\n' /data/mirror1 /data/mirror2 > "$state/mirror-directories"
    gpaddmirrors -a -o "$state/mirrors.conf" -m "$state/mirror-directories"
    cat "$state/mirrors.conf"
    gpaddmirrors -a -i "$state/mirrors.conf"
    wait_sync
    status ;;
 prepare)
    [[ "$target" == lab ]] || { echo 'Fixture writes are restricted to isolated lab.' >&2; exit 2; }
    wait_sync
    if [[ $(sql "SELECT count(*) FROM pg_database WHERE datname='replication_test'") == 0 ]]; then
        createdb -h localhost -U gpadmin replication_test
    fi
    if [[ $(probe "SELECT to_regclass('public.replication_probe') IS NULL") == t ]]; then
        probe "CREATE TABLE public.replication_probe (id integer PRIMARY KEY,payload text NOT NULL) DISTRIBUTED BY (id); INSERT INTO public.replication_probe SELECT id,md5(id::text) FROM generate_series(1,10000) id;"
    fi
    verify_probe ;;
 check)
    healthy || { status; echo 'Expected two synchronized primary/mirror pairs.' >&2; exit 1; }
    sql "$segments"
    if [[ "$target" == lab ]]; then verify_probe; fi
    echo 'PASS: two primary, two mirrors, matching contents, status=u, mode=s.' ;;
 recover) recover ;;
 failover)
    [[ "$target" == lab ]] || exit 2
    healthy
    verify_probe
    # Always recover on exit, including failed assertions and interrupted experiments.
    trap 'result=$?; trap - EXIT; if ! recover; then echo "RECOVERY FAILED: run lab recover and inspect gpAdminLogs" >&2; exit 1; fi; exit "$result"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    sql "$segments" | tee "$state/before-failure-$(date +%Y%m%dT%H%M%S).txt"
    row=$(sql "SELECT dbid,port,datadir FROM gp_segment_configuration WHERE content=0 AND role='p' AND preferred_role='p' AND status='u' AND mode='s'")
    IFS='|' read -r dbid port datadir <<< "$row"
    [[ "$dbid" =~ ^[0-9]+$ && "$port" =~ ^[0-9]+$ && "$port" != 5432 && "$datadir" == /data/data*/gpsne0 && "$datadir" != "$COORDINATOR_DATA_DIRECTORY" ]]
    mirror=$(sql "SELECT dbid FROM gp_segment_configuration WHERE content=0 AND role='m' AND status='u' AND mode='s'")
    [[ "$mirror" =~ ^[0-9]+$ ]]
    echo "Stopping only content=0 primary dbid=$dbid port=$port datadir=$datadir; mirror dbid=$mirror"
    pg_ctl -D "$datadir" stop -m immediate -w -t 30
    promoted=false
    for i in $(seq 1 90); do
        sql 'SELECT gp_request_fts_probe_scan()' >/dev/null
        if [[ $(sql "SELECT count(*) FROM gp_segment_configuration WHERE dbid=$mirror AND role='p' AND preferred_role='m' AND status='u'") == 1 && $(sql "SELECT status FROM gp_segment_configuration WHERE dbid=$dbid") == d ]]; then
            promoted=true; break
        fi
        sleep 2
    done
    [[ "$promoted" == true ]] || { status; echo 'Mirror promotion was not observed.' >&2; exit 1; }
    sql "$segments" | tee "$state/after-failure-$(date +%Y%m%dT%H%M%S).txt"
    verify_probe
    probe "INSERT INTO public.replication_probe SELECT 10001,md5('10001') WHERE NOT EXISTS (SELECT 1 FROM public.replication_probe WHERE id=10001)"
    [[ $(probe 'SELECT count(*) FROM public.replication_probe') == 10001 ]]
    verify_probe
    echo 'PASS: original primary down, former mirror active, SELECT and INSERT succeeded.'
    ;;
 *) echo 'Unknown container action.' >&2; exit 2 ;;
esac
