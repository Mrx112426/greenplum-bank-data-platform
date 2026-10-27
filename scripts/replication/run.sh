#!/usr/bin/env bash
# macOS Bash 3.2 compatible; all GP utilities run in Linux as gpadmin.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
script="$root/scripts/replication/run.sh"
cd "$root"
target=${1:-lab}
action=${2:-status}
case "$target" in
  lab) compose=(docker compose -p greenplum-replication-lab -f compose.replication.yaml) ;;
  platform) compose=(docker compose -f compose.yaml) ;;
  *) echo 'Usage: bash scripts/replication/run.sh lab|platform up|setup|status|prepare|check|failover|recover|restart' >&2; exit 2 ;;
esac
case "$action" in
  up)
    "${compose[@]}" config --quiet
    "${compose[@]}" up -d --wait greenplum
    exit ;;
  restart)
    [[ "$target" == lab ]] || { echo 'Restart is restricted to isolated lab.' >&2; exit 2; }
    bash "$script" lab check
    before=$("${compose[@]}" exec -T --user gpadmin greenplum /usr/local/gpdb/bin/psql -X -At -v ON_ERROR_STOP=1 -h localhost -U gpadmin -d replication_test -c "SELECT count(*),sum(id),md5(string_agg(id::text || ':' || payload,',' ORDER BY id)) FROM public.replication_probe")
    "${compose[@]}" restart greenplum
    "${compose[@]}" up -d --wait greenplum
    bash "$script" lab check
    after=$("${compose[@]}" exec -T --user gpadmin greenplum /usr/local/gpdb/bin/psql -X -At -v ON_ERROR_STOP=1 -h localhost -U gpadmin -d replication_test -c "SELECT count(*),sum(id),md5(string_agg(id::text || ':' || payload,',' ORDER BY id)) FROM public.replication_probe")
    [[ "$before" == "$after" ]] || { echo 'Data changed across restart.' >&2; exit 1; }
    exit ;;
  setup|status|prepare|check|failover|recover) ;;
  *) echo "Unknown action: $action" >&2; exit 2 ;;
esac
if [[ "$action" == failover ]]; then
  [[ "$target" == lab && "${3:-}" == --inject-fault ]] || {
    echo 'Fault injection requires: lab failover --inject-fault' >&2; exit 2;
  }
  cid=$("${compose[@]}" ps -q greenplum)
  label=$(docker inspect --format '{{index .Config.Labels "org.greenplum-bank.replication-lab"}}' "$cid")
  [[ "$label" == true ]] || { echo 'Isolated container label missing.' >&2; exit 2; }
fi
"${compose[@]}" exec -T --user gpadmin greenplum bash -s -- "$action" "$target" < scripts/replication/container.sh
