#!/usr/bin/env bash
# macOS Bash 3.2; Python, libpq and all GP operations run inside Linux containers.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
action=${1:-status}
case "$action" in status|prepare|capabilities|benchmark|verify|all) ;; *) echo 'Usage: run.sh status|prepare|capabilities|benchmark|verify|all' >&2; exit 2;; esac
mkdir -p .cache/sharding docs/stage5-evidence
docker compose config --quiet
if [[ "$action" == all || "$action" == prepare || "$action" == benchmark ]]; then
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' > .cache/sharding/resources.txt
  docker compose exec -T greenplum df -h /data >> .cache/sharding/resources.txt
  docker info --format 'Engine={{.ServerVersion}} RAM={{.MemTotal}} Architecture={{.Architecture}}' >> .cache/sharding/resources.txt
fi
# Python's flock in the container serializes all users of this runner.
# Copy partial evidence even on failure; never include credentials or COPY files.
copy_evidence() {
  docker compose cp airflow-scheduler:/tmp/stage5-evidence/. docs/stage5-evidence/ >/dev/null 2>&1 || true
}
trap copy_evidence EXIT
docker compose exec -T -e PYTHONUNBUFFERED=1 airflow-scheduler \
  python /opt/airflow/project-scripts/sharding/benchmark.py "$action"
docker compose cp airflow-scheduler:/tmp/stage5-evidence/. docs/stage5-evidence/ >/dev/null
trap - EXIT
if [[ -f .cache/sharding/resources.txt ]]; then cp .cache/sharding/resources.txt docs/stage5-evidence/resources.txt; fi
