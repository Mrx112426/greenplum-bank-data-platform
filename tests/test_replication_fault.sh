#!/usr/bin/env bash
# Explicit destructive test; never invoked by ordinary tests or platform startup.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "${1:-}" == --inject-fault ]] || { echo 'Requires --inject-fault' >&2; exit 2; }
bash scripts/replication/run.sh lab failover --inject-fault
marker=$(docker compose -p greenplum-replication-lab -f compose.replication.yaml exec -T --user gpadmin greenplum /usr/local/gpdb/bin/psql -X -At -v ON_ERROR_STOP=1 -h localhost -U gpadmin -d replication_test -c "SELECT count(*) FROM public.replication_probe WHERE id=10001 AND payload=md5('10001')")
[[ "$marker" == 1 ]] || { echo 'Post-failover INSERT was lost during recovery.' >&2; exit 1; }
python3 tests/test_replication.py
