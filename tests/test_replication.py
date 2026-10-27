"""Live, non-fault tests. Requires: run.sh lab up/setup/prepare.
Run with Python 3 on macOS; Docker CLI performs all database access.
"""
import pathlib
import subprocess
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
COMPOSE = ['docker', 'compose', '-p', 'greenplum-replication-lab', '-f', 'compose.replication.yaml']


def command(*args):
    result = subprocess.run(args, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise AssertionError(f"Command failed ({result.returncode}): {args}\n{result.stdout}")
    return result.stdout.strip()


def sql(query, database='postgres'):
    return command(*COMPOSE, 'exec', '-T', '--user', 'gpadmin', 'greenplum',
                   '/usr/local/gpdb/bin/psql', '-X', '-At', '-v', 'ON_ERROR_STOP=1',
                   '-h', 'localhost', '-U', 'gpadmin', '-d', database, '-c', query)


class ReplicationTests(unittest.TestCase):
    def test_fault_requires_isolation_and_explicit_flag(self):
        for args in [('platform', 'failover', '--inject-fault'), ('lab', 'failover')]:
            result = subprocess.run(['bash', 'scripts/replication/run.sh', *args],
                                    cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_two_primary(self):
        self.assertEqual(sql("SELECT count(*) FROM gp_segment_configuration WHERE content>=0 AND role='p'"), '2')

    def test_two_mirrors(self):
        self.assertEqual(sql("SELECT count(*) FROM gp_segment_configuration WHERE content>=0 AND role='m'"), '2')

    def test_pairs_and_sync(self):
        self.assertEqual(sql("SELECT count(*) FROM (SELECT content FROM gp_segment_configuration WHERE content>=0 GROUP BY content HAVING count(*)=2 AND count(*) FILTER (WHERE role='p')=1 AND count(*) FILTER (WHERE role='m')=1 AND bool_and(status='u' AND mode='s')) pairs"), '2')

    def test_preferred_roles_restored(self):
        self.assertEqual(sql("SELECT count(*) FROM gp_segment_configuration WHERE content>=0 AND role<>preferred_role"), '0')

    def test_fixture_integrity(self):
        output = command('bash', 'scripts/replication/run.sh', 'lab', 'check')
        self.assertIn('PASS:', output)

    def test_setup_idempotency(self):
        query = 'SELECT content,dbid,role,preferred_role,hostname,port,datadir FROM gp_segment_configuration ORDER BY dbid'
        before = sql(query)
        command('bash', 'scripts/replication/run.sh', 'lab', 'setup')
        command('bash', 'scripts/replication/run.sh', 'lab', 'setup')
        self.assertEqual(before, sql(query))


if __name__ == '__main__':
    unittest.main(verbosity=2)
