"""Unit tests run on macOS; STAGE5_LIVE=1 adds persisted-run verification.
No test mutates dwh, injects faults, or launches a new benchmark.
"""
import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from decimal import Decimal

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('stage5_benchmark', ROOT/'scripts/sharding/benchmark.py')
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)
EVIDENCE = ROOT/'docs/stage5-evidence'


class MetricsTests(unittest.TestCase):
    def test_only_explicit_test_database_is_writable(self):
        BENCH.validate_database('sharding_stage5_test')
        for name in ('dwh', 'postgres', 'other_test', 'sharding_stage5', ''):
            with self.assertRaises(ValueError):
                BENCH.validate_database(name)

    def test_dwh_connection_is_read_only_even_without_flag(self):
        connection = MagicMock()
        driver = SimpleNamespace(connect=MagicMock(return_value=connection))
        with patch.dict('sys.modules', {'psycopg2': driver}), patch.object(BENCH, 'credentials', return_value={}):
            BENCH.connect('dwh')
        statements = [c.args[0] for c in connection.cursor.return_value.__enter__.return_value.execute.call_args_list]
        self.assertIn('SET default_transaction_read_only=on', statements)

    def test_actual_database_mismatch_is_rejected(self):
        connection = SimpleNamespace(info=SimpleNamespace(dbname='dwh'), close=MagicMock())
        with patch.object(BENCH, 'connect', return_value=connection):
            with self.assertRaises(RuntimeError):
                BENCH.target()
        connection.close.assert_called_once()

    def test_empty_segment_is_included(self):
        result = BENCH.skew_metrics({0: 0, 1: 1000})
        self.assertEqual((result['min'], result['max'], result['mean']), (0, 1000, 500))
        self.assertEqual(result['max_avg'], 2)
        self.assertEqual(result['cv_population'], 1)
        self.assertAlmostEqual(result['cv_sample_pct'], math.sqrt(2)*100)

    def test_balanced_and_empty_relations(self):
        self.assertEqual(BENCH.skew_metrics({0: 100, 1: 100})['cv_population'], 0)
        empty = BENCH.skew_metrics({0: 0, 1: 0})
        self.assertIsNone(empty['max_avg'])
        self.assertIsNone(empty['cv_population'])
        with self.assertRaises(ValueError):
            BENCH.skew_metrics({})

    def test_result_equivalence_detects_changes(self):
        baseline = [{'n': 2, 'amount': Decimal('10.25')}]
        self.assertEqual(BENCH.result_digest(baseline), BENCH.result_digest(list(baseline)))
        self.assertNotEqual(BENCH.result_digest(baseline), BENCH.result_digest([{'n': 2, 'amount': Decimal('10.26')}]))

    def test_robust_summary_retains_outliers(self):
        result = BENCH.summarize([1, 2, 3, 4, 100])
        self.assertEqual(result['median_ms'], 3)
        self.assertEqual((result['min_ms'], result['max_ms']), (1, 100))
        self.assertEqual(len(result['samples_ms']), 5)


@unittest.skipUnless(os.environ.get('STAGE5_LIVE')=='1', 'Set STAGE5_LIVE=1 after run.sh all')
class LiveExperimentsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.records = json.loads((EVIDENCE/'measurements.json').read_text())
        cls.layouts = json.loads((EVIDENCE/'layouts.json').read_text())

    def test_live_snapshot_and_mirrors(self):
        result = subprocess.run(['bash','scripts/sharding/run.sh','verify'], cwd=ROOT,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertIn('verification_passed', result.stdout)

    def test_every_variant_has_five_plans_and_equal_results(self):
        expected = {}
        for row in self.records:
            self.assertGreaterEqual(len(row['samples_ms']), 5)
            expected.setdefault(row['query'],row['equivalence']['sha256'])
            self.assertEqual(row['equivalence']['sha256'], expected[row['query']])
            plans = json.loads((EVIDENCE/f"plans/{row['group']}__{row['variant']}__{row['query']}.json").read_text())
            self.assertEqual(len(plans), len(row['samples_ms']))
            self.assertTrue(all(p['Execution Time']>=0 for p in plans))
            self.assertEqual([p['Execution Time'] for p in plans],row['samples_ms'])
            for key in ('median_ms','min_ms','max_ms','stdev_ms'):
                self.assertAlmostEqual(row[key],BENCH.summarize(row['samples_ms'])[key],places=9)

    def test_physical_layout_and_replica_accounting(self):
        for row in self.layouts:
            counts = row['counts']
            self.assertEqual(set(counts), {'0','1'})
            logical = row['fingerprint']['rows']
            recomputed=BENCH.skew_metrics(counts)
            for key in ('min','max','mean','max_avg','cv_population','cv_sample_pct'):
                self.assertAlmostEqual(row[key],recomputed[key],places=9)
            if row.get('physical_rows_include_replicas'):
                self.assertEqual(list(counts.values()), [logical,logical])
            else:
                self.assertEqual(sum(counts.values()), logical)
        empty = json.loads((EVIDENCE/'empty-segment.json').read_text())
        self.assertEqual(sorted(empty['counts'].values()), [0,1000])

    def test_partition_pruning_and_identical_distribution(self):
        partition_layouts=[r for r in self.layouts if r['group']=='partition']
        self.assertEqual(len(partition_layouts),2)
        self.assertEqual(partition_layouts[0]['fingerprint'],partition_layouts[1]['fingerprint'])
        self.assertEqual(partition_layouts[0]['policy'],partition_layouts[1]['policy'])
        for query, expected in [('partition_filter',1),('partition_all',14)]:
            plans=json.loads((EVIDENCE/f'plans/partition__monthly__{query}.json').read_text())
            for plan in plans:
                scans=[n for n in BENCH.walk(plan['Plan']) if n['Node Type']=='Dynamic Seq Scan']
                self.assertEqual(len(scans),1)
                self.assertEqual(scans[0]['Number of partitions to scan'],expected)
        probes={r['probe']:r['accepted'] for r in json.loads((EVIDENCE/'capabilities.json').read_text())}
        self.assertTrue(probes['compatible_primary_key'])
        self.assertFalse(probes['incompatible_primary_key'])

    def test_matrix_contains_all_required_strategies(self):
        policies = {r['policy'] for r in self.layouts}
        self.assertTrue({'RANDOMLY','REPLICATED','BY (ticket_no)','BY (flight_id)',
                         'BY (ticket_no,flight_id)','BY (fare_conditions)'}.issubset(policies))
        self.assertEqual({r['variant'] for r in self.records if r['group']=='partition'}, {'flat','monthly'})
        self.assertGreater(json.loads((EVIDENCE/'partition_monthly.json').read_text())['count'], 1)
        expected={(g,v,q) for g,(_,_,variants,names) in BENCH.GROUPS.items() for v,_ in variants for q in names}
        expected.update(('partition',v,q) for v in ('flat','monthly') for q in ('partition_filter','partition_all'))
        self.assertEqual({(r['group'],r['variant'],r['query']) for r in self.records}, expected)


if __name__=='__main__':
    unittest.main(verbosity=2)
