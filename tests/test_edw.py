import os
import unittest
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pendulum

from edw.cbr import decimal_value, extraction_dates, parse_rates
from edw.flights import _build_core
from edw.quality import _build_dm
from edw.runtime import gp_connection, initialize, publish_state, transaction


def context(**conf):
    return {"dag_run": SimpleNamespace(conf=conf, run_id="unit_test"),
            "data_interval_end": pendulum.datetime(2026, 10, 15, tz="Europe/Moscow")}


class RatesTests(unittest.TestCase):
    def test_test_database_guard(self):
        with patch.dict(os.environ, {"EDW_TEST_DATABASE": "guard_test"}), patch("edw.runtime.PostgresHook") as hook:
            conn = hook.return_value.get_conn.return_value
            conn.info.dbname = "dwh"
            with self.assertRaisesRegex(RuntimeError, "Refusing test writes"):
                gp_connection()
            hook.assert_called_once_with(postgres_conn_id="greenplum_dwh", database="guard_test")
            conn.close.assert_called_once()
        with patch.dict(os.environ, {"EDW_TEST_DATABASE": "dwh"}):
            with self.assertRaises(ValueError):
                gp_connection()

    def test_decimal_precision_and_invalid_values(self):
        self.assertEqual(decimal_value("1 234,5678"), Decimal("1234.5678"))
        for value in (None, "", "NaN", "Infinity", "bad"):
            self.assertIsNone(decimal_value(value))

    def test_nominal_and_effective_date(self):
        xml = b'<ValCurs Date="09.10.2026"><Valute><CharCode>CNY</CharCode><Nominal>10</Nominal><Value>125,1234</Value></Valute></ValCurs>'
        row = parse_rates(xml, date(2026, 10, 11))[0]
        self.assertEqual(row[0], date(2026, 10, 11))
        self.assertEqual(row[1], date(2026, 10, 9))
        self.assertEqual(row[5], Decimal("12.512340000000"))

    def test_empty_or_wrong_document(self):
        for xml in (b"<error/>", b'<ValCurs Date="09.10.2026"/>'):
            with self.assertRaises(ValueError):
                parse_rates(xml, date(2026, 10, 9))

    def test_date_windows(self):
        days = extraction_dates(context(cbr_start_date="2026-10-10", cbr_end_date="2026-10-12"), None)
        self.assertEqual(days, [date(2026, 10, i) for i in (10, 11, 12)])
        days = extraction_dates(context(cbr_end_date="2026-10-14"), date(2026, 10, 13))
        self.assertEqual(days[-1], date(2026, 10, 14))
        self.assertLess(days[0], date(2026, 10, 13))
        with self.assertRaises(ValueError):
            extraction_dates(context(cbr_start_date="2026-10-13", cbr_end_date="2026-10-12"), None)


@unittest.skipUnless(os.environ.get("EDW_TEST_DATABASE", "").endswith("_test"), "Dedicated Greenplum test DB required")
class GreenplumTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        initialize()

    def test_quarantine_marts_idempotency_and_rollback(self):
        # Every fixture change is rolled back; the real dwh database is never used.
        with self.assertRaisesRegex(RuntimeError, "rollback_fixture"):
            with transaction() as conn, conn.cursor() as cur:
                self.assertEqual(conn.info.dbname, os.environ["EDW_TEST_DATABASE"])
                cur.execute("TRUNCATE staging.aircrafts, staging.airports, staging.bookings, staging.seats, staging.flights, staging.tickets, staging.ticket_flights, staging.boarding_passes, staging.exchange_rates")
                cur.execute("INSERT INTO staging.aircrafts (aircraft_code,model,range,batch_id) VALUES ('AAA','Demo',1000,'fixture')")
                cur.execute("INSERT INTO staging.airports (airport_code,airport_name,city,longitude,latitude,timezone,batch_id) VALUES ('AAA','A','City A',0,0,'UTC','fixture'),('BBB','B','City B',1,1,'UTC','fixture')")
                cur.execute("INSERT INTO staging.bookings VALUES ('B1','2016-01-01',300,'fixture',now()),('BAD','2016-01-01',-1,'fixture',now()),('DUP','2016-01-01',100,'fixture',now()),('DUP','2016-01-01',100,'fixture',now())")
                cur.execute("INSERT INTO staging.seats VALUES ('AAA','1A','Economy','fixture',now())")
                cur.execute("INSERT INTO staging.flights VALUES (1,'F1','2016-01-02 10:00+00','2016-01-02 11:00+00','AAA','BBB','Scheduled','AAA',NULL,NULL,'fixture',now()),(2,'F2','2016-01-03 10:00+00','2016-01-03 11:00+00','AAA','BBB','Scheduled','AAA',NULL,NULL,'fixture',now())")
                cur.execute("INSERT INTO staging.tickets VALUES ('T1','B1','P1','Passenger 1',NULL,'fixture',now()),('T2','B1','P2','Passenger 2',NULL,'fixture',now()),('T3','BAD','P3','Rejected',NULL,'fixture',now())")
                cur.execute("INSERT INTO staging.ticket_flights VALUES ('T1',1,'Economy',100,'fixture',now()),('T1',2,'Economy',100,'fixture',now()),('T2',1,'Economy',100,'fixture',now()),('T3',1,'Economy',10,'fixture',now())")
                cur.execute("INSERT INTO staging.boarding_passes VALUES ('T1',1,1,'1A','fixture',now())")
                cur.execute("INSERT INTO staging.exchange_rates VALUES ('2026-10-13','2026-10-13','USD',1,80,80,'key','hash','fixture',now()),('2026-10-13','2026-10-13','EUR',1,90,90,'key','hash','fixture',now()),('2026-10-13','2026-10-13','CNY',1,10,10,'key','hash','fixture',now())")
                publish_state(cur, "staging_flights", "fixture")
                publish_state(cur, "staging_cbr", "fixture")
                summary = _build_core(cur, "core_fixture", context())
                self.assertEqual(summary["rejected_rows"], 5)
                self.assertEqual(summary["tables"]["bookings"], {"source": 4, "accepted": 1, "rejected": 3})
                self.assertEqual(summary["tables"]["tickets"]["rejected"], 1)
                self.assertEqual(summary["tables"]["ticket_flights"]["rejected"], 1)
                _build_core(cur, "core_fixture", context())
                cur.execute("SELECT count(*) FROM dq.rejected_records WHERE batch_id='core_fixture'")
                self.assertEqual(cur.fetchone()[0], 5)
                _build_dm(cur, "dm_fixture", context())
                cur.execute("SELECT booking_count,booking_amount_rub,ticket_count,ticket_flight_count,ticket_flight_amount_rub FROM dm.booking_sales_daily")
                self.assertEqual(cur.fetchone(), (1, Decimal("300"), 2, 3, Decimal("300")))
                cur.execute("SELECT count(*),sum(amount_rub) FROM dm.ticket_flight_details")
                self.assertEqual(cur.fetchone(), (3, Decimal("300")))
                cur.execute("SELECT sum(flight_count),sum(ticket_count) FROM dm.flight_airport_daily")
                self.assertEqual(cur.fetchone(), (Decimal(2), Decimal(3)))
                # A second incremental slice replaces its date without deleting prior dates.
                cur.execute("UPDATE staging.exchange_rates SET request_date='2026-10-14', rate_date='2026-10-14', rate_value=rate_value+1, source_value=source_value+1")
                _build_core(cur, "core_fixture", context())
                _build_core(cur, "core_fixture", context())
                cur.execute("SELECT count(*) FROM core.exchange_rates")
                self.assertEqual(cur.fetchone()[0], 6)
                cur.execute("SAVEPOINT publication")
                cur.execute("TRUNCATE dm.ticket_flight_details")
                cur.execute("ROLLBACK TO SAVEPOINT publication")
                cur.execute("SELECT count(*) FROM dm.ticket_flight_details")
                self.assertEqual(cur.fetchone()[0], 3)
                raise RuntimeError("rollback_fixture")
        with transaction() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM core.bookings")
            self.assertEqual(cur.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
