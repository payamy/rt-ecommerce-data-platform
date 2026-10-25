import json
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import SparkSession

from src.processing import EVENT_SCHEMA, FraudConfig, label_sessions, parse_events


class ProcessingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("fraud-processing-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def events(self):
        records = []
        for session, attempts in [("fraud", 6), ("normal", 2)]:
            for index in range(attempts):
                for kind in ("payment_attempted", "payment_failed"):
                    records.append(dict(event_id=f"{session}-{index}-{kind}",
                                        event_type=kind, user_id=1,
                                        session_id=session,
                                        event_time=f"2026-10-24T00:00:{index + 20:02d}+00:00"))
        return records

    def test_batch_labels_deduplication_and_invalid_input(self):
        records = self.events()
        records += [records[0]] * 10
        records.append(dict(records[0], event_id="bad", event_time="invalid"))
        payloads = self.spark.createDataFrame(
            [(json.dumps(record),) for record in records] + [("invalid-json",)], ["value"])
        rows = label_sessions(parse_events(payloads)).collect()
        self.assertTrue(rows)
        for row in rows:
            fraudulent = row.session_id == "fraud"
            self.assertEqual(row.is_fraudulent, fraudulent)
            self.assertEqual(row.payment_attempts, 6 if fraudulent else 2)
            self.assertEqual(len(row.fraud_reasons), 2 if fraudulent else 0)

    def test_failures_alone_and_spread_out_attempts(self):
        records = [dict(event_id=str(i), user_id=1, session_id="slow",
                        event_type="payment_attempted",
                        event_time=f"2026-10-24T00:{i * 2:02d}:00Z") for i in range(6)]
        records += [dict(event_id=f"failure-{i}", user_id=1, session_id="failures",
                         event_type="payment_failed", event_time="2026-10-24T00:00:20Z")
                    for i in range(5)]
        rows = label_sessions(self.spark.createDataFrame(records, EVENT_SCHEMA)).collect()
        for row in rows:
            self.assertEqual(row.is_fraudulent, row.session_id == "failures")
            if row.is_fraudulent:
                self.assertEqual(row.fraud_reasons, ["repeated_payment_failures"])

    def test_stream_matches_batch_and_deduplicates_across_triggers(self):
        with tempfile.TemporaryDirectory() as directory:
            incoming = Path(directory) / "input"
            incoming.mkdir()
            stream = self.spark.readStream.schema(EVENT_SCHEMA).json(str(incoming))
            result = label_sessions(stream)
            query = (result.writeStream.format("memory").queryName("fraud_test")
                     .outputMode("append")
                     .option("checkpointLocation", str(Path(directory) / "checkpoints"))
                     .start())
            try:
                records = self.events()
                (incoming / "first.json").write_text("\n".join(map(json.dumps, records)))
                query.processAllAvailable()
                (incoming / "duplicates.json").write_text("\n".join(map(json.dumps, records)))
                query.processAllAvailable()
                # Advance event time so all tested windows can finalize.
                future = dict(records[0], event_id="future", session_id="future",
                              event_time="2026-10-24T00:10:00Z")
                (incoming / "future.json").write_text(json.dumps(future))
                query.processAllAvailable()
                actual = self.spark.table("fraud_test").collect()
                expected = label_sessions(self.spark.createDataFrame(records, EVENT_SCHEMA)).collect()
                self.assertTrue(actual)
                # Lists in fraud_reasons are not hashable; compare keyed dictionaries.
                key = lambda row: (row.session_id, row.window_start, row.window_end)
                self.assertEqual({key(row): row.asDict() for row in actual},
                                 {key(row): row.asDict() for row in expected})
            finally:
                query.stop()

    def test_configuration_validation(self):
        for options in ({"min_attempts": 0}, {"lateness_seconds": -1},
                        {"slide_seconds": 61}, {"min_failures": True}):
            with self.assertRaises(ValueError):
                FraudConfig(**options)
