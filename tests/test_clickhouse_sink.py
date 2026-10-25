import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from src.processing.clickhouse import (
    ClickHouseConfig, execute, initialize_table, insert_partition,
)


class ClickHouseSinkTests(unittest.TestCase):
    def test_partition_chunks_and_empty_partition(self):
        config = ClickHouseConfig(batch_rows=2)
        rows = [{"payload": '{"user_id":1}'} for _ in range(5)]
        with patch("src.processing.clickhouse.execute") as send:
            insert_partition(iter(rows), config)
            self.assertEqual([len(call.args[2].splitlines()) for call in send.call_args_list],
                             [2, 2, 1])
            self.assertTrue(all("FORMAT JSONEachRow" in call.args[1]
                                for call in send.call_args_list))
            send.reset_mock()
            insert_partition(iter([]), config)
            send.assert_not_called()

    def test_http_auth_and_query_errors(self):
        config = ClickHouseConfig(password="secret")
        with patch("src.processing.clickhouse.urlopen") as post:
            post.return_value.__enter__.return_value.read.return_value = b""
            execute(config, "SELECT 1")
            request = post.call_args.args[0]
            self.assertEqual(request.get_header("X-clickhouse-key"), "secret")
            self.assertNotIn("secret", request.full_url)
            self.assertIn("wait_end_of_query=1", request.full_url)
            post.side_effect = HTTPError(config.url, 500, "failed", {}, None)
            with self.assertRaises(HTTPError):
                execute(config, "SELECT 1")

    def test_replay_uses_same_table_key_and_failures_propagate(self):
        config = ClickHouseConfig()
        with patch("src.processing.clickhouse.execute") as send:
            initialize_table(config)
            ddl = send.call_args_list[1].args[1]
            self.assertIn("ReplacingMergeTree()", ddl)
            self.assertIn("ORDER BY (user_id, session_id, window_start, window_end)", ddl)
            send.side_effect = RuntimeError("database unavailable")
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                insert_partition(iter([{"payload": "{}"}]), config)

    def test_validates_identifiers_and_endpoint(self):
        for options in ({"table": "bad; DROP DATABASE ecommerce"},
                        {"database": ""}, {"url": "file:///tmp/test"},
                        {"url": "http://user:secret@localhost:8123"},
                        {"batch_rows": 0}, {"timeout_seconds": -1}):
            with self.assertRaises(ValueError):
                ClickHouseConfig(**options)
        self.assertNotIn("secret", repr(ClickHouseConfig(password="secret")))
