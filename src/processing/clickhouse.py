"""Bounded HTTP inserts usable by Spark workers without another connector."""

from dataclasses import dataclass, field
import re
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class ClickHouseConfig:
    url: str = "http://localhost:8123"
    database: str = "ecommerce"
    table: str = "session_fraud"
    user: str = "ecommerce"
    password: str = field(default="ecommerce_local", repr=False)
    batch_rows: int = 1000
    timeout_seconds: int = 30

    def __post_init__(self):
        for identifier in (self.database, self.table):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
                raise ValueError("ClickHouse database/table must be SQL identifiers")
        parsed = urlsplit(self.url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.query or parsed.fragment or parsed.username or parsed.password):
            raise ValueError("ClickHouse URL must be an HTTP(S) endpoint without credentials/query")
        if self.batch_rows <= 0 or self.timeout_seconds <= 0:
            raise ValueError("ClickHouse batch size and timeout must be positive")

    @property
    def qualified_table(self):
        return f"`{self.database}`.`{self.table}`"


def execute(config: ClickHouseConfig, sql: str, payload: bytes = b"") -> None:
    parameters = urlencode({"query": sql, "date_time_input_format": "best_effort",
                            "wait_end_of_query": "1"})
    request = Request(f"{config.url.rstrip('/')}?{parameters}", data=payload,
                      headers={"X-ClickHouse-User": config.user,
                               "X-ClickHouse-Key": config.password,
                               "Content-Type": "application/octet-stream"}, method="POST")
    # HTTP/query errors propagate so Spark cannot commit a failed microbatch.
    with urlopen(request, timeout=config.timeout_seconds) as response:
        response.read()


def initialize_table(config: ClickHouseConfig) -> None:
    execute(config, f"CREATE DATABASE IF NOT EXISTS `{config.database}`")
    execute(config, f"""
        CREATE TABLE IF NOT EXISTS {config.qualified_table} (
            user_id Int64, session_id String,
            window_start DateTime64(6, 'UTC'), window_end DateTime64(6, 'UTC'),
            event_count UInt64, payment_attempts UInt64, payment_failures UInt64,
            payment_completions UInt64,
            first_event_time DateTime64(6, 'UTC'), last_event_time DateTime64(6, 'UTC'),
            fraud_reasons Array(String), is_fraudulent Bool
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(window_start)
        ORDER BY (user_id, session_id, window_start, window_end)
    """)


def insert_partition(rows, config: ClickHouseConfig) -> None:
    """Insert one partition incrementally; task retries can repeat accepted rows."""
    pending = []
    for row in rows:
        # Spark to_json serializes timestamps on the JVM using the session UTC zone.
        pending.append(row["payload"])
        if len(pending) >= config.batch_rows:
            execute(config, f"INSERT INTO {config.qualified_table} FORMAT JSONEachRow",
                    ("\n".join(pending) + "\n").encode("utf-8"))
            pending.clear()
    if pending:
        execute(config, f"INSERT INTO {config.qualified_table} FORMAT JSONEachRow",
                ("\n".join(pending) + "\n").encode("utf-8"))


def write_batch(batch, batch_id: int, config: ClickHouseConfig) -> None:
    """Fully consume each microbatch; no rows are collected on the driver."""
    from pyspark.sql import functions as F

    payloads = batch.select(F.to_json(F.struct(*batch.columns)).alias("payload"))
    payloads.foreachPartition(lambda rows: insert_partition(rows, config))
