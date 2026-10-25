# Session fraud processing

`label_sessions(events, config=FraudConfig())` accepts a PySpark DataFrame,
streaming or static, and returns a labeled DataFrame without starting a query
or connecting a source or sink. Use `EVENT_SCHEMA` for decoded events, or
`parse_events(payloads, column="value")` for JSON string/binary payloads.
Install the root requirements and use Spark 3.5 with a supported Python and JVM.
Set `spark.sql.session.timeZone` to `UTC` in the application.

```python
from src.processing import FraudConfig, label_sessions, parse_events

# Caller supplies payloads from whichever source is selected later.
events = parse_events(payloads)
labels = label_sessions(events, FraudConfig())

# For batch, labels is directly available for further transformations/writing.
# For streaming, the future application configures a sink and durable checkpoints:
# query = (labels.writeStream.outputMode("append")
#          .format(chosen_sink)
#          .option("checkpointLocation", checkpoint_path)
#          .start())
```

Default rules flag **six payment attempts** or **five payment failures** within
a 60-second window sliding every 10 seconds. These detect the generator's rapid
card testing and large-purchase retry patterns without reading its hidden label.
Thresholds and window durations are configurable. `fraud_reasons` identifies
each triggered rule; `is_fraudulent` is a heuristic label, not ground truth.
No amount anomaly rule is applied without a catalog or user spending baseline.

Output keys are `(user_id, session_id, window_start, window_end)`, with event,
attempt, failure and completion counts and first/last observed event times.
A session can appear in multiple overlapping windows, with different labels.
To derive a session-wide decision from stored results, OR its window labels.
A false label means no rule fired in that window, not that the whole session
has been cleared. There is no session-end event in the input schema.

Duplicates are removed by `event_id` before aggregation, including across
microbatches. IDs must identify immutable events with identical payloads on
retries. Streaming keeps deduplication and aggregation state bounded using a
120-second event-time watermark. Use **append** output mode: window results
appear only when the watermark passes their end. The watermark follows incoming
event times; an idle stream does not finalize windows just because wall time
passes. No source, sink, checkpoint or Spark session is created by this module.

Null/blank event or session IDs, null users, invalid timestamps and unsupported
event types are filtered. Malformed JSON is filtered through these same checks;
retain raw input separately if rejected records are needed. Older-than-watermark
data can be discarded by streaming. Batch uses all supplied valid history, so
it may correct streaming labels when late history is included. For matching
accepted input and configuration, both modes use the same features and rules.
Use a new checkpoint when changing stateful grouping/window configuration.

Run tests with `venv/bin/python -m unittest discover -s tests`.

Spark semantics: [Structured Streaming guide](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html).

## Kafka to ClickHouse application

Run from the repository root using Python 3.11 and Java 17. Install root
requirements first. `main.py` resolves the Kafka connector matching the installed
Spark 3.5 version from Maven on startup; the first run needs network access.

```bash
# Local infrastructure. An explicit empty env file avoids loading unrelated .env content.
docker compose --env-file /dev/null -f docker-compose-kafka.yaml up -d
docker compose --env-file /dev/null -f docker-compose-clickhouse.yaml up -d --wait

export JAVA_HOME=$(/usr/libexec/java_home -v 17)  # macOS; use your Java 17 path elsewhere
unset SPARK_HOME  # use the Spark distribution bundled with the installed PySpark
export PYSPARK_PYTHON="$PWD/venv/bin/python"
export PYSPARK_SUBMIT_ARGS='--master local[2] pyspark-shell'
venv/bin/python -m src.processing.main --checkpoint "$PWD/checkpoints/session-fraud"
```

The application reads the existing `ecommerce-events` Kafka topic, decodes JSON,
calls `label_sessions`, and writes **all labeled windows**, including false
labels, to `ecommerce.session_fraud`. It creates the database/table if missing.
An existing table must have the schema defined in `clickhouse.py`.
No generator is started by the processing application.

Default local ClickHouse credentials are `ecommerce` / `ecommerce_local`.
Override `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` through exported environment
variables for both Compose and the application. The application also accepts
`CLICKHOUSE_URL`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_TABLE`,
`KAFKA_BOOTSTRAP_SERVERS`, and `KAFKA_TOPIC`; it does not load `.env` itself.
Run `venv/bin/python -m src.processing.main --help` for CLI overrides and fraud
thresholds. Local Compose exposes HTTP only on host loopback and persists data
in a named volume. Existing persisted users are not migrated by changing env vars.

Each Spark partition sends bounded JSONEachRow HTTP requests (1,000 rows/request)
directly to ClickHouse. No result set is collected on the driver. Every executor
must reach the ClickHouse URL and Kafka's advertised broker addresses. The local
defaults assume Spark runs on the host; configure routable addresses for a
distributed deployment and ship the `src` package to every worker.
If using an external `SPARK_HOME` instead, its Spark version must match the
installed PySpark version so the Kafka connector matches the running JVM.

Writes are **at least once**: a failed request/task/microbatch is allowed to fail
the query and can be retried using the same checkpoint. Previously accepted
rows may be inserted again. `ReplacingMergeTree` removes rows with the same
session/window key during background merges; use `FINAL` for deduplicated reads:

```sql
SELECT user_id, session_id, max(is_fraudulent) AS is_fraudulent
FROM ecommerce.session_fraud FINAL
GROUP BY user_id, session_id;
```

Kafka offsets are restored from the checkpoint; `--starting-offsets` applies
only to a new checkpoint. Missing retained Kafka offsets fail the query instead
of silently skipping data. Keep a stable durable checkpoint per query; do not
run simultaneous applications against the same checkpoint. Changing rules or
replaying a different history should use a separate output table and checkpoint
to avoid mixing decisions. This table stores finalized window labels, not raw
events or instantaneous alerts; the watermark delay described above still applies.

References: [Spark Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html),
[ClickHouse HTTP interface](https://clickhouse.com/docs/interfaces/http),
[ReplacingMergeTree semantics](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree).
