# Local lakehouse services

Thrift Server keeps downloaded dependencies in dedicated named volumes:
`thriftserver-ivy` for `/tmp/ivy` and `thriftserver-home` for `/home/spark`
(including Hive's `.ivy2` and any local `.m2` cache). The first launch fills
these caches; later restarts and container recreations reuse them. `down`
preserves the caches, while `down -v` deletes them along with the data volumes.
Dependencies already downloaded into an older container are not automatically
copied into these new volumes, so the first launch after this change may
download them once more. Some dependency metadata checks may still use the network.

The shared Spark image runs Python 3.11 and Java 17 with Spark 3.5.7.
After changing the image, rebuild and recreate all Spark services, including
the app runner, so drivers and workers use the same Python version.

The pipeline stores its Delta tables under the shared warehouse:

```text
/lakehouse/warehouse/ecommerce.db/bronze_events
/lakehouse/warehouse/ecommerce.db/payment_events
/lakehouse/warehouse/ecommerce.db/daily_payments
```

Spark, Hive, the app runner, and registration SQL use this warehouse root.
Streaming checkpoints remain separate at `/lakehouse/checkpoints`.

If you previously ran the stack with `/lakehouse/tables`, stop the app before
switching paths. Move each existing table directory to its corresponding location listed above
on the same volume, preserving its Delta logs and the existing checkpoints. Update existing external table definitions to the new
locations (drop and re-register them without deleting the files). The
registration script uses `IF NOT EXISTS`, so it does not relocate existing
catalog entries. Do not reuse old checkpoints against empty new table paths.

From the repository root (start Kafka first because workers share its network):

```bash
docker compose --env-file /dev/null -f docker-compose-kafka.yaml up -d
docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml up -d --build
```

This starts PostgreSQL, a standalone Hive 3.1.3 metastore, Spark 3.5.7 master
and worker, and a Delta 3.3.2 Spark Thrift Server. Allow at least 8 GB of Docker
memory. The worker offers four cores and 4 GB; each application is capped at two
cores so streaming and JDBC queries can run together. First startup downloads
JVM packages from Maven, including the Hive 3.1.3 client. All services target
`linux/arm64` by default for native execution on Apple Silicon. The metastore image copies
the Hive 3.1.3 Java distribution from the upstream AMD64 image into a native
Temurin Java 8 runtime for the selected platform; the source stage executes no commands.
Hadoop uses its portable Java implementation instead of the AMD64 native libraries.

Choose the platform when launching Compose with `LAKEHOUSE_PLATFORM`:

```bash
# Apple Silicon (also the default)
LAKEHOUSE_PLATFORM=linux/arm64 docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml up -d --build

# Intel/AMD
LAKEHOUSE_PLATFORM=linux/amd64 docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml up -d --build
```

Use the same value for subsequent Compose commands. You can also put
`LAKEHOUSE_PLATFORM=linux/arm64` in a dedicated env file and pass it using
`--env-file`. Changing architecture requires rebuilding images and recreating
containers; it cannot change inside an already running container. Named data
volumes are preserved. Running a platform different from your host requires
Docker emulation support.

| Connection | Address |
| --- | --- |
| JDBC (Hive driver) | `jdbc:hive2://localhost:10000/default` |
| Hive metastore | `thrift://localhost:9083` |
| Spark standalone master (host) | `spark://localhost:7077` |
| Spark master UI | <http://localhost:8080> |
| Spark worker UI | <http://localhost:8081> |
| Thrift Server Spark UI | <http://localhost:4040> |

Use the Hive JDBC driver (`org.apache.hive.jdbc.HiveDriver`) with username
`hive` and an empty password. Services use local development credentials and
publish ports only on loopback. The metastore stores table definitions in
PostgreSQL; Delta files and checkpoints live in the shared `lakehouse-data`
volume. Every Spark container sees the same `/lakehouse` paths. This filesystem
layout is suitable for a single Docker host; use shared distributed or object
storage for multiple physical hosts.

## Run the existing streaming application

Start the repository's Kafka stack, then the app:

```bash
docker compose --env-file /dev/null -f docker-compose-kafka.yaml up -d
docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml --profile app up -d lakehouse-app
docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml logs -f lakehouse-app
```

The app driver runs in its own container and connects to
`spark://spark-master:7077`, sharing the metastore and Delta storage with the
Thrift Server. Its executors load source from the read-only `src` mount. Kafka
uses the brokers' internal advertised addresses. If Kafka was started under
a different Compose project name, set `LAKEHOUSE_KAFKA_NETWORK` to its actual
network name (inspect with `docker network ls`). Workers also join this network
so executors can reach Kafka. Start the event generator on the host as usual.

After the app logs show that the tables have initialized, register their paths
once in the metastore:

```bash
docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml exec spark-thriftserver \
  /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive \
  -f /workspace/register-tables.sql
```

Then connect a JDBC tool to `jdbc:hive2://localhost:10000/ecommerce` and run:

```sql
SHOW TABLES;
SELECT * FROM daily_payments ORDER BY event_date DESC, payment_method;
```

The application writes Delta paths directly, so registration is a separate
step. These external table definitions point to the same live directories;
there is no data copy. The SQL script is safe to repeat. Existing host-local
lakehouse directories are not automatically imported into the Docker volume.

## Other applications

Use the supplied Spark image and configuration for other app drivers, with
the same storage/source mounts and network. Inside the Compose network the
master is `spark://spark-master:7077` and the metastore is
`thrift://hive-metastore:9083`. Give each driver a hostname reachable by the
workers using `spark.driver.host` and bind it with
`spark.driver.bindAddress=0.0.0.0`. The app profile demonstrates this setup.

For a Python application launched from the host IDE, setting only
`.master("spark://localhost:7077")` is insufficient: workers must reach the
driver's RPC and block-manager ports, Python versions must match, and paths
must resolve identically on host and containers. The container app runner
avoids those differences. It still runs the source edited in the IDE.

## Verify and stop

```bash
docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml ps
docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml exec spark-thriftserver \
  /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -e 'SELECT 1; SHOW DATABASES;'
docker compose --env-file /dev/null -f docker-compose-lakehouse.yaml --profile app down
```

Stopping preserves named volumes. Removing volumes with `down -v` deletes
the metastore, Delta data, and streaming checkpoints.

Configuration references: [Spark Hive integration](https://spark.apache.org/docs/3.5.6/sql-data-sources-hive-tables.html),
[Spark Thrift Server](https://spark.incubator.apache.org/docs/3.5.7/sql-distributed-sql-engine.html),
and [Hive Docker setup](https://hive.apache.org/docs/latest/admin/setting-up-hive-with-docker/).
