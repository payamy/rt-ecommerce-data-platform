#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
root="$PWD"
state="$root/.run"
python="${PYTHON:-$root/venv/bin/python}"
mkdir -p "$state"

# A PID alone can be reused. Check the module name before signaling a process.
running() {
    local name="$1" pid
    [[ -f "$state/$name.pid" ]] || return 1
    read -r pid < "$state/$name.pid"
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    ps -p "$pid" -o command= | grep -Fq -- "-m src.$name.main"
}

case "${1:-}" in
    start)
        name="${2:?Specify generator or processing}"
        [[ "$name" == generator || "$name" == processing ]] || exit 2
        if running "$name"; then
            echo "$name already running (PID $(cat "$state/$name.pid"))"
            exit 0
        fi
        [[ -x "$python" ]] || { echo "Install requirements into venv first (or set PYTHON)." >&2; exit 1; }
        args=("$python" -u -m "src.$name.main")
        if [[ "$name" == processing ]]; then
            if [[ -z "${JAVA_HOME:-}" && -x /usr/libexec/java_home ]]; then
                JAVA_HOME="$(/usr/libexec/java_home -v 17)"
                export JAVA_HOME
            fi
            unset SPARK_HOME
            export PYSPARK_PYTHON="$python"
            export PYSPARK_SUBMIT_ARGS='--master local[2] pyspark-shell'
            args+=(--checkpoints "$root/checkpoints/session-fraud")
        fi
        nohup "${args[@]}" > "$state/$name.log" 2>&1 < /dev/null &
        echo "$!" > "$state/$name.pid"
        sleep 2
        running "$name" || { tail -n 40 "$state/$name.log"; exit 1; }
        echo "$name started; log: $state/$name.log"
        ;;
    stop)
        name="${2:?Specify generator or processing}"
        [[ "$name" == generator || "$name" == processing ]] || exit 2
        if running "$name"; then
            kill -TERM "$(cat "$state/$name.pid")"
            for ((attempt=0; attempt<60; attempt++)); do
                running "$name" || break
                sleep 1
            done
            if running "$name"; then
                echo "$name is still stopping; see $state/$name.log" >&2
                exit 1
            fi
        fi
        echo "$name stopped"
        ;;
    status)
        for name in generator processing; do
            if running "$name"; then echo "$name: running"; else echo "$name: stopped"; fi
        done
        ;;
    logs)
        for name in generator processing; do
            echo "=== $name ==="
            [[ ! -f "$state/$name.log" ]] || tail -n 20 "$state/$name.log"
        done
        ;;
    wait-kafka|wait-tables)
        timeout="${2:-900}"
        [[ "$timeout" =~ ^[1-9][0-9]*$ ]] || { echo 'Timeout must be a positive integer.' >&2; exit 2; }
        deadline=$((SECONDS + timeout))
        if [[ "$1" == wait-kafka ]]; then
            compose=(docker compose --env-file /dev/null -p rt-ecommerce-data-platform -f docker-compose-kafka.yaml)
            # Broker API requests must succeed against all three brokers.
            probe=(exec -T kafka-1 bash -c 'for broker in kafka-1 kafka-2 kafka-3; do kafka-broker-api-versions --bootstrap-server "$broker:19092" >/dev/null 2>&1 || exit 1; done')
        else
            compose=(docker compose --env-file /dev/null -p rt-ecommerce-data-platform -f docker-compose-lakehouse.yaml)
            probe=(exec -T lakehouse-app python3 -c 'from pathlib import Path; import sys; sys.exit(not all(any((Path("/lakehouse/warehouse/ecommerce.db") / p / "_delta_log").glob("[0-9]*.json")) for p in ("bronze_events", "payment_events", "daily_payments")))')
        fi
        until "${compose[@]}" "${probe[@]}" >/dev/null 2>&1; do
            if ((SECONDS >= deadline)); then
                echo "$1 timed out after ${timeout}s. Check make status and make logs." >&2
                exit 1
            fi
            sleep 5
        done
        echo "$1: ready"
        ;;
    *) echo 'Usage: apps.sh start|stop generator|processing; status; logs; wait-kafka|wait-tables [seconds]' >&2; exit 2 ;;
esac
