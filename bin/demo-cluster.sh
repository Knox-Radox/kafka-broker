#!/usr/bin/env bash
# PRD §9.5: the literal "one command" a stranger — a hiring manager
# skimming this repo, not a contributor — should be able to run, cold, on
# a clean checkout, to get a real, working, replicated 3-broker cluster
# and be told exactly what to do with it. See docs/ARCHITECTURE.md for
# the reader-facing writeup this cluster demonstrates.
#
# Usage:
#   bin/demo-cluster.sh start   (default if no argument given)
#   bin/demo-cluster.sh stop
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DATA_DIR="data/demo"
PID_FILE="$DATA_DIR/pids"
BROKER_IDS=(0 1 2)
PORTS=(9092 9093 9094)
METRICS_PORTS=(9404 9405 9406)

action="${1:-start}"

stop_cluster() {
    if [[ -f "$PID_FILE" ]]; then
        echo "Stopping demo cluster..."
        while read -r pid; do
            if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
                echo "  stopped pid $pid"
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    else
        echo "No running demo cluster found (no $PID_FILE)."
    fi
}

wait_for_port() {
    local port="$1"
    local deadline=$((SECONDS + 20))
    while (( SECONDS < deadline )); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            exec 3>&- 3<&-
            return 0
        fi
        sleep 0.2
    done
    echo "ERROR: broker on port $port never became reachable within 20s -- check $DATA_DIR/broker-*.log" >&2
    return 1
}

case "$action" in
    stop)
        stop_cluster
        exit 0
        ;;
    start)
        ;;
    *)
        echo "usage: $0 [start|stop]" >&2
        exit 1
        ;;
esac

if [[ -f "$PID_FILE" ]]; then
    echo "A demo cluster already appears to be running (found $PID_FILE)."
    echo "Run 'bin/demo-cluster.sh stop' first if you want a fresh one."
    exit 1
fi

if [[ ! -f target/kafka-broker.jar ]]; then
    echo "Building target/kafka-broker.jar (mvn -DskipTests package)..."
    mvn -q -DskipTests package
fi

echo "Starting a fresh 3-broker, replication-factor-3 cluster..."
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
: > "$PID_FILE"

for id in "${BROKER_IDS[@]}"; do
    log_file="$DATA_DIR/broker-$id.log"
    nohup java -jar target/kafka-broker.jar "config/demo/broker-$id.properties" > "$log_file" 2>&1 &
    pid=$!
    echo "$pid" >> "$PID_FILE"
    echo "  broker $id started (pid $pid), listening on port ${PORTS[$id]}, log: $log_file"
done

echo "Waiting for all brokers to be reachable..."
for port in "${PORTS[@]}"; do
    wait_for_port "$port"
done

echo ""
echo "=========================================================================="
echo " Demo cluster is up: 3 brokers, replication factor 3, topic 'demo' (3 partitions)"
echo "=========================================================================="
echo ""
echo "Bootstrap servers: 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"
echo ""
echo "Try it with a real client (kafka-python, pip install kafka-python):"
echo ""
echo "  python3 -c '"
echo "from kafka import KafkaProducer, KafkaConsumer"
echo "p = KafkaProducer(bootstrap_servers=\"127.0.0.1:9092\", acks=\"all\", enable_idempotence=False)"
echo "p.send(\"demo\", b\"hello from the demo cluster\").get(timeout=5)"
echo "p.flush()"
echo "c = KafkaConsumer(\"demo\", bootstrap_servers=\"127.0.0.1:9092\", auto_offset_reset=\"earliest\", consumer_timeout_ms=3000)"
echo "for msg in c: print(msg.value)"
echo "'"
echo ""
echo "Or a real Kafka distribution's own console tools, pointed at the same bootstrap servers"
echo "(remember --producer-property enable.idempotence=false -- see STUDY_GUIDE.md's Bug 2)."
echo ""
echo "Watch it live:"
echo "  curl http://127.0.0.1:9404/metrics   # broker 0's Prometheus endpoint (9405/9406 for brokers 1/2)"
echo ""
echo "Try killing the leader and watching failover (finds the current leader's pid via lsof):"
echo "  lsof -ti:9092 | xargs kill -9   # or 9093 / 9094, whichever Metadata currently reports as leader"
echo ""
echo "Stop the cluster:"
echo "  bin/demo-cluster.sh stop"
echo ""
