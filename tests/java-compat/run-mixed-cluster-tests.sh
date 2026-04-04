#!/bin/bash
#
# Mixed-cluster integration test: Java bookie + Rust bookie in one cluster.
# Starts embedded ZK, initializes cluster, starts one Java bookie and one Rust bookie,
# then runs the MixedClusterTest suite with ensemble_size=2.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BK_ROOT="/Users/lipenghui/Github/apache/bookkeeper"

ZK_PID=""
JAVA_BOOKIE_PID=""
RUST_BOOKIE_PID=""
JAVA_BOOKIE_DIR=""
RUST_BOOKIE_DIR=""

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    [ -n "$RUST_BOOKIE_PID" ] && kill "$RUST_BOOKIE_PID" 2>/dev/null && echo "Stopped Rust bookie (PID $RUST_BOOKIE_PID)" || true
    [ -n "$JAVA_BOOKIE_PID" ] && kill "$JAVA_BOOKIE_PID" 2>/dev/null && echo "Stopped Java bookie (PID $JAVA_BOOKIE_PID)" || true
    [ -n "$ZK_PID" ] && kill "$ZK_PID" 2>/dev/null && echo "Stopped embedded ZK (PID $ZK_PID)" || true
    [ -n "$JAVA_BOOKIE_DIR" ] && rm -rf "$JAVA_BOOKIE_DIR" 2>/dev/null || true
    [ -n "$RUST_BOOKIE_DIR" ] && rm -rf "$RUST_BOOKIE_DIR" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# Step 1: Build Java BookKeeper if needed
echo "=== Step 1: Check Java BookKeeper build ==="
if [ ! -f "$BK_ROOT/bookkeeper-server/target/bookkeeper-server-4.18.0-SNAPSHOT.jar" ]; then
    echo "Building BookKeeper Java (this may take a few minutes)..."
    cd "$BK_ROOT"
    mvn install -DskipTests -pl bookkeeper-server -am -q
    cd "$PROJECT_ROOT"
else
    echo "BookKeeper Java already built."
fi

# Step 2: Build Rust bookie
echo "=== Step 2: Build Rust bookie ==="
cd "$PROJECT_ROOT"
cargo build -p bookkeeper-cli -p bookkeeper-server 2>&1 | tail -3
echo "Rust bookie built."

# Step 3: Compile Java test project
echo "=== Step 3: Compile Java test project ==="
cd "$SCRIPT_DIR"
mvn compile test-compile -q
echo "Java test project compiled."

# Step 4: Start embedded ZooKeeper
echo "=== Step 4: Start embedded ZooKeeper ==="
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout 2>/dev/null)
CLASSPATH="$CLASSPATH:target/classes:target/test-classes"

java -cp "$CLASSPATH" org.apache.bookkeeper.compat.EmbeddedZkServer > /tmp/bk-zk-output.txt 2>&1 &
ZK_PID=$!

for i in $(seq 1 30); do
    if grep -q "ZK_PORT:" /tmp/bk-zk-output.txt 2>/dev/null; then
        ZK_PORT=$(grep "ZK_PORT:" /tmp/bk-zk-output.txt | head -1 | cut -d: -f2)
        break
    fi
    if ! kill -0 "$ZK_PID" 2>/dev/null; then
        echo "ERROR: Embedded ZK died during startup"
        cat /tmp/bk-zk-output.txt
        exit 1
    fi
    sleep 1
done
echo "Embedded ZK running on port $ZK_PORT (PID $ZK_PID)"

# Step 5: Initialize BookKeeper cluster
echo "=== Step 5: Initialize BookKeeper cluster ==="
cd "$PROJECT_ROOT"
./target/debug/bk initnewcluster --zk "localhost:$ZK_PORT" --ledger-root /ledgers 2>&1 || echo "(cluster may already be initialized)"

# Step 6: Start Java bookie
echo "=== Step 6: Start Java bookie ==="
JAVA_BOOKIE_DIR=$(mktemp -d)
JAVA_BOOKIE_CONF="$JAVA_BOOKIE_DIR/bk_server.conf"

cat > "$JAVA_BOOKIE_CONF" <<CONF
bookiePort=0
allowEphemeralPorts=true
allowLoopback=true
advertisedAddress=127.0.0.1
zkServers=localhost:$ZK_PORT
zkLedgersRootPath=/ledgers
journalDirectory=$JAVA_BOOKIE_DIR/journal
ledgerDirectories=$JAVA_BOOKIE_DIR/ledger
indexDirectories=$JAVA_BOOKIE_DIR/index
autoRecoveryDaemonEnabled=false
httpServerEnabled=false
diskUsageThreshold=0.99
journalSyncData=false
CONF

mkdir -p "$JAVA_BOOKIE_DIR/journal" "$JAVA_BOOKIE_DIR/ledger" "$JAVA_BOOKIE_DIR/index"

java -cp "$CLASSPATH" org.apache.bookkeeper.server.Main \
    --conf "$JAVA_BOOKIE_CONF" \
    > /tmp/bk-java-bookie.txt 2>&1 &
JAVA_BOOKIE_PID=$!

# Wait for Java bookie to register
sleep 5
if ! kill -0 "$JAVA_BOOKIE_PID" 2>/dev/null; then
    echo "ERROR: Java bookie died during startup"
    cat /tmp/bk-java-bookie.txt
    exit 1
fi
echo "Java bookie running (PID $JAVA_BOOKIE_PID)"

# Step 7: Start Rust bookie
echo "=== Step 7: Start Rust bookie ==="
RUST_BOOKIE_DIR=$(mktemp -d)
./target/debug/bk serve --port 0 --data-dir "$RUST_BOOKIE_DIR" --zk "localhost:$ZK_PORT" --ledger-root /ledgers > /tmp/bk-rust-bookie.txt 2>&1 &
RUST_BOOKIE_PID=$!

sleep 3
if ! kill -0 "$RUST_BOOKIE_PID" 2>/dev/null; then
    echo "ERROR: Rust bookie died during startup"
    cat /tmp/bk-rust-bookie.txt
    exit 1
fi
echo "Rust bookie running (PID $RUST_BOOKIE_PID)"
cat /tmp/bk-rust-bookie.txt

# Step 8: Run mixed cluster tests
echo ""
echo "=== Step 8: Run mixed cluster integration tests ==="
cd "$SCRIPT_DIR"
mvn test -Dzk.connect="localhost:$ZK_PORT" -Dtest=MixedClusterTest 2>&1
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "==========================================="
    echo "  ALL MIXED CLUSTER TESTS PASSED"
    echo "==========================================="
else
    echo "==========================================="
    echo "  MIXED CLUSTER TESTS FAILED"
    echo "==========================================="
    echo ""
    echo "Java bookie log:"
    tail -30 /tmp/bk-java-bookie.txt
    echo ""
    echo "Rust bookie log:"
    tail -30 /tmp/bk-rust-bookie.txt
fi

exit $EXIT_CODE
