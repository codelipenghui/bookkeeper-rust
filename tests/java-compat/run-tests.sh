#!/bin/bash
#
# Fully automated Java client compatibility test runner.
# Starts embedded ZK, initializes cluster, starts Rust bookie, runs Java tests.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BK_ROOT="/Users/lipenghui/Github/apache/bookkeeper"

ZK_PID=""
BOOKIE_PID=""
BOOKIE_DIR=""

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    [ -n "$BOOKIE_PID" ] && kill "$BOOKIE_PID" 2>/dev/null && echo "Stopped Rust bookie (PID $BOOKIE_PID)" || true
    [ -n "$ZK_PID" ] && kill "$ZK_PID" 2>/dev/null && echo "Stopped embedded ZK (PID $ZK_PID)" || true
    [ -n "$BOOKIE_DIR" ] && rm -rf "$BOOKIE_DIR" 2>/dev/null || true
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

# Step 3: Build and compile Java test project
echo "=== Step 3: Compile Java test project ==="
cd "$SCRIPT_DIR"
mvn compile -q
echo "Java test project compiled."

# Step 4: Start embedded ZooKeeper
echo "=== Step 4: Start embedded ZooKeeper ==="
ZK_PORT=2181
CLASSPATH=$(mvn dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout 2>/dev/null)
CLASSPATH="$CLASSPATH:target/classes"

java -cp "$CLASSPATH" org.apache.bookkeeper.compat.EmbeddedZkServer > /tmp/bk-zk-output.txt 2>&1 &
ZK_PID=$!

# Wait for ZK to start
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

# Step 6: Start Rust bookie with ZK registration
echo "=== Step 6: Start Rust bookie ==="
BOOKIE_DIR=$(mktemp -d)
./target/debug/bk serve --port 0 --data-dir "$BOOKIE_DIR" --zk "localhost:$ZK_PORT" --ledger-root /ledgers > /tmp/bk-bookie-output.txt 2>&1 &
BOOKIE_PID=$!

# Wait for bookie to start and register
sleep 3
if ! kill -0 "$BOOKIE_PID" 2>/dev/null; then
    echo "ERROR: Rust bookie died during startup"
    cat /tmp/bk-bookie-output.txt
    exit 1
fi
echo "Rust bookie running (PID $BOOKIE_PID)"
cat /tmp/bk-bookie-output.txt

# Step 7: Run Java integration tests
echo ""
echo "=== Step 7: Run Java integration tests ==="
cd "$SCRIPT_DIR"
mvn test -Dzk.connect="localhost:$ZK_PORT" 2>&1
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "==========================================="
    echo "  ALL JAVA COMPATIBILITY TESTS PASSED"
    echo "==========================================="
else
    echo "==========================================="
    echo "  JAVA COMPATIBILITY TESTS FAILED"
    echo "==========================================="
fi

exit $EXIT_CODE
