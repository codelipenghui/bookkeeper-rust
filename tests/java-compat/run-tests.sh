#!/bin/bash
# Java Compatibility Tests — Rust Bookie
#
# Prerequisites:
# 1. Java BookKeeper built (provides bookkeeper-server 4.18.0-SNAPSHOT to local Maven repo):
#      cd /Users/lipenghui/Github/apache/bookkeeper
#      mvn install -DskipTests -pl bookkeeper-server -am
#
# 2. ZooKeeper running at localhost:2181 (or the address you pass as $1)
#
# 3. BookKeeper cluster metadata initialized:
#      target/debug/bk initnewcluster --zk localhost:2181 --ledger-root /ledgers
#    (safe to run again if already initialized — the command exits non-zero but is harmless)
#
# 4. Rust bookie started and registered with ZooKeeper, e.g.:
#      target/debug/bookkeeper-server --zk-connect localhost:2181 &
#
# Usage:
#   ./run-tests.sh [zk-connect-string]
#
# Examples:
#   ./run-tests.sh                     # uses localhost:2181
#   ./run-tests.sh localhost:2181      # explicit default
#   ./run-tests.sh 127.0.0.1:2181     # explicit IP

set -euo pipefail

ZK_CONNECT="${1:-localhost:2181}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Running Java compatibility tests against ZK at $ZK_CONNECT"
cd "$SCRIPT_DIR"
mvn test -Dzk.connect="$ZK_CONNECT"
