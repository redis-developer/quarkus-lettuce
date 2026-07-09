#!/usr/bin/env bash
#
# Builds the demo twice (vertx, lettuce) and launches both on :8080 / :8081.
# Run from the repository root. Requires a Redis listening on localhost:6379 and
# the Lettuce-backed redis-client jars already installed in the local Maven repo
# (mvn install -pl extensions/redis-client/runtime,extensions/redis-client/deployment -DskipTests).
#
# Usage:
#   ./run-demo.sh             # JVM mode (default, ~30s build)
#   ./run-demo.sh --native    # Native mode (requires GRAALVM_HOME, ~6-10min build)

set -euo pipefail

MODE="jvm"
if [[ "${1:-}" == "--native" ]]; then
  MODE="native"
  if [[ -z "${GRAALVM_HOME:-}" ]]; then
    echo "ERROR: --native requires GRAALVM_HOME to point at a Mandrel/GraalVM install." >&2
    exit 1
  fi
fi

MODULE_DIR="integration-tests/redis-client-lettuce-demo"
TARGET="$MODULE_DIR/target"

if ! command -v redis-cli >/dev/null 2>&1 || ! redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
  echo "WARN: redis-cli ping on localhost:6379 failed — make sure Redis is running before opening the UI."
fi

build() {
  local backend="$1"
  echo "==> Packaging quarkus.redis.backend=$backend ($MODE)"
  if [[ "$MODE" == "native" ]]; then
    mvn -q -f "$MODULE_DIR/pom.xml" package -Pnative -DskipTests -Dquarkus.build.skip=false \
        -DskipDocs -Dformat.skip -Dquarkus.redis.backend="$backend"
    rm -f "$TARGET/runner-$backend"
    mv "$TARGET"/quarkus-integration-test-*-runner "$TARGET/runner-$backend"
  else
    mvn -q -f "$MODULE_DIR/pom.xml" package -DskipTests -Dquarkus.build.skip=false \
        -DskipDocs -Dformat.skip -Dquarkus.redis.backend="$backend"
    rm -rf "$TARGET/quarkus-app-$backend"
    mv "$TARGET/quarkus-app" "$TARGET/quarkus-app-$backend"
  fi
}

build vertx
build lettuce

launch() {
  local backend="$1" port="$2"
  if [[ "$MODE" == "native" ]]; then
    "$TARGET/runner-$backend" -Dquarkus.http.port="$port" -Ddemo.backend="$backend" &
  else
    java -Dquarkus.http.port="$port" -Ddemo.backend="$backend" \
        -jar "$TARGET/quarkus-app-$backend/quarkus-run.jar" &
  fi
}

launch vertx 8080
PID_V=$!
launch lettuce 8081
PID_L=$!

cleanup() {
  echo
  echo "==> Stopping demo processes"
  kill "$PID_V" "$PID_L" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

cat <<EOF

  Demo running:
    Vert.x   → http://localhost:8080
    Lettuce  → http://localhost:8081

  Press Ctrl-C to stop both.
EOF

wait
