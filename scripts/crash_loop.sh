#!/usr/bin/env bash
# Crash harness — rotate N real process kills across append, batch, snapshot,
# and healing scenarios. Exit non-zero on any durable-truth violation.
set -euo pipefail

ITERATIONS="${1:-1000}"
BIN="${BIN:-./target/release/salamander-demo}"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

pass=0
fail=0
scenarios=(append batch snapshot heal)

for i in $(seq 1 "$ITERATIONS"); do
  dir="$WORKDIR/run_$i"
  scenario="${scenarios[$(( (i - 1) % ${#scenarios[@]} ))]}"
  mkdir -p "$dir"
  if "$BIN" crashtest parent "$dir" "$scenario"; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    echo "iteration $i ($scenario) FAILED at $dir"
  fi
done

echo "$ITERATIONS process crashes across ${scenarios[*]}, $fail violation(s), $pass clean"
[ "$fail" -eq 0 ]
