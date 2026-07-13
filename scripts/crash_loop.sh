#!/usr/bin/env bash
# Crash harness — run `salamander-demo crashtest parent` N times against a
# scratch directory, tally pass/fail, exit non-zero on any INV-1 violation.
set -euo pipefail

ITERATIONS="${1:-1000}"
BIN="${BIN:-./target/release/salamander-demo}"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

pass=0
fail=0

for i in $(seq 1 "$ITERATIONS"); do
  dir="$WORKDIR/run_$i"
  mkdir -p "$dir"
  if "$BIN" crashtest parent "$dir"; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    echo "iteration $i FAILED (dir kept for inspection: $dir)"
  fi
done

echo "$ITERATIONS crashes, $fail violation(s), $pass clean"
[ "$fail" -eq 0 ]
