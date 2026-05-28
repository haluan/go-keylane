#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Haluan Irsad
# SPDX-License-Identifier: AGPL-3.0-only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ok=0
fail=0

build_example() {
  local pkg="$1"
  local label="$2"
  if go build -o /dev/null "$pkg"; then
    echo "ok   $label"
    ok=$((ok + 1))
  else
    echo "FAIL $label"
    fail=$((fail + 1))
    return 1
  fi
}

echo "Verifying root-module examples..."
for main in examples/*/main.go; do
  dir="$(dirname "$main")"
  name="$(basename "$dir")"
  case "$name" in
    prometheus|otel_hooks)
      continue
      ;;
  esac
  build_example "./examples/$name" "./examples/$name" || true
done

echo "Verifying adapter examples (separate go.mod)..."
( cd examples/prometheus && go build -o /dev/null . ) && { echo "ok   ./examples/prometheus"; ok=$((ok+1)); } || { echo "FAIL ./examples/prometheus"; fail=$((fail+1)); }
( cd examples/otel_hooks && go build -o /dev/null . ) && { echo "ok   ./examples/otel_hooks"; ok=$((ok+1)); } || { echo "FAIL ./examples/otel_hooks"; fail=$((fail+1)); }

echo "---"
echo "passed=$ok failed=$fail"
if [[ "$fail" -gt 0 ]]; then
  exit 1
fi
