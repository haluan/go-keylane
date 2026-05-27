#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Haluan Irsad
# SPDX-License-Identifier: AGPL-3.0-only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VERSION="${VERSION:-v0.8.0}"
COUNT="${COUNT:-10}"
OUT_DIR="${OUT_DIR:-/tmp}"
BENCH_TXT="${OUT_DIR}/go-keylane-bench-${VERSION}.txt"
GUARD_TXT="${OUT_DIR}/go-keylane-bench-guardrails-${VERSION}.txt"
JSON_OUT="${ROOT}/benchmarks/baselines/${VERSION}.json"

mkdir -p "$OUT_DIR"
cd "$ROOT"

COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
GO_VERSION="$(go version | awk '{print $3}')"

echo "Capturing production suite (count=${COUNT}) -> ${BENCH_TXT}"
{
  echo "# go-keylane benchmark capture"
  echo "# version=${VERSION} commit=${COMMIT} go=${GO_VERSION}"
  echo "# date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# GOMAXPROCS=${GOMAXPROCS:-$(go env GOMAXPROCS 2>/dev/null || echo default)}"
  go test ./benchmarks -run '^$' -bench='Keylane|Fairness|GCPressure' -benchmem -count="${COUNT}"
} 2>&1 | tee "${BENCH_TXT}"

echo "Capturing root guardrails -> ${GUARD_TXT}"
{
  echo "# go-keylane guardrail benchmarks"
  go test . -run '^$' -bench='BenchmarkSubmitHotPathAllocGuardrail|BenchmarkPipelineSingleStage|BenchmarkBackendAcquireReleaseDisabled' -benchmem -count="${COUNT}"
} 2>&1 | tee "${GUARD_TXT}"

MERGED="${OUT_DIR}/go-keylane-bench-merged-${VERSION}.txt"
cat "${BENCH_TXT}" "${GUARD_TXT}" > "${MERGED}"

echo "Writing structured baseline -> ${JSON_OUT}"
go run ./benchmarks/scripts/parsebench \
  -in "${MERGED}" \
  -out "${JSON_OUT}" \
  -version "${VERSION}" \
  -go "${GO_VERSION}" \
  -commit "${COMMIT}"

echo "Done. Compare with:"
echo "  ./benchmarks/scripts/compare-baseline.sh <old.txt> <new.txt>"
