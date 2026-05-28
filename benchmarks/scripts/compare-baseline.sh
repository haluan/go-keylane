#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Haluan Irsad
# SPDX-License-Identifier: AGPL-3.0-only
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <old-bench.txt> <new-bench.txt>" >&2
  echo "requires benchstat: go install golang.org/x/perf/cmd/benchstat@latest" >&2
  exit 2
fi

if ! command -v benchstat >/dev/null 2>&1; then
  echo "benchstat not found; install with: go install golang.org/x/perf/cmd/benchstat@latest" >&2
  exit 1
fi

OLD="$1"
NEW="$2"
benchstat "${OLD}" "${NEW}"
