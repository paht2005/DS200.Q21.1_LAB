#!/usr/bin/env bash
# Convenience alias requested by the assignment workflow.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
"$ROOT/run_java_rdd_local.sh"
