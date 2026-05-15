#!/usr/bin/env bash
# Convenience wrapper — delegates to run_java_dataframe_local.sh.
set -euo pipefail
exec "$(dirname "$0")/run_java_dataframe_local.sh" "$@"
