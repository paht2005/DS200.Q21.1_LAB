#!/bin/bash
# Convenience wrapper for run_java_streaming_local.sh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$SCRIPT_DIR/run_java_streaming_local.sh" "$@"
