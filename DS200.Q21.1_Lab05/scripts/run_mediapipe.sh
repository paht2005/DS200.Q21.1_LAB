#!/bin/bash
# Run MediaPipe exploration tool
# Usage: ./run_mediapipe.sh --input IMAGE [--mode pose|face|hands|all]

cd "$(dirname "$0")/.."
source venv/bin/activate 2>/dev/null || true

python src/examine_mediapipe.py "$@"
