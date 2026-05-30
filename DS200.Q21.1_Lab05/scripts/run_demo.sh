#!/bin/bash
# Run the end-to-end demo
# Usage: ./run_demo.sh [--frames N] [--video PATH]

cd "$(dirname "$0")/.."
source venv/bin/activate 2>/dev/null || true

echo "Running Demo..."
python src/demo-example.py "$@"
