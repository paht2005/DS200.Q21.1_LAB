#!/bin/bash
# Run the object detector server
# Usage: ./run_detector.sh [--spark]

cd "$(dirname "$0")/.."
source venv/bin/activate 2>/dev/null || true

echo "Starting Detector Server..."
python src/detect_object.py "$@"
