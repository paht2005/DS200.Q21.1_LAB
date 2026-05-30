#!/bin/bash
# Run the frame receiver server
# Usage: ./run_receiver.sh

cd "$(dirname "$0")/.."
source venv/bin/activate 2>/dev/null || true

echo "Starting Receiver Server..."
python src/receiver.py "$@"
