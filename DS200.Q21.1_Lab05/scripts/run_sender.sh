#!/bin/bash
# Run the frame sender
# Usage: ./run_sender.sh [--video PATH] [--frames N] [--fps N]

cd "$(dirname "$0")/.."
source venv/bin/activate 2>/dev/null || true

echo "Starting Sender..."
python src/sender.py "$@"
