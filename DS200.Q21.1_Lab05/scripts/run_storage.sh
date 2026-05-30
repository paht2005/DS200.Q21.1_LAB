#!/bin/bash
# Run the storage server
# Usage: ./run_storage.sh

cd "$(dirname "$0")/.."
source venv/bin/activate 2>/dev/null || true

echo "Starting Storage Server..."
python src/storage_server.py "$@"
