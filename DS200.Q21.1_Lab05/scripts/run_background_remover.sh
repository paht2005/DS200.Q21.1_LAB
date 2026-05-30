#!/bin/bash
# Run background remover utility
# Usage: ./run_background_remover.sh --input IMAGE [--output OUTPUT] [--color green|blue|white|transparent]

cd "$(dirname "$0")/.."
source venv/bin/activate 2>/dev/null || true

python src/background_remover.py "$@"
