#!/bin/bash
# Generate SAHI vs YOLO comparison screenshots

cd "$(dirname "$0")/.." || exit
source venv/bin/activate
python3 src/generate_comparison_screenshots.py
