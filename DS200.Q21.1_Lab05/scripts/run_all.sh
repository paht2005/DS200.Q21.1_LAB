#!/bin/bash
# Run all servers in the correct order
# Usage: ./run_all.sh

cd "$(dirname "$0")/.."
source venv/bin/activate 2>/dev/null || true

echo "=========================================="
echo "  Person Counting System - Full Pipeline"
echo "=========================================="
echo ""
echo "Starting all servers..."
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Stopping all servers..."
    pkill -f "storage_server.py" 2>/dev/null
    pkill -f "detect_object.py" 2>/dev/null
    pkill -f "receiver.py" 2>/dev/null
    echo "Done."
    exit 0
}

trap cleanup SIGINT SIGTERM

# Start Storage Server
echo "[1/3] Starting Storage Server (port 6300)..."
python src/storage_server.py &
STORAGE_PID=$!
sleep 2

# Start Detector Server
echo "[2/3] Starting Detector Server (port 6200)..."
python src/detect_object.py &
DETECTOR_PID=$!
sleep 2

# Start Receiver Server
echo "[3/3] Starting Receiver Server (port 6100)..."
python src/receiver.py &
RECEIVER_PID=$!
sleep 2

echo ""
echo "=========================================="
echo "  All servers running!"
echo "=========================================="
echo ""
echo "  Storage:  localhost:6300 (PID: $STORAGE_PID)"
echo "  Detector: localhost:6200 (PID: $DETECTOR_PID)"
echo "  Receiver: localhost:6100 (PID: $RECEIVER_PID)"
echo ""
echo "Now run the sender in another terminal:"
echo "  python src/sender.py --frames 10"
echo ""
echo "Or with a video file:"
echo "  python src/sender.py --video data/video/sample.mp4"
echo ""
echo "Press Ctrl+C to stop all servers"
echo ""

# Wait for all processes
wait $STORAGE_PID $DETECTOR_PID $RECEIVER_PID
