# DS200.Q21.1 Lab05 — Real-Time Person Counting System

> **Student ID**: 23521143  
> **Course**: DS200.Q21.1 - Big Data  
> **Lab**: 05 - Distributed Stream Processing with Spark Streaming

---

## Overview

A distributed real-time person counting system that processes camera frames using a three-tier server architecture. The system demonstrates big data stream processing concepts with **PySpark Streaming** for scalable frame processing and **YOLO (You Only Look Once)** deep learning model for accurate person detection.

### Features

- **Frame Sender** — Captures and sends video frames via TCP
- **Frame Receiver** — Receives and forwards frames to detector
- **Object Detector** — YOLO-based person detection with bounding boxes
- **Storage Server** — Persists detection results to JSON
- **Background Remover** — Remove background from images using MediaPipe
- **MediaPipe Explorer** — Explore pose, face, and hand detection

---

## Architecture

```
┌─────────────┐     TCP      ┌──────────────┐     TCP      ┌──────────────┐
│   Video/    │ ──────────► │   Receiver   │ ──────────► │  Detector    │
│   Camera    │   frames    │  (port 6100) │   frames    │  (port 6200) │
│  (sender)   │             └──────────────┘             └──────────────┘
└─────────────┘                                                 │
                                                                │ detection
                                                                │ results
                                                                ▼
                                                         ┌──────────────┐
                                                         │   Storage    │
                                                         │  (port 6300) │
                                                         └──────────────┘
                                                                │
                                                                ▼
                                                    output/detections.json
```

---

## Technologies

| Technology | Purpose |
|------------|---------|
| **Python 3.8+** | Primary language |
| **PySpark Streaming** | Big data stream processing |
| **Ultralytics YOLO** | Person detection model |
| **OpenCV** | Image processing |
| **MediaPipe** | Pose/face/hand detection |
| **TCP Sockets** | Inter-server communication |

---

## Project Structure

```
DS200.Q21.1_Lab05/
├── README.md                    ← This documentation
├── 23521143.txt                 ← Student ID
├── requirements.txt             ← Python dependencies
├── tcp_example.py               ← TCP connection example
│
├── src/                         ← Python source code
│   ├── config.py                ← Configuration settings
│   ├── sender.py                ← Frame sender (client)
│   ├── receiver.py              ← Frame receiver server
│   ├── detect_object.py         ← Object detection server (YOLO)
│   ├── storage_server.py        ← Result storage server
│   ├── background_remover.py    ← Background removal utility
│   ├── examine_mediapipe.py     ← MediaPipe exploration tool
│   └── demo-example.py          ← End-to-end demo script
│
├── scripts/                     ← Shell scripts for quick execution
│   ├── run_all.sh               ← Start all servers
│   ├── run_sender.sh            ← Start sender
│   ├── run_receiver.sh          ← Start receiver
│   ├── run_detector.sh          ← Start detector
│   ├── run_storage.sh           ← Start storage
│   ├── run_demo.sh              ← Run demo
│   ├── run_background_remover.sh
│   └── run_mediapipe.sh
│
├── data/
│   ├── images/                  ← Sample images
│   ├── video/                   ← Sample videos
│   └── results/                 ← Detection results
│
├── models/
│   └── yolo/                    ← YOLO model files
│
├── output/
│   ├── detections.json          ← Detection results
│   └── screenshots/             ← System screenshots
│
└── .planning/                   ← Project planning files
```

---

## Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment (optional)
python -m venv venv
source venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

### 2. Run the Demo

```bash
# Option 1: Run full demo (starts all servers + sender)
./scripts/run_demo.sh --frames 10

# Option 2: Start servers manually
./scripts/run_all.sh

# Then in another terminal:
python src/sender.py --frames 10
```

### 3. View Results

```bash
cat output/detections.json
```

---

## Running Individual Components

### Start Servers (in separate terminals)

```bash
# Terminal 1: Storage Server
./scripts/run_storage.sh

# Terminal 2: Detector Server  
./scripts/run_detector.sh

# Terminal 3: Receiver Server
./scripts/run_receiver.sh

# Terminal 4: Send frames
./scripts/run_sender.sh --frames 20
```

### With Video File

```bash
python src/sender.py --video data/video/sample.mp4 --fps 5
```

### With Webcam

```bash
python src/sender.py --camera 0 --fps 2
```

---

## Utility Scripts

### Background Remover

```bash
# Remove background from image
python src/background_remover.py --input image.jpg --output result.png

# With transparent background
python src/background_remover.py --input image.jpg --color transparent

# Blur background
python src/background_remover.py --input image.jpg --blur
```

### MediaPipe Explorer

```bash
# Detect pose
python src/examine_mediapipe.py --input image.jpg --mode pose

# Detect faces
python src/examine_mediapipe.py --input image.jpg --mode face

# Detect hands
python src/examine_mediapipe.py --input image.jpg --mode hands

# All detections
python src/examine_mediapipe.py --input image.jpg --mode all
```

---

## Output Format

Detection results are stored in JSON format:

```json
{
  "frame_id": "abc123",
  "frame_number": 1,
  "timestamp": "2026-05-30T14:30:00",
  "processing_time_ms": 45.2,
  "detection": {
    "person_count": 2,
    "bounding_boxes": [
      {
        "x": 100,
        "y": 50,
        "width": 60,
        "height": 150,
        "confidence": 0.92,
        "class": "person"
      }
    ],
    "detection_method": "YOLO"
  }
}
```

---

## Configuration

Edit `src/config.py` to change settings:

```python
class Config:
    HOST = "localhost"
    RECEIVER_PORT = 6100
    PROCESSING_PORT = 6200
    STORAGE_PORT = 6300
    CONFIDENCE_THRESHOLD = 0.5
```

---

## Requirements

- Python 3.8+
- OpenCV
- NumPy
- Ultralytics (YOLO)
- MediaPipe
- PySpark (optional, for distributed processing)

Install all with:
```bash
pip install opencv-python numpy ultralytics mediapipe pyspark
```

---

## Screenshots

See `output/screenshots/` for system screenshots.

---

## License

This project is for educational purposes as part of DS200.Q21.1 Big Data course.
│   ├── run_detection.py                ← Run Python YOLO detection pipeline
│   └── java.sh                         ← Convenience wrapper
│
├── data/
│   ├── images/                         ← Test images for detection
│   └── video/                          ← Test videos (people-detection.mp4)
│
├── models/
│   └── yolo/                           ← YOLO model files (Git LFS tracked)
│       └── yolo12n.pt                  ← Pre-trained YOLOv12 nano model
│
├── output/
│   ├── results/                        ← Detection results
│   │   └── detections.json             ← Bounding boxes and person counts
│   └── screenshots/                    ← Execution screenshots
│
└── .planning/                          ← Project planning documentation
```

---

## Git LFS (Large File Storage)

This project uses **Git LFS** to track large binary files. Before cloning or committing, ensure Git LFS is installed and configured.

### Installation

```bash
# macOS
brew install git-lfs

# Ubuntu/Debian
sudo apt-get install git-lfs

# Windows
# Download from https://git-lfs.github.com/
```

### Configuration

```bash
# Initialize Git LFS in the repository
git lfs install

# Track large files (already configured in .gitattributes)
git lfs track "*.pt"           # YOLO model weights
git lfs track "*.weights"      # Darknet weights
git lfs track "*.mp4"          # Video files
git lfs track "*.avi"          # Video files
```

### Tracked Files

| File Pattern | Description | Location |
|--------------|-------------|----------|
| `*.pt` | PyTorch model weights | `models/yolo/yolo12n.pt` |
| `*.mp4` | Test video files | `data/video/` |

---

## Prerequisites

### For Python Implementation (YOLO Detection)

```bash
# Python 3.8 or higher
python --version

# Install dependencies
pip install -r requirements.txt
```

### For Java Implementation (Server Infrastructure)

```bash
# Java 11 or higher
java -version

# Maven 3.6 or higher
mvn -v

# Apache Spark (optional, for cluster mode)
spark-submit --version
```

---

## Quick Start

### Option 1: Python Implementation (Recommended for YOLO Detection)

This option executes actual person detection using the YOLO model.

```bash
# Navigate to lab directory
cd DS200.Q21.1_Lab05

# Install Python dependencies
pip install -r requirements.txt

# Run the detection pipeline
python scripts/run_detection.py
```

**Or run servers individually (4 terminals):**

```bash
# Terminal 1 — Storage Server
python src/storage_server.py

# Terminal 2 — Processing Server (YOLO + Spark)
python src/processing_server.py

# Terminal 3 — Receiver Server
python src/receiver_server.py

# Terminal 4 — Video Source
python src/video_source.py
```

### Option 2: Java Implementation (Server Infrastructure Demo)

This option demonstrates the distributed server architecture with mock detection.

```bash
# Navigate to lab directory
cd DS200.Q21.1_Lab05

# Make scripts executable
chmod +x scripts/*.sh

# Build the project
./scripts/run_java_streaming_local.sh build
```

**Run servers (4 terminals):**

```bash
# Terminal 1 — Storage Server
./scripts/run_java_streaming_local.sh storage

# Terminal 2 — Processing Server (Spark Streaming)
./scripts/run_java_streaming_local.sh processing

# Terminal 3 — Receiver Server
./scripts/run_java_streaming_local.sh receiver

# Terminal 4 — Video Source
./scripts/run_java_streaming_local.sh source
```

---

## Model Files

### YOLO Model (Required for Python Detection)

The pre-trained YOLOv12 nano model is included:

```
models/yolo/yolo12n.pt    ← Pre-trained model (Git LFS tracked)
```

To download manually:

```bash
cd models/yolo/
curl -L -o yolo12n.pt "https://github.com/ultralytics/assets/releases/download/v8.3.0/yolo11n.pt"
```

### Test Data

```
data/video/people-detection.mp4    ← Sample video with pedestrians
data/images/                       ← Test images (optional)
```

---

## Output Results

Detection results are saved to `output/results/detections.json`:

```json
{
  "video_file": "data/video/people-detection.mp4",
  "model": "models/yolo/yolo12n.pt",
  "total_frames": 596,
  "summary": {
    "total_person_detections": 389,
    "frames_with_persons": 218,
    "max_persons_in_frame": 4,
    "avg_persons_per_frame": 0.65
  },
  "detections": [
    {
      "frame_id": 1,
      "person_count": 2,
      "bounding_boxes": [
        {"x": 100, "y": 150, "width": 50, "height": 120, "confidence": 0.95},
        {"x": 250, "y": 180, "width": 45, "height": 110, "confidence": 0.89}
      ]
    }
  ]
}
```

Screenshots of execution are saved to `output/screenshots/`.

---

## Configuration

### Python Configuration (`src/config.py`)

```python
class Config:
    RECEIVER_PORT = 6100
    PROCESSING_PORT = 6200
    STORAGE_PORT = 6300
    
    YOLO_MODEL_PATH = "models/yolo/yolo12n.pt"
    CONFIDENCE_THRESHOLD = 0.5
```

### Java Configuration (`spark/java/.../Config.java`)

```java
public class Config {
    public static final int RECEIVER_PORT = 6100;
    public static final int PROCESSING_PORT = 6200;
    public static final int STORAGE_PORT = 6300;
    
    public static final String SPARK_MASTER = "local[*]";
    public static final int SPARK_BATCH_INTERVAL = 1;
}
```

---

## Data Formats

### Frame Payload (JSON via TCP)

```json
{
  "frameId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-05-30T10:30:00Z",
  "frameNumber": 1,
  "data": "base64_encoded_image_data",
  "width": 640,
  "height": 480
}
```

### Detection Result (JSON)

```json
{
  "frameId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-05-30T10:30:01Z",
  "personCount": 3,
  "boundingBoxes": [
    {"x": 100, "y": 150, "width": 50, "height": 120, "confidence": 0.95},
    {"x": 250, "y": 180, "width": 45, "height": 110, "confidence": 0.89}
  ],
  "processingTimeMs": 45.2
}
```

---

## Big Data Context

This system demonstrates big data concepts through:

| Concept | Implementation |
|---------|----------------|
| **Distributed Processing** | Three independent servers communicating via TCP network |
| **Stream Processing** | Real-time frame processing with Apache Spark Streaming |
| **Micro-batch Architecture** | Configurable batch intervals for optimized throughput |
| **Horizontal Scalability** | Architecture supports scaling across multiple nodes |
| **Fault Tolerance** | TCP connection retry logic and graceful error handling |
| **Deep Learning Integration** | YOLO model for accurate person detection |

---

## Execution Screenshots

Execution screenshots are available in `output/screenshots/` directory, demonstrating:

- Server startup sequences
- Real-time frame processing
- Detection results with bounding boxes
- Storage server output

---

## License

This project is developed for educational purposes as part of the DS200.Q21.1 Big Data course at the University of Information Technology (UIT).

---

**Author**: Nguyen Cong Phat (23521143)  
**GitHub**: [paht2005](https://github.com/paht2005)  
**Email**: 23521143@gm.uit.edu.vn
