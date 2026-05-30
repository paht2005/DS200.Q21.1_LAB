# DS200.Q21.1 Lab05 — Real-Time Person Counting System

> **Student ID**: 23521143  
> **Course**: DS200.Q21.1 - Big Data  
> **Lab**: 05 - Distributed Stream Processing with Spark Streaming

---

## Overview

A distributed real-time person counting system that processes camera frames using a three-tier server architecture. The system demonstrates big data stream processing concepts with **Apache Spark Streaming** for scalable frame processing and **YOLO (You Only Look Once)** deep learning model for accurate person detection.

### Implementation Strategy

This project provides **two complementary implementations**:

| Implementation | Purpose | Technology |
|----------------|---------|------------|
| **Java (Spark Streaming)** | Server infrastructure and TCP communication | Java 11+, Apache Spark Streaming, Maven |
| **Python (YOLO Detection)** | Object detection with pre-trained models | Python 3.8+, Ultralytics YOLO, PySpark |

> **Important**: The **Python implementation** is required to execute the YOLO model (`yolo12n.pt`) for actual person detection. The Java implementation provides the distributed server architecture but uses mock detection. For production-quality detection with bounding boxes, run the Python scripts.

---

## Architecture

```
┌─────────────┐     TCP      ┌──────────────┐     TCP      ┌──────────────┐
│   Camera/   │ ──────────► │   Receiver   │ ──────────► │  Processing  │
│   Video     │   frames    │    Server    │   frames    │    Server    │
│   Source    │             │  (port 6100) │             │  (port 6200) │
└─────────────┘             └──────────────┘             └──────────────┘
                                                                │
                                                                │ detection
                                                                │ results (bounding boxes)
                                                                ▼
                                                         ┌──────────────┐
                                                         │   Storage    │
                                                         │    Server    │
                                                         │  (port 6300) │
                                                         └──────────────┘
                                                                │
                                                                ▼
                                                    output/results/detections.json
```

### Server Components

| Server | Port | Description |
|--------|------|-------------|
| **FrameReceiverServer** | 6100 | Receives camera frames via TCP and forwards to processing server |
| **ProcessingServer** | 6200 | Performs object detection using Spark Streaming, outputs bounding boxes |
| **StorageServer** | 6300 | Persists detection results to JSON format |

---

## Technologies

| Technology | Purpose | Implementation |
|------------|---------|----------------|
| **Java 11+** | Server infrastructure and TCP communication | `spark/java/` |
| **Apache Spark Streaming** | Distributed stream processing (big data) | Both Java and Python |
| **Python 3.8+** | YOLO model inference and detection | `src/` |
| **Ultralytics YOLO** | Pre-trained person detection model | `models/yolo/yolo12n.pt` |
| **OpenCV** | Image processing and frame handling | Python |
| **Maven** | Java build and dependency management | `pom.xml` |
| **GSON** | JSON serialization | Java |
| **TCP Sockets** | Inter-server communication | Both |

---

## Project Structure

```
DS200.Q21.1_Lab05/
├── README.md                           ← This documentation
├── 23521143.txt                        ← Student ID file
├── requirements.txt                    ← Python dependencies
│
├── src/                                ← Python implementation (YOLO detection)
│   ├── config.py                       ← Configuration constants
│   ├── receiver_server.py              ← Server 1: Receives frames from camera
│   ├── processing_server.py            ← Server 2: YOLO detection + Spark Streaming
│   ├── storage_server.py               ← Server 3: Persists results to JSON
│   └── video_source.py                 ← Video/camera frame source
│
├── spark/java/lab05-streaming/         ← Java implementation (server infrastructure)
│   ├── pom.xml                         ← Maven configuration
│   └── src/main/java/lab05/
│       ├── MainApp.java                ← Main launcher
│       ├── Config.java                 ← Configuration constants
│       ├── DataModels.java             ← Data classes (Frame, BoundingBox, etc.)
│       ├── FrameReceiverServer.java    ← Server 1: Receives frames
│       ├── ProcessingServer.java       ← Server 2: Spark Streaming processing
│       ├── StorageServer.java          ← Server 3: Stores results
│       └── VideoSource.java            ← Test video/image source
│
├── scripts/
│   ├── run_java_streaming_local.sh     ← Build and run Java servers
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
