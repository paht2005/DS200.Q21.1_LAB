# DS200.Q21.2 Lab05 — Real-Time Person Counting System

> **Student ID**: 23521143  
> **Course**: DS200.Q21.2 - Big Data  
> **Lab**: 05 - Distributed Stream Processing with Java Spark Streaming

---

## Overview

A distributed real-time person counting system that processes camera frames using **Java Spark Streaming**. The system consists of three interconnected TCP servers for receiving, processing, and storing detection results.

## Architecture

```
┌─────────────┐     TCP      ┌──────────────┐     TCP      ┌──────────────┐
│   Camera/   │ ──────────► │   Receiver   │ ──────────► │  Processing  │
│   Video     │   frames    │    Server    │   frames    │    Server    │
│   Source    │             │  (port 6100) │             │  (port 6200) │
└─────────────┘             └──────────────┘             └──────────────┘
                                                                │
                                                                │ detection
                                                                │ results
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

### Components

| Server | Port | Description |
|--------|------|-------------|
| **FrameReceiverServer** | 6100 | Receives camera frames via TCP and forwards to processing |
| **ProcessingServer** | 6200 | Object detection with Spark Streaming, outputs bounding boxes |
| **StorageServer** | 6300 | Stores detection results persistently to JSON |

---

## Technologies

| Technology | Purpose |
|------------|---------|
| **Java 11+** | Primary implementation language |
| **Apache Spark Streaming** | Big data stream processing |
| **Maven** | Build and dependency management |
| **JavaCV/OpenCV** | Image processing |
| **GSON** | JSON serialization |
| **TCP Sockets** | Inter-server communication |

---

## Project Structure

```
DS200.Q21.2_Lab05/
├── README.md                           ← This file
├── 23521143.txt                        ← Student ID
├── requirements.txt                    ← Python dependencies (optional)
│
├── spark/java/lab05-streaming/         ← PRIMARY: Java Spark Streaming project
│   ├── pom.xml                         ← Maven configuration
│   └── src/main/java/lab05/
│       ├── MainApp.java                ← Main launcher
│       ├── Config.java                 ← Configuration constants
│       ├── DataModels.java             ← Data classes (Frame, BoundingBox, etc.)
│       ├── FrameReceiverServer.java    ← Server 1: Receives frames
│       ├── ProcessingServer.java       ← Server 2: Detection + Spark
│       ├── StorageServer.java          ← Server 3: Stores results
│       └── VideoSource.java            ← Test video/image source
│
├── src/lab05/                          ← OPTIONAL: Python implementation
│   ├── __init__.py
│   └── config.py
│
├── scripts/
│   ├── run_java_streaming_local.sh     ← Build + run Java servers
│   └── java.sh                         ← Convenience wrapper
│
├── data/
│   ├── images/                         ← Test images for detection (download here)
│   └── video/                          ← Test videos (optional)
│
├── models/
│   └── yolo/                           ← YOLO model files (download here)
│       ├── yolov4-tiny.weights
│       ├── yolov4-tiny.cfg
│       └── coco.names
│
├── output/
│   ├── results/                        ← Detection results (auto-generated)
│   │   └── detections.json
│   └── screenshots/                    ← Submission screenshots
│
└── .planning/                          ← Project planning docs
```

---

## 📥 What to Download & Where to Put

### 1. Test Images → `data/images/`

Download sample images with people for detection testing:

```bash
cd data/images/

# Option 1: Download from COCO dataset samples
wget https://images.cocodataset.org/val2017/000000039769.jpg
wget https://images.cocodataset.org/val2017/000000397133.jpg
wget https://images.cocodataset.org/val2017/000000037777.jpg

# Option 2: Use any JPG/PNG images with people
# Just copy your images to data/images/
```

**Supported formats**: `.jpg`, `.jpeg`, `.png`, `.bmp`

### 2. YOLO Model Files → `models/yolo/` (Optional for real detection)

For real person detection (instead of mock), download YOLO files:

```bash
cd models/yolo/

# YOLOv12 nano (Ultralytics - recommended)
curl -L -o yolo12n.pt "https://github.com/ultralytics/assets/releases/download/v8.3.0/yolo11n.pt"

# Or YOLOv4-tiny (legacy Darknet format)
# wget https://github.com/AlexeyAB/darknet/releases/download/darknet_yolo_v4_pre/yolov4-tiny.weights
# wget https://raw.githubusercontent.com/AlexeyAB/darknet/master/cfg/yolov4-tiny.cfg
# wget https://raw.githubusercontent.com/pjreddie/darknet/master/data/coco.names
```

> **Note**: Model already downloaded: `models/yolo/yolo12n.pt` ✓

### 3. Test Video → `data/video/` (Already included)

A test video with people is already included:
- `data/video/people-detection.mp4` ✓

To download additional videos:
```bash
cd data/video/
# Download pedestrian detection video
wget -O pedestrians.mp4 "https://github.com/intel-iot-devkit/sample-videos/raw/master/people-detection.mp4"
```

---

## 🚀 Quick Start

### Prerequisites

- **Java 11+**: `java -version`
- **Maven 3.6+**: `mvn -v`
- **Apache Spark** (optional, for cluster mode)

### Build & Run

```bash
# Navigate to lab directory
cd DS200.Q21.2_Lab05

# Make scripts executable
chmod +x scripts/*.sh

# Build the project
./scripts/run_java_streaming_local.sh build
```

### Run Demo (4 Terminals)

Open **4 separate terminal windows** and run in order:

**Terminal 1 — Storage Server:**
```bash
./scripts/run_java_streaming_local.sh storage
```

**Terminal 2 — Processing Server (Spark):**
```bash
./scripts/run_java_streaming_local.sh processing
```

**Terminal 3 — Receiver Server:**
```bash
./scripts/run_java_streaming_local.sh receiver
```

**Terminal 4 — Video Source (Test Frames):**
```bash
./scripts/run_java_streaming_local.sh source
```

### View Results

Detection results are saved to:
```bash
cat output/results/detections.json
```

---

## Alternative: Run with spark-submit

For full Spark cluster integration:

```bash
# Build JAR
cd spark/java/lab05-streaming
mvn clean package

# Run ProcessingServer with spark-submit
spark-submit --class lab05.ProcessingServer \
             --master local[*] \
             target/lab05-streaming-1.0-SNAPSHOT.jar
```

---

## Configuration

Edit `spark/java/lab05-streaming/src/main/java/lab05/Config.java`:

```java
public class Config {
    // Server ports
    public static final int RECEIVER_PORT = 6100;
    public static final int PROCESSING_PORT = 6200;
    public static final int STORAGE_PORT = 6300;
    
    // Spark settings
    public static final String SPARK_MASTER = "local[*]";
    public static final int SPARK_BATCH_INTERVAL = 1; // seconds
    
    // Detection
    public static final double CONFIDENCE_THRESHOLD = 0.5;
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
| **Distributed Processing** | Three independent servers communicating via network |
| **Stream Processing** | Real-time frame processing with Spark Streaming |
| **Micro-batch Architecture** | Configurable batch intervals for throughput |
| **Scalability** | Architecture supports horizontal scaling |
| **Fault Tolerance** | TCP connection retry logic |

---

## Screenshots

See `output/screenshots/` for execution screenshots.

---

## Python Alternative (Optional)

A Python implementation is available in `src/lab05/` for reference:

```bash
pip install -r requirements.txt
python -m lab05.storage_server
python -m lab05.processing_server
python -m lab05.receiver_server
python -m lab05.video_source
```

---

## License

Educational use for DS200.Q21.2 course at UIT.

---

**Author**: Nguyen Cong Phat (23521143)  
**GitHub**: [paht2005](https://github.com/paht2005)  
**Email**: 23521143@gm.uit.edu.vn
