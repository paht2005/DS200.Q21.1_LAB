# DS200.Q21.2 Lab05 — Real-Time Person Counting System

> **Student ID**: 23521143  
> **Course**: DS200.Q21.2 - Big Data  
> **Lab**: 05 - Distributed Stream Processing

## Overview

A distributed real-time person counting system that processes camera frames using big data technologies (Apache Spark Streaming). The system consists of three interconnected servers communicating via TCP sockets.

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
```

### Components

| Server | Port | Description |
|--------|------|-------------|
| **Receiver Server** | 6100 | Receives camera frames and forwards to processing |
| **Processing Server** | 6200 | Object detection using YOLO, outputs bounding boxes |
| **Storage Server** | 6300 | Stores detection results persistently |

## Technologies

- **Python 3.8+**
- **Apache Spark Streaming** — Big data stream processing
- **OpenCV** — Image processing
- **YOLO** — Object detection model
- **TCP Sockets** — Inter-server communication

## Project Structure

```
DS200.Q21.2_Lab05/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── src/
│   ├── config.py               # Configuration settings
│   ├── receiver_server.py      # Frame receiver server
│   ├── processing_server.py    # Object detection server with Spark
│   ├── storage_server.py       # Results storage server
│   └── video_source.py         # Video/camera frame simulator
├── data/
│   └── results/                # Stored detection results
├── models/
│   └── yolo/                   # YOLO model files
├── output/
│   └── screenshots/            # Execution screenshots
└── .planning/                  # Project planning docs
```

## Installation

### 1. Clone the repository
```bash
git clone https://github.com/paht2005/DS200.Q21.2_Lab05.git
cd DS200.Q21.2_Lab05
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Download YOLO model (optional - for full detection)
```bash
# Download YOLOv8 weights
wget https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n.pt -P models/yolo/
```

## Usage

### Start all servers (in separate terminals)

**Terminal 1 — Storage Server:**
```bash
python src/storage_server.py
```

**Terminal 2 — Processing Server:**
```bash
python src/processing_server.py
```

**Terminal 3 — Receiver Server:**
```bash
python src/receiver_server.py
```

**Terminal 4 — Video Source (Simulator):**
```bash
python src/video_source.py
```

### With Spark Streaming
```bash
spark-submit --master local[*] src/processing_server.py
```

## Configuration

Edit `src/config.py` to modify server settings:

```python
class Config:
    # Server ports
    RECEIVER_PORT = 6100
    PROCESSING_PORT = 6200
    STORAGE_PORT = 6300
    
    # Spark settings
    SPARK_BATCH_INTERVAL = 1  # seconds
```

## Data Formats

### Frame Payload (JSON)
```json
{
  "frame_id": "uuid-string",
  "timestamp": "2026-05-30T10:30:00Z",
  "data": "base64_encoded_image_data"
}
```

### Detection Result (JSON)
```json
{
  "frame_id": "uuid-string",
  "timestamp": "2026-05-30T10:30:00Z",
  "person_count": 3,
  "bounding_boxes": [
    {"x": 100, "y": 150, "width": 50, "height": 120, "confidence": 0.95},
    {"x": 250, "y": 180, "width": 45, "height": 110, "confidence": 0.89},
    {"x": 400, "y": 160, "width": 55, "height": 125, "confidence": 0.92}
  ]
}
```

## Big Data Context

This system demonstrates big data concepts through:

1. **Distributed Processing**: Three independent servers communicating via network
2. **Stream Processing**: Real-time frame processing with Spark Streaming
3. **Scalability**: Architecture supports horizontal scaling
4. **Fault Tolerance**: TCP connection handling with retry logic

## Screenshots

See `output/screenshots/` for execution screenshots.

## License

Educational use for DS200.Q21.2 course at UIT.

---

**Author**: Student 23521143  
**GitHub**: [paht2005](https://github.com/paht2005)
