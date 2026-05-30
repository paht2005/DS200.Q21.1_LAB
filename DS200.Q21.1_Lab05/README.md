<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis (Lab 05)</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.8+" />
  <img src="https://img.shields.io/badge/Spark%20Streaming-3.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark Streaming" />
  <img src="https://img.shields.io/badge/YOLO-v12-00FFFF?style=for-the-badge&logo=yolo&logoColor=black" alt="YOLO" />
  <img src="https://img.shields.io/badge/SAHI-Sliced%20Inference-FF6B6B?style=for-the-badge" alt="SAHI" />
  <img src="https://img.shields.io/badge/OpenCV-4.x-5C3EE8?style=for-the-badge&logo=opencv&logoColor=white" alt="OpenCV" />
</p>

<p align="center">
  <a href="https://github.com/paht2005"><img src="https://img.shields.io/badge/GitHub-paht2005-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/in/ncp2005/"><img src="https://img.shields.io/badge/LinkedIn-Phat%20Nguyen-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23521143@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23521143%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1_Lab/DS200.Q21.1_Lab05/` | Parent overview: [../README.md](../README.md)

---

## Student Information

| Student ID | Full Name        | GitHub                                  | Email                  |
|:----------:|------------------|-----------------------------------------|------------------------|
| 23521143   | Phat Cong Nguyen | [paht2005](https://github.com/paht2005) | 23521143@gm.uit.edu.vn |

---

## Table of Contents

1. [Objective](#objective)
2. [System Architecture](#system-architecture)
3. [Technologies](#technologies)
4. [Repository Layout](#repository-layout)
5. [Prerequisites](#prerequisites)
6. [Quick Start](#quick-start)
7. [Batch Video Processing](#batch-video-processing)
8. [Running Individual Components](#running-individual-components)
9. [Utility Scripts](#utility-scripts)
10. [Output Format](#output-format)
11. [Configuration](#configuration)
12. [Screenshots](#screenshots)
13. [Submission Checklist](#submission-checklist)

---

## Objective

Build a **distributed real-time person counting system** that processes camera frames using a three-tier server architecture. The system demonstrates big data stream processing concepts with **PySpark Streaming** for scalable frame processing and **YOLO (You Only Look Once)** deep learning model for accurate person detection.

### Features

- **Real-time Streaming** - TCP-based frame transmission with PySpark Streaming support
- **YOLO Detection** - YOLOv12 nano model for fast and accurate person detection
- **SAHI Integration** - Slicing Aided Hyper Inference for improved detection of small or distant persons
- **Batch Processing** - Process multiple videos in parallel with per-video output structure
- **Background Removal** - Remove or blur backgrounds using MediaPipe segmentation
- **MediaPipe Explorer** - Explore pose, face, and hand detection capabilities

---

## System Architecture

```
┌─────────────┐     TCP      ┌──────────────┐     TCP      ┌──────────────┐
│   Video/    │ ──────────► │   Receiver   │ ──────────► │  Detector    │
│   Camera    │   frames    │  (port 6100) │   frames    │  (port 6200) │
│  (sender)   │             └──────────────┘             │  YOLO/SAHI   │
└─────────────┘                                          └──────────────┘
                                                                │
                                                                │ detection
                                                                │ results
                                                                ▼
                                                         ┌──────────────┐
                                                         │   Storage    │
                                                         │  (port 6300) │
                                                         └──────────────┘
                                                                │
                                                                ▼
                                                    output/results/{video}/
```

---

## Technologies

| Technology | Purpose |
|------------|---------|
| **Python 3.8+** | Primary programming language |
| **PySpark Streaming** | Big data stream processing framework |
| **Ultralytics YOLO** | YOLOv12 person detection model |
| **SAHI** | Slicing Aided Hyper Inference for small object detection |
| **OpenCV** | Image and video processing |
| **MediaPipe** | Pose, face, and hand detection |
| **TCP Sockets** | Inter-server communication |

---

## Repository Layout

```text
DS200.Q21.1_Lab05/
├── README.md                    # This documentation
├── 23521143.txt                 # Student ID
├── requirements.txt             # Python dependencies
├── tcp_example.py               # TCP connection example
│
├── src/                         # Python source code
│   ├── config.py                # Configuration settings
│   ├── sender.py                # Frame sender (client)
│   ├── receiver.py              # Frame receiver server
│   ├── detect_object.py         # Object detection server (YOLO + SAHI)
│   ├── storage_server.py        # Result storage server
│   ├── process_videos.py        # Batch video processing script
│   ├── background_remover.py    # Background removal utility
│   ├── examine_mediapipe.py     # MediaPipe exploration tool
│   └── demo-example.py          # End-to-end demo script
│
├── scripts/                     # Shell scripts for quick execution
│   ├── run_all.sh               # Start all servers
│   ├── run_sender.sh            # Start sender
│   ├── run_receiver.sh          # Start receiver
│   ├── run_detector.sh          # Start detector
│   ├── run_storage.sh           # Start storage
│   ├── run_demo.sh              # Run demo
│   ├── run_background_remover.sh
│   └── run_mediapipe.sh
│
├── data/
│   ├── images/                  # Sample images
│   └── video/                   # Sample videos (pedestrian videos)
│
├── models/
│   └── yolo/                    # YOLO model files (yolo12n.pt)
│
├── output/
│   └── results/                 # Detection results (per-video directories)
│       ├── overall_summary.json
│       ├── {video-name}/
│       │   ├── frame_detections.json
│       │   ├── summary.json
│       │   └── report.txt
│       └── ...
│
└── screenshots/                 # System execution screenshots
```

---

## Prerequisites

### Python Environment

```bash
# Python 3.8 or higher required
python3 --version

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

Or install manually:

```bash
pip install opencv-python numpy ultralytics mediapipe pyspark sahi
```

### Optional: Java Runtime (for PySpark parallel processing)

PySpark requires Java 8+ for parallel video processing. Without Java, the system falls back to sequential processing.

```bash
# macOS
brew install openjdk@11

# Ubuntu/Debian
sudo apt-get install openjdk-11-jdk
```

---

## Quick Start

### 1. Install Dependencies

```bash
cd DS200.Q21.1_Lab05
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Demo

```bash
# Option 1: Run full demo with video file
python3 src/demo-example.py --video data/video/people-detection.mp4 --frames 30

# Option 2: Start servers manually (4 terminals)
./scripts/run_all.sh

# Then in another terminal:
python3 src/sender.py --video data/video/people-detection.mp4 --frames 30
```

### 3. View Results

```bash
cat output/results/detections.json
```

---

## Batch Video Processing

Process multiple videos at once with per-video output structure using the batch processing script.

### Basic Usage

```bash
# Process all videos in data/video/ with SAHI (improved small object detection)
python3 src/process_videos.py

# Process without SAHI (faster, standard YOLO)
python3 src/process_videos.py --no-sahi

# Custom video directory and output
python3 src/process_videos.py --videos-dir /path/to/videos --output-dir /path/to/output
```

### SAHI (Slicing Aided Hyper Inference)

SAHI improves detection accuracy for small or distant persons by:
- Slicing the image into overlapping patches (256x256)
- Running detection on each patch
- Merging results with Non-Maximum Suppression

```bash
# Enable SAHI (default, slower but more accurate for crowded scenes)
python3 src/process_videos.py

# Disable SAHI (faster processing)
python3 src/process_videos.py --no-sahi
```

### Output Structure

After processing, results are organized per video:

```text
output/results/
├── overall_summary.json          # Combined summary of all videos
├── pedestrian-video1/
│   ├── frame_detections.json     # Per-frame detection data
│   ├── summary.json              # Video statistics
│   └── report.txt                # Human-readable report
├── pedestrian-video2/
│   └── ...
└── people-detection/
    └── ...
```

---
cat output/results/detections.json
```

---

## Running Individual Components

### Start Servers (in separate terminals)

```bash
# Terminal 1: Storage Server
python3 src/storage_server.py

# Terminal 2: Detector Server  
python3 src/detect_object.py

# Terminal 3: Receiver Server
python3 src/receiver.py

# Terminal 4: Send frames
python3 src/sender.py --video data/video/people-detection.mp4 --frames 30
```

### With Video File

```bash
python3 src/sender.py --video data/video/people-detection.mp4 --fps 5
```

### With Webcam

```bash
python3 src/sender.py --camera 0 --fps 2
```

---

## Utility Scripts

### Background Remover

```bash
# Remove background from image
python3 src/background_remover.py --input image.jpg --output result.png

# With transparent background
python3 src/background_remover.py --input image.jpg --color transparent

# Blur background
python3 src/background_remover.py --input image.jpg --blur
```

### MediaPipe Explorer

```bash
# Detect pose
python3 src/examine_mediapipe.py --input image.jpg --mode pose

# Detect faces
python3 src/examine_mediapipe.py --input image.jpg --mode face

# Detect hands
python3 src/examine_mediapipe.py --input image.jpg --mode hands

# All detections
python3 src/examine_mediapipe.py --input image.jpg --mode all
```

---

## Output Format

### Per-Video Summary (summary.json)

```json
{
  "video_name": "pedestrian-video1",
  "video_path": "data/video/pedestrian-video1.mp4",
  "video_info": {
    "total_frames": 3493,
    "fps": 30.0,
    "resolution": "1920x1080",
    "duration_seconds": 116.43
  },
  "detection_summary": {
    "total_detections": 30234,
    "frames_processed": 3493,
    "avg_persons_per_frame": 8.66,
    "max_persons_in_frame": 15,
    "frames_with_persons": 3450,
    "detection_method": "YOLO"
  },
  "processing": {
    "processing_time_seconds": 432.10,
    "fps_processing": 8.08,
    "timestamp": "2026-05-30T15:42:40.788"
  }
}
```

### Frame Detections (frame_detections.json)

```json
[
  {
    "frame_number": 1,
    "timestamp": 0.033,
    "person_count": 3,
    "bounding_boxes": [
      {
        "x": 100,
        "y": 50,
        "width": 60,
        "height": 150,
        "confidence": 0.92
      }
    ],
    "detection_method": "YOLO"
  }
]
```

### Overall Summary (overall_summary.json)

```json
{
  "processing_info": {
    "total_videos": 3,
    "total_processing_time": 653.05,
    "detection_method": "YOLO",
    "spark_enabled": false,
    "timestamp": "2026-05-30T15:46:20"
  },
  "videos": [
    {"video_name": "pedestrian-video1", "...": "..."},
    {"video_name": "pedestrian-video2", "...": "..."},
    {"video_name": "people-detection", "...": "..."}
  ]
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
    YOLO_MODEL_PATH = "models/yolo/yolo12n.pt"
```

### Tips for Better Detection Accuracy

To improve person detection accuracy, especially in crowded scenes or when persons appear small in the frame:

1. **Lower the confidence threshold**: Reduce `CONFIDENCE_THRESHOLD` in `src/config.py` from `0.5` to a lower value (e.g., `0.3` or `0.25`). This allows detections with lower confidence scores to be included.

2. **Enable SAHI**: Use SAHI (Slicing Aided Hyper Inference) for batch processing:
   ```bash
   # With SAHI enabled (default)
   python3 src/process_videos.py
   ```
   SAHI slices the image into overlapping patches and runs detection on each patch, significantly improving detection of small or distant persons.

**Note**: Lower thresholds may increase false positives. SAHI increases processing time but provides more accurate counts in complex scenes.

---

## Screenshots

Detection visualization screenshots are available in `screenshots/`:

```text
screenshots/
├── pedestrian-video1/          # Sampled frames with detection boxes
│   ├── frame_0001.jpg
│   ├── frame_0100.jpg
│   ├── frame_0500.jpg
│   ├── frame_1000.jpg
│   ├── frame_2000.jpg
│   └── frame_3000.jpg
└── pedestrian-video2/
    ├── frame_0001.jpg
    ├── frame_0100.jpg
    ├── frame_0300.jpg
    ├── frame_0500.jpg
    ├── frame_0800.jpg
    └── frame_1000.jpg
```

Each screenshot shows:
- Green bounding boxes around detected persons
- Confidence score for each detection
- Frame number and total person count

---

## Submission Checklist

- [x] All Python source code in `src/`
- [x] Shell scripts in `scripts/`
- [x] YOLO model in `models/yolo/`
- [x] Sample video in `data/video/`
- [x] Detection results in `output/results/`
- [x] Screenshots in `output/screenshots/`
- [x] Student ID file `23521143.txt`
- [x] This README with documentation

---

## License

This project is developed for educational purposes as part of the DS200.Q21.1 Big Data course at the University of Information Technology (UIT).

---

**Author**: Phat Cong Nguyen (23521143)  
**GitHub**: [paht2005](https://github.com/paht2005)  
**Email**: 23521143@gm.uit.edu.vn
