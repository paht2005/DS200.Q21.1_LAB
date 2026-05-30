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
  <img src="https://img.shields.io/badge/OpenCV-4.x-5C3EE8?style=for-the-badge&logo=opencv&logoColor=white" alt="OpenCV" />
</p>

<p align="center">
  <a href="https://github.com/paht2005"><img src="https://img.shields.io/badge/GitHub-paht2005-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/in/ncp2005/"><img src="https://img.shields.io/badge/LinkedIn-Phat%20Nguyen-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23521143@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23521143%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1_Lab/DS200.Q21.1_Lab05/` — Parent overview: [../README.md](../README.md)

---

## Student information

| Student ID | Full name        | GitHub                                  | Email                  |
|:----------:|------------------|-----------------------------------------|------------------------|
| 23521143   | Phat Cong Nguyen | [paht2005](https://github.com/paht2005) | 23521143@gm.uit.edu.vn |

---

## Outline

1. [Objective](#objective)
2. [System Architecture](#system-architecture)
3. [Technologies](#technologies)
4. [Repository Layout](#repository-layout)
5. [Prerequisites](#prerequisites)
6. [Quick Start](#quick-start)
7. [Running Individual Components](#running-individual-components)
8. [Utility Scripts](#utility-scripts)
9. [Output Format](#output-format)
10. [Configuration](#configuration)
11. [Screenshots](#screenshots)
12. [Submission Checklist](#submission-checklist)

---

## Objective

Build a **distributed real-time person counting system** that processes camera frames using a three-tier server architecture. The system demonstrates big data stream processing concepts with **PySpark Streaming** for scalable frame processing and **YOLO (You Only Look Once)** deep learning model for accurate person detection.

### Features

- **Frame Sender** — Captures and sends video frames via TCP
- **Frame Receiver** — Receives and forwards frames to detector
- **Object Detector** — YOLO-based person detection with bounding boxes
- **Storage Server** — Persists detection results to JSON
- **Background Remover** — Remove background from images using MediaPipe
- **MediaPipe Explorer** — Explore pose, face, and hand detection

---

## System Architecture

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

## Repository Layout

```text
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
│   ├── video/                   ← Sample videos (people-detection.mp4)
│   └── results/                 ← Detection results
│
├── models/
│   └── yolo/                    ← YOLO model files (yolo12n.pt)
│
├── output/
│   ├── results/                 ← Detection results JSON
│   └── screenshots/             ← System screenshots
│
└── .planning/                   ← Project planning files
```

---

## Prerequisites

### Python Environment

```bash
# Python 3.8 or higher
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
pip install opencv-python numpy ultralytics mediapipe pyspark
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

Detection results are stored in JSON format at `output/results/detections.json`:

```json
{
  "video_file": "data/video/people-detection.mp4",
  "model": "models/yolo/yolo12n.pt",
  "total_frames": 596,
  "confidence_threshold": 0.5,
  "summary": {
    "total_person_detections": 389,
    "frames_with_persons": 218,
    "max_persons_in_frame": 4,
    "avg_persons_per_frame": 0.65
  },
  "detections": [
    {
      "frame_id": 1,
      "timestamp": "2026-05-30T13:55:51.045079",
      "person_count": 2,
      "bounding_boxes": [
        {
          "x": 100,
          "y": 50,
          "width": 60,
          "height": 150,
          "confidence": 0.92
        }
      ]
    }
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

---

## Screenshots

See `output/screenshots/` for system execution screenshots demonstrating:

- Server startup sequences
- Real-time frame processing
- Detection results with bounding boxes
- Storage server output

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
