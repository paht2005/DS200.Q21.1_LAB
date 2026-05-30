#!/usr/bin/env python3
"""
Run YOLO person detection on video file.
Outputs detection results to JSON and saves screenshots with bounding boxes.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import json
import cv2
from datetime import datetime
from ultralytics import YOLO

# Configuration
VIDEO_PATH = "data/video/people-detection.mp4"
MODEL_PATH = "models/yolo/yolo12n.pt"
OUTPUT_JSON = "output/results/detections.json"
OUTPUT_SCREENSHOTS = "output/screenshots"
CONFIDENCE_THRESHOLD = 0.5
FRAME_INTERVAL = 30  # Save every 30th frame for screenshots


def main():
    # Change to project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)
    
    print(f"Project root: {project_root}")
    print(f"Loading YOLO model from: {MODEL_PATH}")
    
    # Load model
    model = YOLO(MODEL_PATH)
    print("Model loaded successfully!")
    
    # Open video
    cap = cv2.VideoCapture(VIDEO_PATH)
    if not cap.isOpened():
        print(f"Error: Could not open video {VIDEO_PATH}")
        return
    
    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    print(f"Video: {VIDEO_PATH}")
    print(f"FPS: {fps}, Total frames: {total_frames}")
    
    # Create output directories
    os.makedirs(OUTPUT_SCREENSHOTS, exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    
    # Detection results
    all_detections = []
    frame_count = 0
    screenshot_count = 0
    total_persons = 0
    
    print("\nProcessing video...")
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        frame_count += 1
        
        # Run YOLO detection (class 0 = person)
        results = model(frame, classes=[0], verbose=False)
        
        # Extract bounding boxes
        bounding_boxes = []
        for result in results:
            for box in result.boxes:
                x, y, w, h = box.xywh[0].tolist()
                confidence = float(box.conf[0])
                
                if confidence >= CONFIDENCE_THRESHOLD:
                    bbox = {
                        "x": int(x - w/2),
                        "y": int(y - h/2),
                        "width": int(w),
                        "height": int(h),
                        "confidence": round(confidence, 3)
                    }
                    bounding_boxes.append(bbox)
                    
                    # Draw bounding box on frame
                    x1, y1 = bbox["x"], bbox["y"]
                    x2, y2 = x1 + bbox["width"], y1 + bbox["height"]
                    cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                    cv2.putText(frame, f"Person {confidence:.2f}", (x1, y1 - 10),
                               cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
        
        person_count = len(bounding_boxes)
        total_persons += person_count
        
        # Store detection result
        detection = {
            "frame_id": frame_count,
            "timestamp": datetime.now().isoformat(),
            "person_count": person_count,
            "bounding_boxes": bounding_boxes
        }
        all_detections.append(detection)
        
        # Save screenshot every N frames (or if persons detected)
        if frame_count % FRAME_INTERVAL == 0 or (person_count > 0 and screenshot_count < 20):
            screenshot_path = os.path.join(OUTPUT_SCREENSHOTS, f"frame_{frame_count:04d}.jpg")
            cv2.imwrite(screenshot_path, frame)
            screenshot_count += 1
            print(f"  Frame {frame_count}/{total_frames}: {person_count} persons detected -> saved screenshot")
        
        # Progress update
        if frame_count % 100 == 0:
            print(f"  Processed {frame_count}/{total_frames} frames...")
    
    cap.release()
    
    # Save detection results to JSON
    output_data = {
        "video_file": VIDEO_PATH,
        "model": MODEL_PATH,
        "total_frames": frame_count,
        "confidence_threshold": CONFIDENCE_THRESHOLD,
        "processed_at": datetime.now().isoformat(),
        "summary": {
            "total_person_detections": total_persons,
            "frames_with_persons": sum(1 for d in all_detections if d["person_count"] > 0),
            "max_persons_in_frame": max(d["person_count"] for d in all_detections) if all_detections else 0,
            "avg_persons_per_frame": round(total_persons / frame_count, 2) if frame_count > 0 else 0
        },
        "detections": all_detections
    }
    
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n{'='*50}")
    print("Detection Complete!")
    print(f"{'='*50}")
    print(f"Total frames processed: {frame_count}")
    print(f"Total person detections: {total_persons}")
    print(f"Frames with persons: {output_data['summary']['frames_with_persons']}")
    print(f"Max persons in single frame: {output_data['summary']['max_persons_in_frame']}")
    print(f"Average persons per frame: {output_data['summary']['avg_persons_per_frame']}")
    print(f"\nResults saved to: {OUTPUT_JSON}")
    print(f"Screenshots saved to: {OUTPUT_SCREENSHOTS}/ ({screenshot_count} images)")


if __name__ == "__main__":
    main()
