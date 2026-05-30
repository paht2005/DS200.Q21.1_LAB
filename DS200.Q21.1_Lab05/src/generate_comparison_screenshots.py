"""
Generate comparison screenshots between SAHI and non-SAHI detection.

This script processes sample frames from videos and saves annotated screenshots
in two directories for visual comparison:
- output/screenshots/no-sahi/   : Standard YOLO detection
- output/screenshots/with-sahi/ : SAHI sliced inference detection

Usage:
    python src/generate_comparison_screenshots.py
"""

import os
import sys
import cv2
import numpy as np
from pathlib import Path
from typing import Dict, Any, List

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config

# YOLO imports
try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False
    print("Error: Ultralytics required. Install with: pip install ultralytics")
    sys.exit(1)

# SAHI imports
try:
    from sahi import AutoDetectionModel
    from sahi.predict import get_sliced_prediction
    SAHI_AVAILABLE = True
except ImportError:
    SAHI_AVAILABLE = False
    print("Warning: SAHI not available. Install with: pip install sahi")


class ComparisonGenerator:
    """Generate comparison screenshots between SAHI and non-SAHI detection."""
    
    def __init__(self, model_path: str, confidence: float = 0.3):
        self.model_path = model_path
        self.confidence = confidence
        self.yolo_model = None
        self.sahi_model = None
        self._load_models()
    
    def _load_models(self):
        """Load YOLO and SAHI models."""
        print(f"Loading YOLO model from {self.model_path}...")
        self.yolo_model = YOLO(self.model_path)
        
        if SAHI_AVAILABLE:
            print("Loading SAHI model...")
            self.sahi_model = AutoDetectionModel.from_pretrained(
                model_type="yolov8",
                model_path=self.model_path,
                confidence_threshold=self.confidence,
                device="cpu"
            )
            print("SAHI model loaded successfully")
        else:
            print("SAHI not available - only YOLO comparison will be shown")
    
    def detect_yolo(self, frame: np.ndarray) -> Dict[str, Any]:
        """Standard YOLO detection."""
        results = self.yolo_model(frame, verbose=False)
        
        boxes = []
        for result in results:
            for box in result.boxes:
                if int(box.cls[0]) == 0 and float(box.conf[0]) >= self.confidence:
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    boxes.append({
                        "x1": int(x1), "y1": int(y1),
                        "x2": int(x2), "y2": int(y2),
                        "confidence": float(box.conf[0])
                    })
        
        return {"person_count": len(boxes), "boxes": boxes}
    
    def detect_sahi(self, frame: np.ndarray) -> Dict[str, Any]:
        """SAHI sliced detection."""
        if not self.sahi_model:
            return {"person_count": 0, "boxes": []}
        
        result = get_sliced_prediction(
            frame,
            self.sahi_model,
            slice_height=256,
            slice_width=256,
            overlap_height_ratio=0.2,
            overlap_width_ratio=0.2,
            postprocess_type="NMS",
            postprocess_match_threshold=0.5,
            verbose=0
        )
        
        boxes = []
        for pred in result.object_prediction_list:
            if pred.category.id == 0:  # Person class
                bbox = pred.bbox
                boxes.append({
                    "x1": int(bbox.minx), "y1": int(bbox.miny),
                    "x2": int(bbox.maxx), "y2": int(bbox.maxy),
                    "confidence": pred.score.value
                })
        
        return {"person_count": len(boxes), "boxes": boxes}
    
    def draw_detections(self, frame: np.ndarray, detection: Dict, 
                       method: str, color: tuple = (0, 255, 0)) -> np.ndarray:
        """Draw bounding boxes on frame."""
        annotated = frame.copy()
        
        for box in detection["boxes"]:
            cv2.rectangle(annotated, 
                         (box["x1"], box["y1"]), 
                         (box["x2"], box["y2"]), 
                         color, 2)
            label = f"{box['confidence']:.2f}"
            cv2.putText(annotated, label, 
                       (box["x1"], box["y1"] - 10),
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
        
        # Add info overlay
        info = f"{method}: {detection['person_count']} persons | Conf: {self.confidence}"
        cv2.rectangle(annotated, (10, 10), (400, 40), (0, 0, 0), -1)
        cv2.putText(annotated, info, (15, 30), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        
        return annotated
    
    def process_video(self, video_path: str, output_base: str, 
                     sample_frames: List[int] = None):
        """
        Process a video and generate comparison screenshots.
        
        Args:
            video_path: Path to video file
            output_base: Base output directory (output/screenshots)
            sample_frames: List of frame numbers to capture (default: auto-select)
        """
        video_name = Path(video_path).stem
        print(f"\nProcessing video: {video_name}")
        
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"  Error: Cannot open video {video_path}")
            return
        
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        
        # Auto-select sample frames if not provided
        if sample_frames is None:
            if total_frames > 1000:
                sample_frames = [1, 100, 300, 500, 800, 1000]
            elif total_frames > 500:
                sample_frames = [1, 100, 250, 400, 500]
            else:
                sample_frames = [1, 50, 100, 200, min(300, total_frames-1)]
        
        # Create output directories
        no_sahi_dir = os.path.join(output_base, "no-sahi", video_name)
        with_sahi_dir = os.path.join(output_base, "with-sahi", video_name)
        os.makedirs(no_sahi_dir, exist_ok=True)
        os.makedirs(with_sahi_dir, exist_ok=True)
        
        print(f"  Total frames: {total_frames}")
        print(f"  Sample frames: {sample_frames}")
        
        stats = {"yolo": [], "sahi": []}
        
        for frame_num in sample_frames:
            if frame_num >= total_frames:
                continue
                
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
            ret, frame = cap.read()
            if not ret:
                continue
            
            # YOLO detection (no SAHI)
            yolo_result = self.detect_yolo(frame)
            yolo_annotated = self.draw_detections(frame, yolo_result, "YOLO", (0, 255, 0))
            yolo_path = os.path.join(no_sahi_dir, f"frame_{frame_num:04d}.jpg")
            cv2.imwrite(yolo_path, yolo_annotated)
            stats["yolo"].append(yolo_result["person_count"])
            
            # SAHI detection
            if SAHI_AVAILABLE:
                sahi_result = self.detect_sahi(frame)
                sahi_annotated = self.draw_detections(frame, sahi_result, "SAHI", (0, 255, 255))
                sahi_path = os.path.join(with_sahi_dir, f"frame_{frame_num:04d}.jpg")
                cv2.imwrite(sahi_path, sahi_annotated)
                stats["sahi"].append(sahi_result["person_count"])
                
                print(f"  Frame {frame_num}: YOLO={yolo_result['person_count']}, SAHI={sahi_result['person_count']} persons")
            else:
                print(f"  Frame {frame_num}: YOLO={yolo_result['person_count']} persons")
        
        cap.release()
        
        # Print summary
        if stats["yolo"]:
            avg_yolo = sum(stats["yolo"]) / len(stats["yolo"])
            print(f"  YOLO avg: {avg_yolo:.1f} persons/frame")
        if stats["sahi"]:
            avg_sahi = sum(stats["sahi"]) / len(stats["sahi"])
            print(f"  SAHI avg: {avg_sahi:.1f} persons/frame")
            improvement = ((avg_sahi - avg_yolo) / avg_yolo * 100) if avg_yolo > 0 else 0
            print(f"  SAHI improvement: +{improvement:.1f}%")


def main():
    """Main function."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Paths
    model_path = Config.YOLO_MODEL_PATH
    videos_dir = os.path.join(project_root, "data", "video")
    output_dir = os.path.join(project_root, "output", "screenshots")
    
    # Create output directories
    os.makedirs(os.path.join(output_dir, "no-sahi"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "with-sahi"), exist_ok=True)
    
    print("=" * 60)
    print("SAHI vs YOLO Detection Comparison Generator")
    print("=" * 60)
    print(f"Confidence threshold: {Config.CONFIDENCE_THRESHOLD}")
    print(f"Model: {model_path}")
    print(f"Output: {output_dir}")
    print("=" * 60)
    
    # Initialize generator
    generator = ComparisonGenerator(model_path, Config.CONFIDENCE_THRESHOLD)
    
    # Find videos
    video_extensions = ('.mp4', '.avi', '.mov', '.mkv')
    video_paths = []
    
    for f in os.listdir(videos_dir):
        if f.lower().endswith(video_extensions):
            video_paths.append(os.path.join(videos_dir, f))
    
    video_paths.sort()
    print(f"\nFound {len(video_paths)} videos to process")
    
    # Process each video
    for video_path in video_paths:
        generator.process_video(video_path, output_dir)
    
    print("\n" + "=" * 60)
    print("Comparison screenshots generated!")
    print(f"  No SAHI:   {output_dir}/no-sahi/")
    print(f"  With SAHI: {output_dir}/with-sahi/")
    print("=" * 60)


if __name__ == "__main__":
    main()
