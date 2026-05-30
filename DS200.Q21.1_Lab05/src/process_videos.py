"""
Multi-Video Processing Pipeline with PySpark and SAHI.

Processes multiple video files in parallel using PySpark RDD operations
and SAHI (Slicing Aided Hyper Inference) for improved small object detection.

Usage:
    python process_videos.py [--videos-dir DIR] [--output-dir DIR] [--no-sahi]
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# OpenCV imports
try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    print("Error: OpenCV is required. Install with: pip install opencv-python")
    sys.exit(1)

# YOLO imports
try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False
    print("Error: Ultralytics is required. Install with: pip install ultralytics")
    sys.exit(1)

# SAHI imports
try:
    from sahi import AutoDetectionModel
    from sahi.predict import get_sliced_prediction
    SAHI_AVAILABLE = True
except ImportError:
    SAHI_AVAILABLE = False
    print("Warning: SAHI not available. Using standard YOLO detection.")

# PySpark imports
try:
    from pyspark import SparkContext, SparkConf
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Warning: PySpark not available. Processing videos sequentially.")

from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("VideoProcessor")


class VideoProcessor:
    """Process videos for person detection with SAHI and YOLO."""
    
    def __init__(self, model_path: str, use_sahi: bool = True, confidence: float = 0.5):
        self.model_path = model_path
        self.use_sahi = use_sahi and SAHI_AVAILABLE
        self.confidence = confidence
        self.model = None
        self.sahi_model = None
        self._load_models()
    
    def _load_models(self):
        """Load YOLO and optionally SAHI models."""
        if YOLO_AVAILABLE:
            self.model = YOLO(self.model_path)
            logger.info(f"YOLO model loaded from {self.model_path}")
            
            if self.use_sahi:
                self.sahi_model = AutoDetectionModel.from_pretrained(
                    model_type="yolov8",
                    model_path=self.model_path,
                    confidence_threshold=self.confidence,
                    device="cpu"
                )
                logger.info("SAHI sliced inference enabled")
    
    def detect_persons(self, frame: np.ndarray) -> Dict[str, Any]:
        """Detect persons in a single frame."""
        if self.sahi_model:
            return self._sahi_detect(frame)
        elif self.model:
            return self._yolo_detect(frame)
        else:
            return {"person_count": 0, "bounding_boxes": [], "method": "none"}
    
    def _sahi_detect(self, frame: np.ndarray) -> Dict[str, Any]:
        """SAHI sliced detection for better small object detection."""
        try:
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
                        "x": int(bbox.minx),
                        "y": int(bbox.miny),
                        "width": int(bbox.maxx - bbox.minx),
                        "height": int(bbox.maxy - bbox.miny),
                        "confidence": round(pred.score.value, 3)
                    })
            
            return {
                "person_count": len(boxes),
                "bounding_boxes": boxes,
                "method": "SAHI"
            }
        except Exception as e:
            logger.error(f"SAHI detection error: {e}")
            return self._yolo_detect(frame)
    
    def _yolo_detect(self, frame: np.ndarray) -> Dict[str, Any]:
        """Standard YOLO detection."""
        results = self.model(frame, verbose=False)
        
        boxes = []
        for result in results:
            for box in result.boxes:
                if int(box.cls[0]) == 0 and float(box.conf[0]) >= self.confidence:
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    boxes.append({
                        "x": int(x1),
                        "y": int(y1),
                        "width": int(x2 - x1),
                        "height": int(y2 - y1),
                        "confidence": round(float(box.conf[0]), 3)
                    })
        
        return {
            "person_count": len(boxes),
            "bounding_boxes": boxes,
            "method": "YOLO"
        }
    
    def process_video(self, video_path: str, output_dir: str) -> Dict[str, Any]:
        """
        Process a single video file and save results.
        
        Args:
            video_path: Path to the video file
            output_dir: Directory to save results
            
        Returns:
            Summary statistics for the video
        """
        video_name = Path(video_path).stem
        logger.info(f"Processing video: {video_name}")
        
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            logger.error(f"Cannot open video: {video_path}")
            return {"error": f"Cannot open video: {video_path}"}
        
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        
        frame_results = []
        total_persons = 0
        frame_num = 0
        start_time = datetime.now()
        
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            frame_num += 1
            detection = self.detect_persons(frame)
            
            frame_result = {
                "frame_number": frame_num,
                "timestamp": frame_num / fps if fps > 0 else 0,
                "person_count": detection["person_count"],
                "bounding_boxes": detection["bounding_boxes"],
                "detection_method": detection["method"]
            }
            frame_results.append(frame_result)
            total_persons += detection["person_count"]
            
            # Log progress every 100 frames
            if frame_num % 100 == 0:
                logger.info(f"  {video_name}: Processed {frame_num}/{total_frames} frames")
        
        cap.release()
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Create output for this video
        video_output_dir = os.path.join(output_dir, video_name)
        os.makedirs(video_output_dir, exist_ok=True)
        
        # Save frame-by-frame results
        frames_file = os.path.join(video_output_dir, "frame_detections.json")
        with open(frames_file, "w") as f:
            json.dump(frame_results, f, indent=2)
        
        # Create summary
        summary = {
            "video_name": video_name,
            "video_path": video_path,
            "video_info": {
                "total_frames": total_frames,
                "fps": fps,
                "resolution": f"{width}x{height}",
                "duration_seconds": total_frames / fps if fps > 0 else 0
            },
            "detection_summary": {
                "total_detections": total_persons,
                "frames_processed": frame_num,
                "avg_persons_per_frame": round(total_persons / frame_num, 2) if frame_num > 0 else 0,
                "max_persons_in_frame": max(r["person_count"] for r in frame_results) if frame_results else 0,
                "frames_with_persons": sum(1 for r in frame_results if r["person_count"] > 0),
                "detection_method": frame_results[0]["detection_method"] if frame_results else "unknown"
            },
            "processing": {
                "processing_time_seconds": round(processing_time, 2),
                "fps_processing": round(frame_num / processing_time, 2) if processing_time > 0 else 0,
                "timestamp": datetime.now().isoformat()
            }
        }
        
        # Save summary
        summary_file = os.path.join(video_output_dir, "summary.json")
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)
        
        # Save human-readable report
        report_file = os.path.join(video_output_dir, "report.txt")
        with open(report_file, "w") as f:
            f.write(f"Video Analysis Report: {video_name}\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Video Info:\n")
            f.write(f"  - Resolution: {width}x{height}\n")
            f.write(f"  - FPS: {fps}\n")
            f.write(f"  - Total Frames: {total_frames}\n")
            f.write(f"  - Duration: {total_frames/fps:.2f} seconds\n\n")
            f.write(f"Detection Results:\n")
            f.write(f"  - Detection Method: {summary['detection_summary']['detection_method']}\n")
            f.write(f"  - Total Persons Detected: {total_persons}\n")
            f.write(f"  - Average Persons/Frame: {summary['detection_summary']['avg_persons_per_frame']}\n")
            f.write(f"  - Max Persons in Single Frame: {summary['detection_summary']['max_persons_in_frame']}\n")
            f.write(f"  - Frames with Persons: {summary['detection_summary']['frames_with_persons']}/{frame_num}\n\n")
            f.write(f"Processing:\n")
            f.write(f"  - Processing Time: {processing_time:.2f} seconds\n")
            f.write(f"  - Processing FPS: {summary['processing']['fps_processing']}\n")
        
        logger.info(f"Completed {video_name}: {total_persons} persons in {frame_num} frames ({processing_time:.2f}s)")
        
        return summary


def process_video_wrapper(args):
    """Wrapper function for parallel processing."""
    video_path, model_path, use_sahi, confidence, output_dir = args
    processor = VideoProcessor(model_path, use_sahi, confidence)
    return processor.process_video(video_path, output_dir)


def process_videos_parallel(video_paths: List[str], model_path: str, output_dir: str, 
                           use_sahi: bool = True, confidence: float = 0.5) -> List[Dict]:
    """
    Process multiple videos in parallel using PySpark (if Java available) or sequentially.
    
    Args:
        video_paths: List of video file paths
        model_path: Path to YOLO model
        output_dir: Output directory for results
        use_sahi: Whether to use SAHI
        confidence: Detection confidence threshold
        
    Returns:
        List of summary results for each video
    """
    # Try PySpark if available and multiple videos
    if SPARK_AVAILABLE and len(video_paths) > 1:
        try:
            logger.info(f"Attempting to process {len(video_paths)} videos in parallel with PySpark")
            
            conf = SparkConf().setAppName("VideoProcessor").setMaster("local[*]")
            sc = SparkContext(conf=conf)
            sc.setLogLevel("ERROR")
            
            try:
                # Prepare arguments for each video
                video_args = [(vp, model_path, use_sahi, confidence, output_dir) for vp in video_paths]
                
                # Create RDD and process in parallel
                rdd = sc.parallelize(video_args, len(video_paths))
                results = rdd.map(process_video_wrapper).collect()
                
                return results
            finally:
                sc.stop()
        except Exception as e:
            logger.warning(f"PySpark failed (possibly no Java): {e}")
            logger.info("Falling back to sequential processing")
    
    # Sequential processing fallback
    logger.info(f"Processing {len(video_paths)} videos sequentially")
    processor = VideoProcessor(model_path, use_sahi, confidence)
    results = []
    for video_path in video_paths:
        result = processor.process_video(video_path, output_dir)
        results.append(result)
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Process multiple videos for person detection with SAHI and PySpark"
    )
    parser.add_argument(
        "--videos-dir", "-v",
        default=os.path.join(Config.DATA_DIR, "video"),
        help="Directory containing video files"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=os.path.join(Config.OUTPUT_DIR, "results"),
        help="Output directory for results"
    )
    parser.add_argument(
        "--model", "-m",
        default=Config.YOLO_MODEL_PATH,
        help="Path to YOLO model"
    )
    parser.add_argument(
        "--no-sahi",
        action="store_true",
        help="Disable SAHI sliced inference"
    )
    parser.add_argument(
        "--confidence", "-c",
        type=float,
        default=Config.CONFIDENCE_THRESHOLD,
        help="Detection confidence threshold"
    )
    
    args = parser.parse_args()
    
    # Find video files
    videos_dir = args.videos_dir
    video_extensions = ('.mp4', '.avi', '.mov', '.mkv')
    video_paths = []
    
    if os.path.isdir(videos_dir):
        for f in os.listdir(videos_dir):
            if f.lower().endswith(video_extensions):
                video_paths.append(os.path.join(videos_dir, f))
    else:
        logger.error(f"Videos directory not found: {videos_dir}")
        sys.exit(1)
    
    if not video_paths:
        logger.error(f"No video files found in {videos_dir}")
        sys.exit(1)
    
    video_paths.sort()
    logger.info(f"Found {len(video_paths)} video(s) to process:")
    for vp in video_paths:
        logger.info(f"  - {os.path.basename(vp)}")
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Process videos
    use_sahi = not args.no_sahi
    logger.info(f"Detection mode: {'SAHI + YOLO' if use_sahi else 'YOLO only'}")
    
    start_time = datetime.now()
    results = process_videos_parallel(
        video_paths, 
        args.model, 
        args.output_dir,
        use_sahi,
        args.confidence
    )
    total_time = (datetime.now() - start_time).total_seconds()
    
    # Create overall summary
    overall_summary = {
        "processing_info": {
            "total_videos": len(video_paths),
            "total_processing_time": round(total_time, 2),
            "detection_method": "SAHI + YOLO" if use_sahi else "YOLO",
            "spark_enabled": SPARK_AVAILABLE,
            "timestamp": datetime.now().isoformat()
        },
        "videos": results
    }
    
    summary_file = os.path.join(args.output_dir, "overall_summary.json")
    with open(summary_file, "w") as f:
        json.dump(overall_summary, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 60)
    print("  VIDEO PROCESSING COMPLETE")
    print("=" * 60)
    print(f"Videos Processed: {len(video_paths)}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Detection Method: {'SAHI + YOLO' if use_sahi else 'YOLO'}")
    print(f"Output Directory: {args.output_dir}")
    print("\nPer-Video Results:")
    for r in results:
        if "error" not in r:
            print(f"  - {r['video_name']}: {r['detection_summary']['total_detections']} persons detected")
    print("=" * 60)


if __name__ == "__main__":
    main()
