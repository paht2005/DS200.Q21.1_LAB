"""
Detect Object - Object detection server using YOLO with PySpark Streaming.

Receives frames from receiver, performs person detection using YOLO,
and sends results to the storage server.

Usage:
    python detect_object.py [--port PORT]
"""

import socket
import json
import logging
import threading
import base64
import io
import uuid
import argparse
from datetime import datetime

# PySpark imports
try:
    from pyspark import SparkContext
    from pyspark.streaming import StreamingContext
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Warning: PySpark not available. Running in standalone mode.")

# OpenCV imports
try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    print("Warning: OpenCV not available. Using mock detection.")

# YOLO imports
try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False
    print("Warning: Ultralytics YOLO not available. Using mock detection.")

# SAHI imports (Slicing Aided Hyper Inference)
try:
    from sahi import AutoDetectionModel
    from sahi.predict import get_sliced_prediction
    SAHI_AVAILABLE = True
except ImportError:
    SAHI_AVAILABLE = False
    print("Warning: SAHI not available. Using standard YOLO detection.")

from config import Config, MessageType

# Configure logging
logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("Detector")


class PersonDetector:
    """Person detection using YOLO model with optional SAHI sliced inference."""
    
    def __init__(self, model_path=Config.YOLO_MODEL_PATH, confidence=Config.CONFIDENCE_THRESHOLD, use_sahi=True):
        self.model = None
        self.sahi_model = None
        self.model_path = model_path
        self.confidence_threshold = confidence
        self.use_sahi = use_sahi and SAHI_AVAILABLE
        self._load_model()
    
    def _load_model(self):
        """Load the YOLO model and optionally SAHI wrapper."""
        if YOLO_AVAILABLE:
            try:
                self.model = YOLO(self.model_path)
                logger.info(f"YOLO model loaded from {self.model_path}")
                
                # Initialize SAHI model wrapper if available
                if self.use_sahi and SAHI_AVAILABLE:
                    self.sahi_model = AutoDetectionModel.from_pretrained(
                        model_type="yolov8",
                        model_path=self.model_path,
                        confidence_threshold=self.confidence_threshold,
                        device="cpu"  # Use "cuda:0" for GPU
                    )
                    logger.info("SAHI sliced inference enabled")
            except Exception as e:
                logger.warning(f"Could not load YOLO model: {e}")
                logger.info("Using mock detection instead")
                self.model = None
                self.sahi_model = None
        else:
            logger.warning("YOLO not available. Using mock detection.")
    
    def detect(self, image_data):
        """
        Detect persons in the image using SAHI sliced inference or standard YOLO.
        
        Args:
            image_data: Base64 encoded image string or numpy array
            
        Returns:
            dict with person_count and bounding_boxes
        """
        image = None
        
        if isinstance(image_data, str):
            # Decode base64 image
            try:
                image_bytes = base64.b64decode(image_data)
                nparr = np.frombuffer(image_bytes, np.uint8)
                image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            except Exception as e:
                logger.error(f"Error decoding image: {e}")
                return self._mock_detection()
        else:
            image = image_data
        
        if image is None:
            return self._mock_detection()
        
        if self.sahi_model and CV2_AVAILABLE:
            return self._sahi_detection(image)
        elif self.model and CV2_AVAILABLE:
            return self._yolo_detection(image)
        else:
            return self._mock_detection()
    
    def _sahi_detection(self, image):
        """Perform SAHI sliced inference for better small object detection."""
        try:
            # Run sliced prediction - slices the image into overlapping patches
            result = get_sliced_prediction(
                image,
                self.sahi_model,
                slice_height=256,
                slice_width=256,
                overlap_height_ratio=0.2,
                overlap_width_ratio=0.2,
                postprocess_type="NMS",
                postprocess_match_threshold=0.5,
                verbose=0
            )
            
            bounding_boxes = []
            person_count = 0
            
            for prediction in result.object_prediction_list:
                # Filter for person class (class 0 in COCO)
                if prediction.category.id == 0:
                    bbox = prediction.bbox
                    confidence = prediction.score.value
                    
                    bounding_boxes.append({
                        "x": int(bbox.minx),
                        "y": int(bbox.miny),
                        "width": int(bbox.maxx - bbox.minx),
                        "height": int(bbox.maxy - bbox.miny),
                        "confidence": round(confidence, 3),
                        "class": "person"
                    })
                    person_count += 1
            
            return {
                "person_count": person_count,
                "bounding_boxes": bounding_boxes,
                "detection_method": "SAHI"
            }
            
        except Exception as e:
            logger.error(f"SAHI detection error: {e}")
            # Fall back to standard YOLO
            return self._yolo_detection(image)
    
    def _yolo_detection(self, image):
        """Perform actual YOLO detection."""
        try:
            # Run inference
            results = self.model(image, verbose=False)
            
            bounding_boxes = []
            person_count = 0
            
            for result in results:
                boxes = result.boxes
                for box in boxes:
                    # Class 0 is 'person' in COCO dataset
                    if int(box.cls[0]) == 0 and float(box.conf[0]) >= self.confidence_threshold:
                        x1, y1, x2, y2 = box.xyxy[0].tolist()
                        confidence = float(box.conf[0])
                        
                        bounding_boxes.append({
                            "x": int(x1),
                            "y": int(y1),
                            "width": int(x2 - x1),
                            "height": int(y2 - y1),
                            "confidence": round(confidence, 3),
                            "class": "person"
                        })
                        person_count += 1
            
            return {
                "person_count": person_count,
                "bounding_boxes": bounding_boxes,
                "detection_method": "YOLO"
            }
            
        except Exception as e:
            logger.error(f"YOLO detection error: {e}")
            return self._mock_detection()
    
    def _mock_detection(self):
        """Return mock detection result for testing."""
        import random
        
        count = random.randint(0, 3)
        boxes = []
        
        for i in range(count):
            boxes.append({
                "x": random.randint(50, 400),
                "y": random.randint(50, 300),
                "width": random.randint(40, 80),
                "height": random.randint(100, 180),
                "confidence": round(random.uniform(0.6, 0.95), 3),
                "class": "person"
            })
        
        return {
            "person_count": count,
            "bounding_boxes": boxes,
            "detection_method": "mock"
        }


class ObjectDetectionServer:
    """TCP server for object detection with optional PySpark Streaming and SAHI."""
    
    def __init__(self, host=Config.HOST, port=Config.PROCESSING_PORT, use_spark=False, use_sahi=True):
        self.host = host
        self.port = port
        self.use_spark = use_spark and SPARK_AVAILABLE
        self.use_sahi = use_sahi
        self.server_socket = None
        self.storage_connection = None
        self.running = False
        self.detector = PersonDetector(use_sahi=use_sahi)
        self.frame_count = 0
        
        # Spark context
        self.sc = None
        self.ssc = None
        
        if self.use_spark:
            self._init_spark()
    
    def _init_spark(self):
        """Initialize Spark Streaming context."""
        try:
            self.sc = SparkContext(Config.SPARK_MASTER, Config.SPARK_APP_NAME)
            self.sc.setLogLevel("ERROR")
            self.ssc = StreamingContext(self.sc, Config.SPARK_BATCH_INTERVAL)
            logger.info("PySpark Streaming context initialized")
        except Exception as e:
            logger.warning(f"Could not initialize Spark: {e}")
            self.use_spark = False
    
    def _connect_to_storage(self):
        """Connect to storage server."""
        try:
            self.storage_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.storage_connection.connect((Config.HOST, Config.STORAGE_PORT))
            logger.info(f"Connected to Storage at {Config.HOST}:{Config.STORAGE_PORT}")
        except ConnectionRefusedError:
            logger.warning("Storage server not available. Run storage_server.py first.")
            self.storage_connection = None
        except Exception as e:
            logger.error(f"Error connecting to storage: {e}")
            self.storage_connection = None
    
    def start(self):
        """Start the detection server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        logger.info("=" * 60)
        logger.info(f"  DETECTOR SERVER started on {self.host}:{self.port}")
        logger.info("=" * 60)
        logger.info(f"Detection method: {'SAHI + YOLO' if self.detector.sahi_model else ('YOLO' if self.detector.model else 'Mock')}")
        logger.info(f"Spark Streaming: {'Enabled' if self.use_spark else 'Disabled'}")
        logger.info(f"SAHI Sliced Inference: {'Enabled' if self.detector.sahi_model else 'Disabled'}")
        logger.info("Waiting for receiver to connect...")
        
        # Connect to storage
        self._connect_to_storage()
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                logger.info(f"Receiver connected from {address}")
                
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
            except KeyboardInterrupt:
                logger.info("Shutting down detector...")
                self.stop()
            except Exception as e:
                logger.error(f"Error accepting connection: {e}")
    
    def _handle_client(self, client_socket, address):
        """Handle incoming frames from receiver."""
        buffer = ""
        
        try:
            while self.running:
                data = client_socket.recv(Config.BUFFER_SIZE)
                if not data:
                    break
                    
                buffer += data.decode('utf-8')
                
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    if message.strip():
                        self._process_frame(message)
                        
        except Exception as e:
            logger.error(f"Error handling receiver {address}: {e}")
        finally:
            client_socket.close()
            logger.info(f"Receiver {address} disconnected")
    
    def _process_frame(self, message):
        """Process frame and perform detection."""
        try:
            frame_data = json.loads(message)
            self.frame_count += 1
            
            frame_id = frame_data.get("frame_id", str(uuid.uuid4()))
            frame_num = frame_data.get("frame_number", self.frame_count)
            image_data = frame_data.get("data")
            
            logger.info(f"Processing frame #{frame_num}...")
            
            # Perform detection
            start_time = datetime.now()
            detection_result = self.detector.detect(image_data)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Create result payload
            result = {
                "type": MessageType.DETECTION_RESULT,
                "frame_id": frame_id,
                "frame_number": frame_num,
                "timestamp": datetime.now().isoformat(),
                "processing_time_ms": round(processing_time * 1000, 2),
                "detection": detection_result
            }
            
            person_count = detection_result.get("person_count", 0)
            method = detection_result.get("detection_method", "unknown")
            
            logger.info(f"  → Detected {person_count} person(s) [{method}] in {processing_time*1000:.1f}ms")
            
            # Send to storage
            if self.storage_connection:
                try:
                    result_message = json.dumps(result) + "\n"
                    self.storage_connection.send(result_message.encode('utf-8'))
                    logger.debug(f"Sent result to storage")
                except BrokenPipeError:
                    logger.warning("Lost connection to storage. Reconnecting...")
                    self._connect_to_storage()
                except Exception as e:
                    logger.error(f"Error sending to storage: {e}")
            else:
                # Try to reconnect periodically
                if self.frame_count % 10 == 0:
                    self._connect_to_storage()
                    
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing frame: {e}")
    
    def stop(self):
        """Stop the detection server."""
        self.running = False
        
        if self.ssc:
            try:
                self.ssc.stop(stopSparkContext=True, stopGraceFully=True)
            except:
                pass
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        if self.storage_connection:
            try:
                self.storage_connection.close()
            except:
                pass
        
        logger.info(f"Detector stopped. Total frames processed: {self.frame_count}")


def main():
    parser = argparse.ArgumentParser(description="Object detection server with YOLO, SAHI and PySpark Streaming")
    parser.add_argument("--host", default=Config.HOST, help=f"Host to bind (default: {Config.HOST})")
    parser.add_argument("--port", "-p", type=int, default=Config.PROCESSING_PORT,
                        help=f"Port to listen on (default: {Config.PROCESSING_PORT})")
    parser.add_argument("--no-spark", action="store_true", help="Disable PySpark Streaming (standalone mode)")
    parser.add_argument("--no-sahi", action="store_true", help="Disable SAHI sliced inference (use standard YOLO)")
    
    args = parser.parse_args()
    
    # PySpark Streaming enabled by default
    use_spark = not args.no_spark
    use_sahi = not args.no_sahi
    
    server = ObjectDetectionServer(
        host=args.host,
        port=args.port,
        use_spark=use_spark,
        use_sahi=use_sahi
    )
    
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
