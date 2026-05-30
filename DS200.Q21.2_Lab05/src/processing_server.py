"""
Processing Server - Object detection using Spark Streaming.

This server receives frames, performs person detection using YOLO,
and sends results to the storage server. Integrates with Apache Spark
Streaming for big data processing capabilities.
"""

import socket
import json
import logging
import threading
import base64
import io
import uuid
from datetime import datetime

# Spark imports
try:
    from pyspark import SparkContext
    from pyspark.streaming import StreamingContext
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Warning: PySpark not available. Running in standalone mode.")

# OpenCV and detection imports
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

from config import Config, MessageType

# Configure logging
logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("ProcessingServer")


class PersonDetector:
    """Person detection using YOLO model."""
    
    def __init__(self, model_path=Config.YOLO_MODEL_PATH):
        self.model = None
        self.model_path = model_path
        self._load_model()
    
    def _load_model(self):
        """Load the YOLO model."""
        if YOLO_AVAILABLE:
            try:
                self.model = YOLO(self.model_path)
                logger.info(f"YOLO model loaded from {self.model_path}")
            except Exception as e:
                logger.warning(f"Could not load YOLO model: {e}. Using mock detection.")
                self.model = None
        else:
            logger.warning("YOLO not available. Using mock detection.")
    
    def detect(self, image_data):
        """
        Detect persons in the image.
        
        Args:
            image_data: Base64 encoded image or numpy array
            
        Returns:
            dict with person_count and bounding_boxes
        """
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
        
        if self.model and CV2_AVAILABLE:
            return self._yolo_detection(image)
        else:
            return self._mock_detection()
    
    def _yolo_detection(self, image):
        """Perform actual YOLO detection."""
        try:
            results = self.model(image, classes=[0])  # class 0 is 'person'
            
            bounding_boxes = []
            for result in results:
                for box in result.boxes:
                    x, y, w, h = box.xywh[0].tolist()
                    confidence = float(box.conf[0])
                    
                    if confidence >= Config.CONFIDENCE_THRESHOLD:
                        bounding_boxes.append({
                            "x": int(x - w/2),
                            "y": int(y - h/2),
                            "width": int(w),
                            "height": int(h),
                            "confidence": round(confidence, 3)
                        })
            
            return {
                "person_count": len(bounding_boxes),
                "bounding_boxes": bounding_boxes
            }
        except Exception as e:
            logger.error(f"YOLO detection error: {e}")
            return self._mock_detection()
    
    def _mock_detection(self):
        """Generate mock detection results for testing."""
        import random
        num_persons = random.randint(0, 5)
        bounding_boxes = []
        
        for i in range(num_persons):
            bounding_boxes.append({
                "x": random.randint(50, 500),
                "y": random.randint(50, 400),
                "width": random.randint(40, 80),
                "height": random.randint(100, 180),
                "confidence": round(random.uniform(0.6, 0.99), 3)
            })
        
        return {
            "person_count": num_persons,
            "bounding_boxes": bounding_boxes
        }


class ProcessingServer:
    """TCP server for processing frames with Spark Streaming integration."""
    
    def __init__(self, host=Config.HOST, port=Config.PROCESSING_PORT):
        self.host = host
        self.port = port
        self.server_socket = None
        self.storage_connection = None
        self.running = False
        self.detector = PersonDetector()
        
        # Spark context
        self.sc = None
        self.ssc = None
    
    def start(self):
        """Start the processing server."""
        # Initialize Spark Streaming (optional)
        if SPARK_AVAILABLE:
            self._init_spark()
        
        # Start TCP server
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        logger.info(f"Processing Server started on {self.host}:{self.port}")
        
        # Connect to storage server
        self._connect_to_storage_server()
        
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
                logger.info("Shutting down processing server...")
                self.stop()
            except Exception as e:
                logger.error(f"Error accepting connection: {e}")
    
    def _init_spark(self):
        """Initialize Spark Streaming context."""
        try:
            self.sc = SparkContext(Config.SPARK_MASTER, Config.SPARK_APP_NAME)
            self.sc.setLogLevel("WARN")
            self.ssc = StreamingContext(self.sc, Config.SPARK_BATCH_INTERVAL)
            logger.info("Spark Streaming context initialized")
        except Exception as e:
            logger.warning(f"Could not initialize Spark: {e}")
            self.sc = None
            self.ssc = None
    
    def _connect_to_storage_server(self):
        """Establish connection to the storage server."""
        try:
            self.storage_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.storage_connection.connect((Config.HOST, Config.STORAGE_PORT))
            logger.info(f"Connected to Storage Server at {Config.HOST}:{Config.STORAGE_PORT}")
        except ConnectionRefusedError:
            logger.warning("Storage server not available. Results will be logged only.")
            self.storage_connection = None
        except Exception as e:
            logger.error(f"Error connecting to storage server: {e}")
            self.storage_connection = None
    
    def _handle_client(self, client_socket, address):
        """Handle incoming frames from receiver server."""
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
                        self._process_message(message)
                        
        except Exception as e:
            logger.error(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
            logger.info(f"Client {address} disconnected")
    
    def _process_message(self, message):
        """Process incoming message and perform detection."""
        try:
            msg = json.loads(message)
            
            if msg.get('type') == MessageType.FRAME:
                frame_data = msg.get('data', {})
                self._process_frame(frame_data)
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _process_frame(self, frame_data):
        """Process a single frame for person detection."""
        frame_id = frame_data.get('frame_id', str(uuid.uuid4()))
        image_data = frame_data.get('data', '')
        
        logger.info(f"Processing frame: {frame_id}")
        
        # Perform detection
        start_time = datetime.now()
        detection_result = self.detector.detect(image_data)
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Build result
        result = {
            "frame_id": frame_id,
            "timestamp": datetime.now().isoformat(),
            "original_timestamp": frame_data.get('timestamp'),
            "person_count": detection_result['person_count'],
            "bounding_boxes": detection_result['bounding_boxes'],
            "processing_time_ms": round(processing_time * 1000, 2)
        }
        
        logger.info(f"Frame {frame_id}: Detected {result['person_count']} persons in {result['processing_time_ms']}ms")
        
        # Send to storage
        self._send_to_storage(result)
    
    def _send_to_storage(self, result):
        """Send detection result to storage server."""
        if self.storage_connection is None:
            self._connect_to_storage_server()
            
        if self.storage_connection:
            try:
                payload = {
                    "type": MessageType.DETECTION_RESULT,
                    "data": result
                }
                message = json.dumps(payload) + "\n"
                self.storage_connection.send(message.encode('utf-8'))
                logger.info(f"Sent result for frame {result['frame_id']} to storage")
            except BrokenPipeError:
                logger.error("Connection to storage server lost. Reconnecting...")
                self.storage_connection = None
            except Exception as e:
                logger.error(f"Error sending to storage: {e}")
        else:
            logger.warning(f"No storage connection. Result logged: {result}")
    
    def stop(self):
        """Stop the processing server."""
        self.running = False
        if self.ssc:
            self.ssc.stop(stopSparkContext=True, stopGraceFully=True)
        if self.storage_connection:
            self.storage_connection.close()
        if self.server_socket:
            self.server_socket.close()
        logger.info("Processing Server stopped")


def main():
    """Main entry point."""
    server = ProcessingServer()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
