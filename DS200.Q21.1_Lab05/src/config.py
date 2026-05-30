"""
Configuration module for the Real-Time Person Counting System.
Contains all server settings and constants.
"""

import os

# Get project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Config:
    """Central configuration for all servers."""
    
    # Server hostnames
    HOST = "localhost"
    
    # Server ports
    RECEIVER_PORT = 6100      # Frame receiver listens here
    PROCESSING_PORT = 6200    # Processing/detector server listens here
    STORAGE_PORT = 6300       # Storage server listens here
    
    # Buffer sizes
    BUFFER_SIZE = 65536       # 64KB buffer for frame data
    
    # Spark Streaming settings
    SPARK_APP_NAME = "PersonCountingStream"
    SPARK_MASTER = "local[*]"
    SPARK_BATCH_INTERVAL = 1  # seconds
    
    # Detection settings
    CONFIDENCE_THRESHOLD = 0.5
    YOLO_MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "yolo", "yolo12n.pt")
    
    # Storage settings
    STORAGE_FILE = os.path.join(PROJECT_ROOT, "output", "detections.json")
    
    # Data directories
    DATA_DIR = os.path.join(PROJECT_ROOT, "data")
    OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
    
    # Logging
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class MessageType:
    """Message types for inter-server communication."""
    FRAME = "frame"
    DETECTION_RESULT = "detection_result"
    HEARTBEAT = "heartbeat"
    ERROR = "error"


# Ensure output directory exists
os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
