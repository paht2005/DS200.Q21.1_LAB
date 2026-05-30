"""
Lab05 - Optional Python Implementation
Configuration module for the Real-Time Person Counting System.
"""


class Config:
    """Central configuration for all servers."""
    
    # Server hostnames
    HOST = "localhost"
    
    # Server ports (same as Java implementation)
    RECEIVER_PORT = 6100      # Frame receiver listens here
    PROCESSING_PORT = 6200    # Processing server listens here
    STORAGE_PORT = 6300       # Storage server listens here
    
    # Buffer sizes
    BUFFER_SIZE = 65536       # 64KB buffer for frame data
    
    # Spark Streaming settings
    SPARK_APP_NAME = "PersonCountingStream"
    SPARK_MASTER = "local[*]"
    SPARK_BATCH_INTERVAL = 1  # seconds
    
    # Detection settings
    CONFIDENCE_THRESHOLD = 0.5
    YOLO_MODEL_PATH = "models/yolo/yolov8n.pt"
    
    # Storage settings
    STORAGE_FILE = "output/results/detections.json"
    
    # Logging
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class MessageType:
    """Message types for inter-server communication."""
    FRAME = "frame"
    DETECTION_RESULT = "detection_result"
    HEARTBEAT = "heartbeat"
    ERROR = "error"
