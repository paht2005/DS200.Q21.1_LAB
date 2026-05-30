package lab05;

/**
 * Configuration constants for the Person Counting System.
 * All servers share these settings for consistency.
 */
public class Config {
    
    // ==================== SERVER PORTS ====================
    /** Port where FrameReceiverServer listens for camera frames */
    public static final int RECEIVER_PORT = 6100;
    
    /** Port where ProcessingServer listens for frames to process */
    public static final int PROCESSING_PORT = 6200;
    
    /** Port where StorageServer listens for detection results */
    public static final int STORAGE_PORT = 6300;
    
    // ==================== NETWORK SETTINGS ====================
    /** Default host for all servers */
    public static final String DEFAULT_HOST = "localhost";
    
    /** Buffer size for TCP socket reading (64KB) */
    public static final int BUFFER_SIZE = 65536;
    
    /** Connection timeout in milliseconds */
    public static final int CONNECTION_TIMEOUT = 5000;
    
    // ==================== SPARK STREAMING SETTINGS ====================
    /** Spark application name */
    public static final String SPARK_APP_NAME = "PersonCountingStream";
    
    /** Spark master URL (local mode with all cores) */
    public static final String SPARK_MASTER = "local[*]";
    
    /** Batch interval in seconds for Spark Streaming */
    public static final int SPARK_BATCH_INTERVAL = 1;
    
    // ==================== DETECTION SETTINGS ====================
    /** Minimum confidence threshold for person detection (0.0 - 1.0) */
    public static final double CONFIDENCE_THRESHOLD = 0.5;
    
    /** YOLO model weights file path */
    public static final String YOLO_WEIGHTS = "models/yolo/yolov4-tiny.weights";
    
    /** YOLO model config file path */
    public static final String YOLO_CONFIG = "models/yolo/yolov4-tiny.cfg";
    
    /** COCO class names file path */
    public static final String COCO_NAMES = "models/yolo/coco.names";
    
    /** Person class ID in COCO dataset */
    public static final int PERSON_CLASS_ID = 0;
    
    // ==================== STORAGE SETTINGS ====================
    /** Output directory for detection results */
    public static final String OUTPUT_DIR = "output/results";
    
    /** Detection results JSON file */
    public static final String RESULTS_FILE = "output/results/detections.json";
    
    // ==================== DATA PATHS ====================
    /** Directory for test images */
    public static final String IMAGES_DIR = "data/images";
    
    /** Directory for test videos */
    public static final String VIDEO_DIR = "data/video";
    
    // ==================== MESSAGE TYPES ====================
    public static final String MSG_TYPE_FRAME = "frame";
    public static final String MSG_TYPE_RESULT = "detection_result";
    public static final String MSG_TYPE_HEARTBEAT = "heartbeat";
}
