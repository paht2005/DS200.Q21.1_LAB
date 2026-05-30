package lab05;

import java.util.List;

/**
 * Data models for inter-server communication.
 * Uses GSON for JSON serialization/deserialization.
 */
public class DataModels {

    /**
     * Frame data sent from camera/video source to ReceiverServer.
     */
    public static class FrameData {
        public String frameId;
        public String timestamp;
        public int frameNumber;
        public String data; // Base64 encoded image data
        public int width;
        public int height;
        
        public FrameData() {}
        
        public FrameData(String frameId, String timestamp, int frameNumber, String data) {
            this.frameId = frameId;
            this.timestamp = timestamp;
            this.frameNumber = frameNumber;
            this.data = data;
        }
    }

    /**
     * Bounding box for a detected person.
     */
    public static class BoundingBox {
        public int x;
        public int y;
        public int width;
        public int height;
        public double confidence;
        
        public BoundingBox() {}
        
        public BoundingBox(int x, int y, int width, int height, double confidence) {
            this.x = x;
            this.y = y;
            this.width = width;
            this.height = height;
            this.confidence = confidence;
        }
        
        @Override
        public String toString() {
            return String.format("BBox[x=%d, y=%d, w=%d, h=%d, conf=%.3f]", 
                    x, y, width, height, confidence);
        }
    }

    /**
     * Detection result sent from ProcessingServer to StorageServer.
     */
    public static class DetectionResult {
        public String frameId;
        public String timestamp;
        public String originalTimestamp;
        public int personCount;
        public List<BoundingBox> boundingBoxes;
        public double processingTimeMs;
        
        public DetectionResult() {}
        
        @Override
        public String toString() {
            return String.format("DetectionResult[frameId=%s, persons=%d, time=%.2fms]",
                    frameId, personCount, processingTimeMs);
        }
    }

    /**
     * Generic message wrapper for TCP communication.
     */
    public static class Message {
        public String type;
        public Object data;
        
        public Message() {}
        
        public Message(String type, Object data) {
            this.type = type;
            this.data = data;
        }
    }

    /**
     * Storage statistics.
     */
    public static class StorageStats {
        public int totalFrames;
        public int totalPersonsDetected;
        public double avgPersonsPerFrame;
        public double avgProcessingTimeMs;
        
        @Override
        public String toString() {
            return String.format(
                "Stats[frames=%d, totalPersons=%d, avgPerFrame=%.2f, avgTime=%.2fms]",
                totalFrames, totalPersonsDetected, avgPersonsPerFrame, avgProcessingTimeMs);
        }
    }
}
