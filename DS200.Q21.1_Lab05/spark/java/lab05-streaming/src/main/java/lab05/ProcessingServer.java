package lab05;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Processing Server with Spark Streaming integration.
 * 
 * Responsibilities:
 * - Receive frames from FrameReceiverServer via custom Receiver
 * - Process frames using Spark Streaming (distributed)
 * - Perform person detection (YOLO or mock)
 * - Send detection results to StorageServer
 * 
 * Big Data Context:
 * - Uses Spark Streaming for scalable stream processing
 * - Can distribute detection workload across cluster
 * - Processes frames in micro-batches for throughput
 */
public class ProcessingServer {
    private static final Logger logger = LoggerFactory.getLogger(ProcessingServer.class);
    private static final Gson gson = new Gson();
    
    private final int port;
    private final String storageHost;
    private final int storagePort;
    
    private JavaStreamingContext jssc;
    private Socket storageSocket;
    private PrintWriter storageWriter;
    
    public ProcessingServer() {
        this(Config.PROCESSING_PORT, Config.DEFAULT_HOST, Config.STORAGE_PORT);
    }
    
    public ProcessingServer(int port, String storageHost, int storagePort) {
        this.port = port;
        this.storageHost = storageHost;
        this.storagePort = storagePort;
    }
    
    /**
     * Start the processing server with Spark Streaming.
     */
    public void start() {
        logger.info("========================================");
        logger.info("Processing Server starting with Spark Streaming");
        logger.info("Listening on port: {}", port);
        logger.info("========================================");
        
        // Connect to storage server
        connectToStorageServer();
        
        // Initialize Spark Streaming
        SparkConf conf = new SparkConf()
                .setAppName(Config.SPARK_APP_NAME)
                .setMaster(Config.SPARK_MASTER)
                .set("spark.streaming.receiver.writeAheadLog.enable", "false")
                .set("spark.ui.enabled", "false");
        
        jssc = new JavaStreamingContext(conf, Durations.seconds(Config.SPARK_BATCH_INTERVAL));
        
        // Create custom receiver for TCP input
        JavaReceiverInputDStream<String> frameStream = jssc.receiverStream(
                new FrameReceiver(port));
        
        // Process each frame
        frameStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                rdd.collect().forEach(this::processFrame);
            }
        });
        
        // Start streaming
        jssc.start();
        logger.info("Spark Streaming started. Waiting for frames...");
        
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.info("Streaming interrupted");
        }
    }
    
    /**
     * Connect to StorageServer.
     */
    private void connectToStorageServer() {
        try {
            storageSocket = new Socket(storageHost, storagePort);
            storageWriter = new PrintWriter(
                new OutputStreamWriter(storageSocket.getOutputStream()), true);
            logger.info("Connected to Storage Server at {}:{}", storageHost, storagePort);
        } catch (IOException e) {
            logger.warn("Storage server not available. Results will be logged only.");
            storageSocket = null;
            storageWriter = null;
        }
    }
    
    /**
     * Process a single frame and perform detection.
     */
    private void processFrame(String jsonLine) {
        try {
            // Parse incoming message
            JsonObject msgObj = gson.fromJson(jsonLine, JsonObject.class);
            String msgType = msgObj.has("type") ? msgObj.get("type").getAsString() : "";
            
            if (!Config.MSG_TYPE_FRAME.equals(msgType)) {
                return;
            }
            
            JsonObject dataObj = msgObj.getAsJsonObject("data");
            DataModels.FrameData frame = gson.fromJson(dataObj, DataModels.FrameData.class);
            
            logger.info("Processing frame: {}", frame.frameId);
            
            long startTime = System.currentTimeMillis();
            
            // Perform detection (mock or real YOLO)
            List<DataModels.BoundingBox> boxes = detectPersons(frame);
            
            long processingTime = System.currentTimeMillis() - startTime;
            
            // Build result
            DataModels.DetectionResult result = new DataModels.DetectionResult();
            result.frameId = frame.frameId;
            result.timestamp = Instant.now().toString();
            result.originalTimestamp = frame.timestamp;
            result.personCount = boxes.size();
            result.boundingBoxes = boxes;
            result.processingTimeMs = processingTime;
            
            logger.info("Frame {}: Detected {} persons in {}ms", 
                    frame.frameId, result.personCount, processingTime);
            
            // Send to storage
            sendToStorage(result);
            
        } catch (Exception e) {
            logger.error("Error processing frame: {}", e.getMessage());
        }
    }
    
    /**
     * Detect persons in frame.
     * Currently uses mock detection. Replace with real YOLO for production.
     */
    private List<DataModels.BoundingBox> detectPersons(DataModels.FrameData frame) {
        // TODO: Integrate real YOLO detection here
        // For now, use mock detection for demonstration
        return mockDetection();
    }
    
    /**
     * Mock detection for testing without YOLO model.
     */
    private List<DataModels.BoundingBox> mockDetection() {
        List<DataModels.BoundingBox> boxes = new ArrayList<>();
        Random random = new Random();
        int numPersons = random.nextInt(6); // 0-5 persons
        
        for (int i = 0; i < numPersons; i++) {
            boxes.add(new DataModels.BoundingBox(
                    random.nextInt(500) + 50,   // x
                    random.nextInt(350) + 50,   // y
                    random.nextInt(40) + 40,    // width
                    random.nextInt(80) + 100,   // height
                    0.6 + random.nextDouble() * 0.39 // confidence 0.6-0.99
            ));
        }
        
        return boxes;
    }
    
    /**
     * Send detection result to StorageServer.
     */
    private synchronized void sendToStorage(DataModels.DetectionResult result) {
        if (storageWriter == null) {
            connectToStorageServer();
        }
        
        if (storageWriter != null) {
            try {
                DataModels.Message message = new DataModels.Message(Config.MSG_TYPE_RESULT, result);
                String json = gson.toJson(message);
                storageWriter.println(json);
                logger.info("Sent result for frame {} to StorageServer", result.frameId);
            } catch (Exception e) {
                logger.error("Error sending to storage: {}", e.getMessage());
                storageWriter = null;
            }
        } else {
            logger.warn("No storage connection. Result: {}", result);
        }
    }
    
    /**
     * Stop the processing server.
     */
    public void stop() {
        if (jssc != null) {
            jssc.stop(true, true);
        }
        try {
            if (storageSocket != null) storageSocket.close();
        } catch (IOException ignored) {}
        
        logger.info("Processing Server stopped");
    }
    
    /**
     * Custom Spark Receiver for TCP frame input.
     */
    public static class FrameReceiver extends Receiver<String> {
        private static final Logger logger = LoggerFactory.getLogger(FrameReceiver.class);
        private final int port;
        
        public FrameReceiver(int port) {
            super(StorageLevel.MEMORY_AND_DISK_2());
            this.port = port;
        }
        
        @Override
        public void onStart() {
            new Thread(this::receive).start();
        }
        
        @Override
        public void onStop() {
            // Cleanup handled by Spark
        }
        
        private void receive() {
            try (java.net.ServerSocket serverSocket = new java.net.ServerSocket(port)) {
                logger.info("FrameReceiver listening on port {}", port);
                
                while (!isStopped()) {
                    Socket socket = serverSocket.accept();
                    logger.info("FrameReceiver accepted connection from {}", 
                            socket.getRemoteSocketAddress());
                    
                    // Handle in separate thread
                    new Thread(() -> handleConnection(socket)).start();
                }
            } catch (Exception e) {
                if (!isStopped()) {
                    restart("Error in receiver: " + e.getMessage());
                }
            }
        }
        
        private void handleConnection(Socket socket) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()))) {
                String line;
                while (!isStopped() && (line = reader.readLine()) != null) {
                    store(line);
                }
            } catch (IOException e) {
                logger.debug("Connection closed: {}", e.getMessage());
            }
        }
    }
    
    public static void main(String[] args) {
        ProcessingServer server = new ProcessingServer();
        
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        
        server.start();
    }
}
