package lab05;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Storage Server - Persists detection results.
 * 
 * Responsibilities:
 * - Receive detection results from ProcessingServer
 * - Store results in JSON format
 * - Provide statistics and summaries
 * 
 * Output:
 * - output/results/detections.json - All detection results
 */
public class StorageServer {
    private static final Logger logger = LoggerFactory.getLogger(StorageServer.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    private final int port;
    private final String outputFile;
    
    private ServerSocket serverSocket;
    private ExecutorService executor;
    private volatile boolean running = false;
    
    private final List<DataModels.DetectionResult> results = new CopyOnWriteArrayList<>();
    
    public StorageServer() {
        this(Config.STORAGE_PORT, Config.RESULTS_FILE);
    }
    
    public StorageServer(int port, String outputFile) {
        this.port = port;
        this.outputFile = outputFile;
        this.executor = Executors.newCachedThreadPool();
    }
    
    /**
     * Start the storage server.
     */
    public void start() {
        try {
            // Initialize storage
            initStorage();
            
            serverSocket = new ServerSocket(port);
            running = true;
            
            logger.info("========================================");
            logger.info("Storage Server started on port {}", port);
            logger.info("Results will be saved to: {}", outputFile);
            logger.info("========================================");
            
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    logger.info("ProcessingServer connected from {}", 
                            clientSocket.getRemoteSocketAddress());
                    executor.submit(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    if (running) {
                        logger.error("Error accepting connection: {}", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Failed to start server: {}", e.getMessage());
        }
    }
    
    /**
     * Initialize storage directory and load existing results.
     */
    private void initStorage() {
        try {
            // Create output directory
            Files.createDirectories(Paths.get(outputFile).getParent());
            
            // Load existing results if file exists
            File file = new File(outputFile);
            if (file.exists()) {
                String content = new String(Files.readAllBytes(file.toPath()));
                List<DataModels.DetectionResult> existing = gson.fromJson(
                        content, 
                        new TypeToken<List<DataModels.DetectionResult>>(){}.getType());
                if (existing != null) {
                    results.addAll(existing);
                    logger.info("Loaded {} existing results from storage", results.size());
                }
            }
        } catch (Exception e) {
            logger.warn("Could not load existing results: {}", e.getMessage());
        }
    }
    
    /**
     * Handle incoming results from ProcessingServer.
     */
    private void handleClient(Socket clientSocket) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()))) {
            
            String line;
            while (running && (line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    processMessage(line);
                }
            }
        } catch (IOException e) {
            logger.debug("Client disconnected: {}", e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException ignored) {}
        }
    }
    
    /**
     * Process incoming message.
     */
    private void processMessage(String jsonLine) {
        try {
            JsonObject msgObj = gson.fromJson(jsonLine, JsonObject.class);
            String msgType = msgObj.has("type") ? msgObj.get("type").getAsString() : "";
            
            if (Config.MSG_TYPE_RESULT.equals(msgType)) {
                JsonObject dataObj = msgObj.getAsJsonObject("data");
                DataModels.DetectionResult result = gson.fromJson(
                        dataObj, DataModels.DetectionResult.class);
                storeResult(result);
            }
        } catch (Exception e) {
            logger.error("Error processing message: {}", e.getMessage());
        }
    }
    
    /**
     * Store a detection result.
     */
    private void storeResult(DataModels.DetectionResult result) {
        results.add(result);
        
        // Persist to file
        saveToFile();
        
        // Print summary
        printSummary(result);
    }
    
    /**
     * Save results to JSON file.
     */
    private synchronized void saveToFile() {
        try {
            String json = gson.toJson(results);
            Files.write(Paths.get(outputFile), json.getBytes());
        } catch (IOException e) {
            logger.error("Error saving to file: {}", e.getMessage());
        }
    }
    
    /**
     * Print detection summary to console.
     */
    private void printSummary(DataModels.DetectionResult result) {
        System.out.println();
        System.out.println("============================================================");
        System.out.println("DETECTION RESULT - Frame: " + result.frameId.substring(0, 8) + "...");
        System.out.println("============================================================");
        System.out.println("Timestamp      : " + result.timestamp);
        System.out.println("Persons Detected: " + result.personCount);
        System.out.println("Processing Time : " + result.processingTimeMs + "ms");
        
        if (result.boundingBoxes != null && !result.boundingBoxes.isEmpty()) {
            System.out.println("\nBounding Boxes:");
            for (int i = 0; i < result.boundingBoxes.size(); i++) {
                DataModels.BoundingBox box = result.boundingBoxes.get(i);
                System.out.printf("  %d. x=%d, y=%d, w=%d, h=%d, conf=%.3f%n",
                        i + 1, box.x, box.y, box.width, box.height, box.confidence);
            }
        }
        
        System.out.println("\nTotal stored results: " + results.size());
        System.out.println("============================================================");
        System.out.println();
    }
    
    /**
     * Get statistics about stored results.
     */
    public DataModels.StorageStats getStatistics() {
        DataModels.StorageStats stats = new DataModels.StorageStats();
        stats.totalFrames = results.size();
        
        if (!results.isEmpty()) {
            int totalPersons = 0;
            double totalTime = 0;
            
            for (DataModels.DetectionResult r : results) {
                totalPersons += r.personCount;
                totalTime += r.processingTimeMs;
            }
            
            stats.totalPersonsDetected = totalPersons;
            stats.avgPersonsPerFrame = (double) totalPersons / results.size();
            stats.avgProcessingTimeMs = totalTime / results.size();
        }
        
        return stats;
    }
    
    /**
     * Stop the storage server.
     */
    public void stop() {
        running = false;
        executor.shutdown();
        
        // Final save
        saveToFile();
        
        // Print final statistics
        DataModels.StorageStats stats = getStatistics();
        logger.info("Final statistics: {}", stats);
        
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        
        logger.info("Storage Server stopped");
    }
    
    public static void main(String[] args) {
        StorageServer server = new StorageServer();
        
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        
        server.start();
    }
}
