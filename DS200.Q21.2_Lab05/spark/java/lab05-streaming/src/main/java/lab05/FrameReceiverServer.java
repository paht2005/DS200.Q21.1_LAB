package lab05;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Frame Receiver Server - Entry point for camera frames.
 * 
 * Responsibilities:
 * - Listen on RECEIVER_PORT for incoming camera/video connections
 * - Receive frame data via TCP
 * - Forward frames to ProcessingServer
 * 
 * Architecture:
 * [Camera/Video] --TCP--> [FrameReceiverServer] --TCP--> [ProcessingServer]
 */
public class FrameReceiverServer {
    private static final Logger logger = LoggerFactory.getLogger(FrameReceiverServer.class);
    private static final Gson gson = new Gson();
    
    private final int port;
    private final String processingHost;
    private final int processingPort;
    
    private ServerSocket serverSocket;
    private Socket processingSocket;
    private PrintWriter processingWriter;
    private ExecutorService executor;
    private volatile boolean running = false;
    
    public FrameReceiverServer() {
        this(Config.RECEIVER_PORT, Config.DEFAULT_HOST, Config.PROCESSING_PORT);
    }
    
    public FrameReceiverServer(int port, String processingHost, int processingPort) {
        this.port = port;
        this.processingHost = processingHost;
        this.processingPort = processingPort;
        this.executor = Executors.newCachedThreadPool();
    }
    
    /**
     * Start the receiver server.
     */
    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            running = true;
            
            logger.info("========================================");
            logger.info("Frame Receiver Server started on port {}", port);
            logger.info("Waiting for frame sources to connect...");
            logger.info("========================================");
            
            // Connect to processing server
            connectToProcessingServer();
            
            // Accept client connections
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    logger.info("Frame source connected from {}", clientSocket.getRemoteSocketAddress());
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
     * Connect to the ProcessingServer.
     */
    private void connectToProcessingServer() {
        try {
            processingSocket = new Socket(processingHost, processingPort);
            processingWriter = new PrintWriter(
                new OutputStreamWriter(processingSocket.getOutputStream()), true);
            logger.info("Connected to Processing Server at {}:{}", processingHost, processingPort);
        } catch (IOException e) {
            logger.warn("Processing server not available at {}:{}. Will retry on demand.",
                    processingHost, processingPort);
            processingSocket = null;
            processingWriter = null;
        }
    }
    
    /**
     * Handle incoming frames from a client.
     */
    private void handleClient(Socket clientSocket) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()))) {
            
            String line;
            while (running && (line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    processFrame(line);
                }
            }
        } catch (IOException e) {
            logger.debug("Client disconnected: {}", e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException ignored) {}
            logger.info("Client {} disconnected", clientSocket.getRemoteSocketAddress());
        }
    }
    
    /**
     * Process received frame and forward to ProcessingServer.
     */
    private void processFrame(String jsonLine) {
        try {
            DataModels.FrameData frame = gson.fromJson(jsonLine, DataModels.FrameData.class);
            
            // Add receiver timestamp
            String receiverTimestamp = Instant.now().toString();
            
            logger.info("Received frame: {} (frame #{})", frame.frameId, frame.frameNumber);
            
            // Forward to processing server
            forwardToProcessing(frame);
            
        } catch (Exception e) {
            logger.error("Error processing frame: {}", e.getMessage());
        }
    }
    
    /**
     * Forward frame to ProcessingServer.
     */
    private synchronized void forwardToProcessing(DataModels.FrameData frame) {
        // Reconnect if needed
        if (processingWriter == null) {
            connectToProcessingServer();
        }
        
        if (processingWriter != null) {
            try {
                DataModels.Message message = new DataModels.Message(Config.MSG_TYPE_FRAME, frame);
                String json = gson.toJson(message);
                processingWriter.println(json);
                logger.info("Forwarded frame {} to ProcessingServer", frame.frameId);
            } catch (Exception e) {
                logger.error("Error forwarding frame: {}", e.getMessage());
                processingWriter = null; // Trigger reconnect on next call
            }
        } else {
            logger.warn("No connection to ProcessingServer. Frame {} dropped.", frame.frameId);
        }
    }
    
    /**
     * Stop the server.
     */
    public void stop() {
        running = false;
        executor.shutdown();
        
        try {
            if (processingSocket != null) processingSocket.close();
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
        
        logger.info("Frame Receiver Server stopped");
    }
    
    public static void main(String[] args) {
        FrameReceiverServer server = new FrameReceiverServer();
        
        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        
        server.start();
    }
}
