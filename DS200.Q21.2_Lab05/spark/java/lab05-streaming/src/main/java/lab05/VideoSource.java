package lab05;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Video/Image Source Simulator.
 * 
 * Sends frames to FrameReceiverServer for testing.
 * Can read from:
 * - Image files in data/images/
 * - Video file (requires OpenCV)
 * - Generate synthetic frames
 */
public class VideoSource {
    private static final Logger logger = LoggerFactory.getLogger(VideoSource.class);
    private static final Gson gson = new Gson();
    
    private final String host;
    private final int port;
    private final double fps;
    
    private Socket socket;
    private PrintWriter writer;
    private volatile boolean running = false;
    
    public VideoSource() {
        this(Config.DEFAULT_HOST, Config.RECEIVER_PORT, 1.0);
    }
    
    public VideoSource(String host, int port, double fps) {
        this.host = host;
        this.port = port;
        this.fps = fps;
    }
    
    /**
     * Connect to FrameReceiverServer.
     */
    public boolean connect() {
        try {
            socket = new Socket(host, port);
            writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            logger.info("Connected to FrameReceiverServer at {}:{}", host, port);
            return true;
        } catch (IOException e) {
            logger.error("Failed to connect to {}:{} - {}", host, port, e.getMessage());
            return false;
        }
    }
    
    /**
     * Stream frames from images in a directory.
     */
    public void streamFromDirectory(String imageDir, int maxFrames) {
        if (!connect()) return;
        
        try {
            List<Path> imageFiles = Files.list(Paths.get(imageDir))
                    .filter(p -> {
                        String name = p.toString().toLowerCase();
                        return name.endsWith(".jpg") || name.endsWith(".jpeg") || 
                               name.endsWith(".png") || name.endsWith(".bmp");
                    })
                    .sorted()
                    .limit(maxFrames > 0 ? maxFrames : Long.MAX_VALUE)
                    .collect(Collectors.toList());
            
            if (imageFiles.isEmpty()) {
                logger.warn("No images found in {}. Using synthetic frames.", imageDir);
                streamSynthetic(maxFrames > 0 ? maxFrames : 10);
                return;
            }
            
            logger.info("Found {} images. Starting stream at {} FPS...", imageFiles.size(), fps);
            running = true;
            long frameInterval = (long) (1000.0 / fps);
            int frameNumber = 0;
            
            for (Path imagePath : imageFiles) {
                if (!running) break;
                
                long startTime = System.currentTimeMillis();
                
                sendImageFile(imagePath, frameNumber++);
                
                // Maintain FPS
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed < frameInterval) {
                    Thread.sleep(frameInterval - elapsed);
                }
            }
            
            logger.info("Streaming complete. Sent {} frames.", frameNumber);
            
        } catch (Exception e) {
            logger.error("Error streaming from directory: {}", e.getMessage());
        } finally {
            disconnect();
        }
    }
    
    /**
     * Stream synthetic frames for testing.
     */
    public void streamSynthetic(int numFrames) {
        if (socket == null && !connect()) return;
        
        logger.info("Generating {} synthetic frames at {} FPS...", numFrames, fps);
        running = true;
        long frameInterval = (long) (1000.0 / fps);
        
        try {
            for (int i = 0; i < numFrames && running; i++) {
                long startTime = System.currentTimeMillis();
                
                sendSyntheticFrame(i);
                
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed < frameInterval) {
                    Thread.sleep(frameInterval - elapsed);
                }
            }
            
            logger.info("Streaming complete. Sent {} synthetic frames.", numFrames);
            
        } catch (InterruptedException e) {
            logger.info("Streaming interrupted");
        } finally {
            disconnect();
        }
    }
    
    /**
     * Send an image file as a frame.
     */
    private void sendImageFile(Path imagePath, int frameNumber) {
        try {
            // Read image and encode to Base64
            BufferedImage image = ImageIO.read(imagePath.toFile());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(image, "jpg", baos);
            String base64Data = Base64.getEncoder().encodeToString(baos.toByteArray());
            
            // Create frame data
            DataModels.FrameData frame = new DataModels.FrameData();
            frame.frameId = UUID.randomUUID().toString();
            frame.timestamp = Instant.now().toString();
            frame.frameNumber = frameNumber;
            frame.data = base64Data;
            frame.width = image.getWidth();
            frame.height = image.getHeight();
            
            // Send
            String json = gson.toJson(frame);
            writer.println(json);
            
            logger.info("Sent frame #{} from {}", frameNumber, imagePath.getFileName());
            
        } catch (IOException e) {
            logger.error("Error sending image {}: {}", imagePath, e.getMessage());
        }
    }
    
    /**
     * Send a synthetic frame.
     */
    private void sendSyntheticFrame(int frameNumber) {
        DataModels.FrameData frame = new DataModels.FrameData();
        frame.frameId = UUID.randomUUID().toString();
        frame.timestamp = Instant.now().toString();
        frame.frameNumber = frameNumber;
        frame.data = "synthetic_frame_data_" + frameNumber; // Placeholder
        frame.width = 640;
        frame.height = 480;
        
        String json = gson.toJson(frame);
        writer.println(json);
        
        logger.info("Sent synthetic frame #{}", frameNumber);
    }
    
    /**
     * Stop streaming.
     */
    public void stop() {
        running = false;
    }
    
    /**
     * Disconnect from server.
     */
    public void disconnect() {
        try {
            if (writer != null) writer.close();
            if (socket != null) socket.close();
        } catch (IOException ignored) {}
        logger.info("Disconnected from server");
    }
    
    public static void main(String[] args) {
        String imageDir = Config.IMAGES_DIR;
        int numFrames = 10;
        double fps = 1.0;
        
        // Parse arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dir":
                case "-d":
                    imageDir = args[++i];
                    break;
                case "--frames":
                case "-n":
                    numFrames = Integer.parseInt(args[++i]);
                    break;
                case "--fps":
                case "-f":
                    fps = Double.parseDouble(args[++i]);
                    break;
            }
        }
        
        VideoSource source = new VideoSource(Config.DEFAULT_HOST, Config.RECEIVER_PORT, fps);
        
        // Check if image directory exists and has images
        Path imagePath = Paths.get(imageDir);
        if (Files.exists(imagePath) && Files.isDirectory(imagePath)) {
            source.streamFromDirectory(imageDir, numFrames);
        } else {
            logger.info("Image directory not found. Using synthetic frames.");
            source.streamSynthetic(numFrames);
        }
    }
}
