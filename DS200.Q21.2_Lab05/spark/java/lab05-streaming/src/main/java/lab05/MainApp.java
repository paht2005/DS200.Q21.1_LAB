package lab05;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Application - Launcher for the Person Counting System.
 * 
 * Usage:
 *   java -jar lab05-streaming.jar [component]
 * 
 * Components:
 *   receiver   - Start Frame Receiver Server (port 6100)
 *   processing - Start Processing Server with Spark Streaming (port 6200)
 *   storage    - Start Storage Server (port 6300)
 *   source     - Start Video Source Simulator
 *   all        - Print instructions for running all components
 */
public class MainApp {
    private static final Logger logger = LoggerFactory.getLogger(MainApp.class);
    
    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            return;
        }
        
        String component = args[0].toLowerCase();
        String[] remainingArgs = new String[args.length - 1];
        System.arraycopy(args, 1, remainingArgs, 0, remainingArgs.length);
        
        switch (component) {
            case "receiver":
                FrameReceiverServer.main(remainingArgs);
                break;
                
            case "processing":
                ProcessingServer.main(remainingArgs);
                break;
                
            case "storage":
                StorageServer.main(remainingArgs);
                break;
                
            case "source":
                VideoSource.main(remainingArgs);
                break;
                
            case "all":
                printAllInstructions();
                break;
                
            default:
                System.err.println("Unknown component: " + component);
                printUsage();
        }
    }
    
    private static void printUsage() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║      Lab05 - Real-time Person Counting System (Java)         ║");
        System.out.println("║                  with Spark Streaming                        ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║                                                              ║");
        System.out.println("║  Usage: java -jar lab05-streaming.jar <component>            ║");
        System.out.println("║                                                              ║");
        System.out.println("║  Components:                                                 ║");
        System.out.println("║    storage    - Start Storage Server (port 6300)             ║");
        System.out.println("║    processing - Start Processing Server (port 6200)          ║");
        System.out.println("║    receiver   - Start Frame Receiver Server (port 6100)      ║");
        System.out.println("║    source     - Start Video Source Simulator                 ║");
        System.out.println("║    all        - Show instructions for all components         ║");
        System.out.println("║                                                              ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
    
    private static void printAllInstructions() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║           HOW TO RUN THE COMPLETE SYSTEM                     ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║                                                              ║");
        System.out.println("║  Open 4 separate terminals and run in order:                 ║");
        System.out.println("║                                                              ║");
        System.out.println("║  Terminal 1 - Storage Server:                                ║");
        System.out.println("║    java -jar lab05-streaming.jar storage                     ║");
        System.out.println("║                                                              ║");
        System.out.println("║  Terminal 2 - Processing Server:                             ║");
        System.out.println("║    java -jar lab05-streaming.jar processing                  ║");
        System.out.println("║    OR with spark-submit:                                     ║");
        System.out.println("║    spark-submit --class lab05.ProcessingServer \\             ║");
        System.out.println("║                 lab05-streaming.jar                          ║");
        System.out.println("║                                                              ║");
        System.out.println("║  Terminal 3 - Frame Receiver:                                ║");
        System.out.println("║    java -jar lab05-streaming.jar receiver                    ║");
        System.out.println("║                                                              ║");
        System.out.println("║  Terminal 4 - Video Source (Test):                           ║");
        System.out.println("║    java -jar lab05-streaming.jar source                      ║");
        System.out.println("║    java -jar lab05-streaming.jar source -d data/images -n 20 ║");
        System.out.println("║                                                              ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║                    ARCHITECTURE                              ║");
        System.out.println("║                                                              ║");
        System.out.println("║  [Camera/Video] ──TCP──► [Receiver:6100]                     ║");
        System.out.println("║                               │                              ║");
        System.out.println("║                               ▼                              ║");
        System.out.println("║                    [Processing:6200 + Spark]                 ║");
        System.out.println("║                               │                              ║");
        System.out.println("║                               ▼                              ║");
        System.out.println("║                       [Storage:6300]                         ║");
        System.out.println("║                               │                              ║");
        System.out.println("║                               ▼                              ║");
        System.out.println("║                 output/results/detections.json               ║");
        System.out.println("║                                                              ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}
