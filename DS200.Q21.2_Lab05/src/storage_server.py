"""
Storage Server - Persists detection results.

This server receives detection results from the processing server
and stores them persistently in JSON format.
"""

import socket
import json
import logging
import threading
import os
from datetime import datetime
from config import Config, MessageType

# Configure logging
logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("StorageServer")


class StorageServer:
    """TCP server for storing detection results."""
    
    def __init__(self, host=Config.HOST, port=Config.STORAGE_PORT):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.results = []
        self.lock = threading.Lock()
        
        # Ensure storage directory exists
        self._init_storage()
    
    def _init_storage(self):
        """Initialize storage directory and file."""
        storage_dir = os.path.dirname(Config.STORAGE_FILE)
        if storage_dir and not os.path.exists(storage_dir):
            os.makedirs(storage_dir)
            logger.info(f"Created storage directory: {storage_dir}")
        
        # Load existing results if file exists
        if os.path.exists(Config.STORAGE_FILE):
            try:
                with open(Config.STORAGE_FILE, 'r') as f:
                    self.results = json.load(f)
                logger.info(f"Loaded {len(self.results)} existing results")
            except Exception as e:
                logger.warning(f"Could not load existing results: {e}")
                self.results = []
    
    def start(self):
        """Start the storage server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        logger.info(f"Storage Server started on {self.host}:{self.port}")
        logger.info(f"Results will be stored in: {Config.STORAGE_FILE}")
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                logger.info(f"Processing server connected from {address}")
                
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
            except KeyboardInterrupt:
                logger.info("Shutting down storage server...")
                self.stop()
            except Exception as e:
                logger.error(f"Error accepting connection: {e}")
    
    def _handle_client(self, client_socket, address):
        """Handle incoming results from processing server."""
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
        """Process incoming message and store result."""
        try:
            msg = json.loads(message)
            
            if msg.get('type') == MessageType.DETECTION_RESULT:
                result_data = msg.get('data', {})
                self._store_result(result_data)
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _store_result(self, result):
        """Store a detection result."""
        with self.lock:
            # Add storage timestamp
            result['stored_at'] = datetime.now().isoformat()
            
            # Append to results list
            self.results.append(result)
            
            # Persist to file
            self._save_to_file()
            
            logger.info(
                f"Stored result for frame {result.get('frame_id')}: "
                f"{result.get('person_count')} persons detected"
            )
            
            # Print summary
            self._print_summary(result)
    
    def _save_to_file(self):
        """Save results to JSON file."""
        try:
            with open(Config.STORAGE_FILE, 'w') as f:
                json.dump(self.results, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving to file: {e}")
    
    def _print_summary(self, result):
        """Print a formatted summary of the detection result."""
        print("\n" + "="*60)
        print(f"DETECTION RESULT - Frame: {result.get('frame_id', 'unknown')[:8]}...")
        print("="*60)
        print(f"Timestamp: {result.get('timestamp')}")
        print(f"Persons Detected: {result.get('person_count')}")
        print(f"Processing Time: {result.get('processing_time_ms')}ms")
        
        if result.get('bounding_boxes'):
            print("\nBounding Boxes:")
            for i, box in enumerate(result['bounding_boxes'], 1):
                print(f"  {i}. x={box['x']}, y={box['y']}, "
                      f"w={box['width']}, h={box['height']}, "
                      f"conf={box['confidence']}")
        
        print(f"\nTotal stored results: {len(self.results)}")
        print("="*60 + "\n")
    
    def get_statistics(self):
        """Get statistics about stored results."""
        if not self.results:
            return {"total_frames": 0, "total_persons": 0}
        
        total_persons = sum(r.get('person_count', 0) for r in self.results)
        avg_persons = total_persons / len(self.results) if self.results else 0
        avg_processing_time = sum(r.get('processing_time_ms', 0) for r in self.results) / len(self.results)
        
        return {
            "total_frames": len(self.results),
            "total_persons": total_persons,
            "avg_persons_per_frame": round(avg_persons, 2),
            "avg_processing_time_ms": round(avg_processing_time, 2)
        }
    
    def stop(self):
        """Stop the storage server."""
        self.running = False
        
        # Final save
        with self.lock:
            self._save_to_file()
        
        # Print final statistics
        stats = self.get_statistics()
        logger.info(f"Final statistics: {stats}")
        
        if self.server_socket:
            self.server_socket.close()
        logger.info("Storage Server stopped")


def main():
    """Main entry point."""
    server = StorageServer()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
