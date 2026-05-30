"""
Receiver - Receives camera frames via TCP and forwards to detector.

Usage:
    python receiver.py [--port PORT]
"""

import socket
import json
import logging
import threading
import argparse
from datetime import datetime
from config import Config, MessageType

# Configure logging
logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("Receiver")


class FrameReceiver:
    """TCP server that receives frames from sender and forwards to detector."""
    
    def __init__(self, host=Config.HOST, port=Config.RECEIVER_PORT):
        self.host = host
        self.port = port
        self.server_socket = None
        self.detector_connection = None
        self.running = False
        self.frame_count = 0
        
    def start(self):
        """Start the receiver server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        logger.info("=" * 60)
        logger.info(f"  RECEIVER SERVER started on {self.host}:{self.port}")
        logger.info("=" * 60)
        logger.info("Waiting for sender to connect...")
        logger.info("Run sender.py to start sending frames")
        
        # Try to connect to detector
        self._connect_to_detector()
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                logger.info(f"Sender connected from {address}")
                
                # Handle client in a new thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
            except KeyboardInterrupt:
                logger.info("Shutting down receiver...")
                self.stop()
            except Exception as e:
                logger.error(f"Error accepting connection: {e}")
    
    def _connect_to_detector(self):
        """Establish connection to the detector server."""
        try:
            self.detector_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.detector_connection.connect((Config.HOST, Config.PROCESSING_PORT))
            logger.info(f"Connected to Detector at {Config.HOST}:{Config.PROCESSING_PORT}")
        except ConnectionRefusedError:
            logger.warning("Detector not available. Run detect_object.py first.")
            logger.warning("Frames will be logged but not processed.")
            self.detector_connection = None
        except Exception as e:
            logger.error(f"Error connecting to detector: {e}")
            self.detector_connection = None
    
    def _handle_client(self, client_socket, address):
        """Handle incoming frames from a sender."""
        buffer = ""
        
        try:
            while self.running:
                data = client_socket.recv(Config.BUFFER_SIZE)
                if not data:
                    break
                    
                buffer += data.decode('utf-8')
                
                # Process complete JSON messages (newline-delimited)
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    if message.strip():
                        self._process_frame(message)
                        
        except Exception as e:
            logger.error(f"Error handling sender {address}: {e}")
        finally:
            client_socket.close()
            logger.info(f"Sender {address} disconnected")
    
    def _process_frame(self, message):
        """Process received frame and forward to detector."""
        try:
            frame_data = json.loads(message)
            self.frame_count += 1
            
            frame_id = frame_data.get("frame_id", "unknown")
            frame_num = frame_data.get("frame_number", self.frame_count)
            
            logger.info(f"Received frame #{frame_num} (ID: {frame_id[:8]}...)")
            
            # Forward to detector if connected
            if self.detector_connection:
                try:
                    forward_message = json.dumps(frame_data) + "\n"
                    self.detector_connection.send(forward_message.encode('utf-8'))
                    logger.debug(f"Forwarded frame #{frame_num} to detector")
                except BrokenPipeError:
                    logger.warning("Lost connection to detector. Reconnecting...")
                    self._connect_to_detector()
                except Exception as e:
                    logger.error(f"Error forwarding to detector: {e}")
            else:
                # Try to reconnect periodically
                if self.frame_count % 10 == 0:
                    self._connect_to_detector()
                    
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing frame: {e}")
    
    def stop(self):
        """Stop the receiver server."""
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        if self.detector_connection:
            try:
                self.detector_connection.close()
            except:
                pass
        logger.info(f"Receiver stopped. Total frames received: {self.frame_count}")


def main():
    parser = argparse.ArgumentParser(description="Receive frames from sender")
    parser.add_argument("--host", default=Config.HOST, help=f"Host to bind (default: {Config.HOST})")
    parser.add_argument("--port", "-p", type=int, default=Config.RECEIVER_PORT,
                        help=f"Port to listen on (default: {Config.RECEIVER_PORT})")
    
    args = parser.parse_args()
    
    receiver = FrameReceiver(host=args.host, port=args.port)
    
    try:
        receiver.start()
    except KeyboardInterrupt:
        receiver.stop()


if __name__ == "__main__":
    main()
