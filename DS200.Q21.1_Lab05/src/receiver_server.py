"""
Receiver Server - Receives camera frames and forwards to processing server.

This server acts as the entry point for camera/video frames.
It receives frames via TCP and forwards them to the processing server.
"""

import socket
import json
import logging
import threading
from datetime import datetime
from config import Config, MessageType

# Configure logging
logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("ReceiverServer")


class ReceiverServer:
    """TCP server that receives frames and forwards to processing."""
    
    def __init__(self, host=Config.HOST, port=Config.RECEIVER_PORT):
        self.host = host
        self.port = port
        self.server_socket = None
        self.processing_connection = None
        self.running = False
        
    def start(self):
        """Start the receiver server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        logger.info(f"Receiver Server started on {self.host}:{self.port}")
        logger.info("Waiting for frame sources to connect...")
        
        # Connect to processing server
        self._connect_to_processing_server()
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                logger.info(f"Frame source connected from {address}")
                
                # Handle client in a new thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
            except KeyboardInterrupt:
                logger.info("Shutting down receiver server...")
                self.stop()
            except Exception as e:
                logger.error(f"Error accepting connection: {e}")
    
    def _connect_to_processing_server(self):
        """Establish connection to the processing server."""
        try:
            self.processing_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.processing_connection.connect((Config.HOST, Config.PROCESSING_PORT))
            logger.info(f"Connected to Processing Server at {Config.HOST}:{Config.PROCESSING_PORT}")
        except ConnectionRefusedError:
            logger.warning("Processing server not available. Frames will be queued.")
            self.processing_connection = None
        except Exception as e:
            logger.error(f"Error connecting to processing server: {e}")
            self.processing_connection = None
    
    def _handle_client(self, client_socket, address):
        """Handle incoming frames from a client."""
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
            logger.error(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()
            logger.info(f"Client {address} disconnected")
    
    def _process_frame(self, message):
        """Process received frame and forward to processing server."""
        try:
            frame_data = json.loads(message)
            
            # Add receiver timestamp
            frame_data['receiver_timestamp'] = datetime.now().isoformat()
            
            logger.info(f"Received frame: {frame_data.get('frame_id', 'unknown')}")
            
            # Forward to processing server
            self._forward_to_processing(frame_data)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON received: {e}")
        except Exception as e:
            logger.error(f"Error processing frame: {e}")
    
    def _forward_to_processing(self, frame_data):
        """Forward frame to the processing server."""
        if self.processing_connection is None:
            self._connect_to_processing_server()
            
        if self.processing_connection:
            try:
                payload = {
                    "type": MessageType.FRAME,
                    "data": frame_data
                }
                message = json.dumps(payload) + "\n"
                self.processing_connection.send(message.encode('utf-8'))
                logger.info(f"Forwarded frame {frame_data.get('frame_id')} to processing")
            except BrokenPipeError:
                logger.error("Connection to processing server lost. Reconnecting...")
                self.processing_connection = None
                self._connect_to_processing_server()
            except Exception as e:
                logger.error(f"Error forwarding frame: {e}")
        else:
            logger.warning("No connection to processing server. Frame dropped.")
    
    def stop(self):
        """Stop the receiver server."""
        self.running = False
        if self.processing_connection:
            self.processing_connection.close()
        if self.server_socket:
            self.server_socket.close()
        logger.info("Receiver Server stopped")


def main():
    """Main entry point."""
    server = ReceiverServer()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
