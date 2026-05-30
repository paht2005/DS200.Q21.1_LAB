"""
Video Source - Simulates camera frames for testing.

This module simulates a video source by sending frames to the receiver server.
Can use a video file, webcam, or generate synthetic frames.
"""

import socket
import json
import logging
import time
import uuid
import base64
from datetime import datetime

try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    print("Warning: OpenCV not available. Using synthetic frames.")

from config import Config

# Configure logging
logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
logger = logging.getLogger("VideoSource")


class VideoSource:
    """Simulates video frames from camera or video file."""
    
    def __init__(self, source=None, fps=1):
        """
        Initialize video source.
        
        Args:
            source: Video file path, camera index (0), or None for synthetic
            fps: Frames per second to send
        """
        self.source = source
        self.fps = fps
        self.cap = None
        self.connection = None
        self.running = False
        
        if source is not None and CV2_AVAILABLE:
            self._init_capture()
    
    def _init_capture(self):
        """Initialize video capture."""
        try:
            self.cap = cv2.VideoCapture(self.source)
            if not self.cap.isOpened():
                logger.warning(f"Could not open video source: {self.source}")
                self.cap = None
        except Exception as e:
            logger.error(f"Error initializing capture: {e}")
            self.cap = None
    
    def connect(self, host=Config.HOST, port=Config.RECEIVER_PORT):
        """Connect to the receiver server."""
        try:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.connect((host, port))
            logger.info(f"Connected to Receiver Server at {host}:{port}")
            return True
        except ConnectionRefusedError:
            logger.error(f"Receiver server not available at {host}:{port}")
            return False
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def start_streaming(self, num_frames=None):
        """
        Start streaming frames to the receiver server.
        
        Args:
            num_frames: Number of frames to send (None for infinite)
        """
        if not self.connection:
            if not self.connect():
                return
        
        self.running = True
        frame_count = 0
        frame_interval = 1.0 / self.fps
        
        logger.info(f"Starting frame stream at {self.fps} FPS...")
        
        try:
            while self.running:
                if num_frames and frame_count >= num_frames:
                    break
                
                start_time = time.time()
                
                # Get frame
                frame_data = self._get_frame()
                
                if frame_data:
                    # Create payload
                    payload = {
                        "frame_id": str(uuid.uuid4()),
                        "timestamp": datetime.now().isoformat(),
                        "frame_number": frame_count,
                        "data": frame_data
                    }
                    
                    # Send frame
                    self._send_frame(payload)
                    frame_count += 1
                    
                    logger.info(f"Sent frame {frame_count}")
                
                # Maintain FPS
                elapsed = time.time() - start_time
                if elapsed < frame_interval:
                    time.sleep(frame_interval - elapsed)
                    
        except KeyboardInterrupt:
            logger.info("Streaming interrupted")
        finally:
            self.stop()
        
        logger.info(f"Streaming complete. Sent {frame_count} frames.")
    
    def _get_frame(self):
        """Get next frame as base64 encoded string."""
        if self.cap and self.cap.isOpened():
            ret, frame = self.cap.read()
            if ret:
                # Encode frame as JPEG and then base64
                _, buffer = cv2.imencode('.jpg', frame)
                return base64.b64encode(buffer).decode('utf-8')
            else:
                # Video ended, loop back
                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                return self._get_frame()
        else:
            # Generate synthetic frame
            return self._generate_synthetic_frame()
    
    def _generate_synthetic_frame(self):
        """Generate a synthetic frame for testing."""
        if CV2_AVAILABLE:
            # Create a random image with some shapes (simulating people)
            import random
            
            img = np.zeros((480, 640, 3), dtype=np.uint8)
            img[:] = (50, 50, 50)  # Dark gray background
            
            # Draw some rectangles to simulate people
            num_people = random.randint(1, 5)
            for _ in range(num_people):
                x = random.randint(50, 550)
                y = random.randint(50, 350)
                w = random.randint(40, 80)
                h = random.randint(100, 150)
                color = (random.randint(100, 255), random.randint(100, 255), random.randint(100, 255))
                cv2.rectangle(img, (x, y), (x+w, y+h), color, -1)
            
            # Encode as JPEG and base64
            _, buffer = cv2.imencode('.jpg', img)
            return base64.b64encode(buffer).decode('utf-8')
        else:
            # Return minimal placeholder
            return base64.b64encode(b"synthetic_frame_data").decode('utf-8')
    
    def _send_frame(self, payload):
        """Send frame payload to receiver server."""
        try:
            message = json.dumps(payload) + "\n"
            self.connection.send(message.encode('utf-8'))
        except BrokenPipeError:
            logger.error("Connection to receiver lost")
            self.running = False
        except Exception as e:
            logger.error(f"Error sending frame: {e}")
    
    def stop(self):
        """Stop streaming and cleanup."""
        self.running = False
        if self.cap:
            self.cap.release()
        if self.connection:
            self.connection.close()
        logger.info("Video source stopped")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Video frame source for person counting system')
    parser.add_argument('--source', '-s', default=None, 
                        help='Video file path or camera index (default: synthetic)')
    parser.add_argument('--fps', '-f', type=float, default=1.0,
                        help='Frames per second (default: 1)')
    parser.add_argument('--frames', '-n', type=int, default=10,
                        help='Number of frames to send (default: 10)')
    parser.add_argument('--host', default=Config.HOST,
                        help=f'Receiver host (default: {Config.HOST})')
    parser.add_argument('--port', '-p', type=int, default=Config.RECEIVER_PORT,
                        help=f'Receiver port (default: {Config.RECEIVER_PORT})')
    
    args = parser.parse_args()
    
    source = VideoSource(source=args.source, fps=args.fps)
    
    if source.connect(args.host, args.port):
        source.start_streaming(num_frames=args.frames)


if __name__ == "__main__":
    main()
