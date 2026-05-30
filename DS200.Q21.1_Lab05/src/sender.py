"""
Sender - Sends camera/video frames to the receiver server via TCP.

Usage:
    python sender.py [--video PATH] [--fps N] [--frames N]
"""

import socket
import json
import logging
import time
import uuid
import base64
import argparse
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
logger = logging.getLogger("Sender")


class FrameSender:
    """Sends video frames to receiver server via TCP."""
    
    def __init__(self, source=None, fps=2, target_host=Config.HOST, target_port=Config.RECEIVER_PORT):
        """
        Initialize frame sender.
        
        Args:
            source: Video file path, camera index (0 for webcam), or None for synthetic
            fps: Frames per second to send
            target_host: Receiver server host
            target_port: Receiver server port
        """
        self.source = source
        self.fps = fps
        self.target_host = target_host
        self.target_port = target_port
        self.cap = None
        self.connection = None
        self.running = False
        
        if source is not None and CV2_AVAILABLE:
            self._init_capture()
    
    def _init_capture(self):
        """Initialize video capture from file or camera."""
        try:
            self.cap = cv2.VideoCapture(self.source)
            if not self.cap.isOpened():
                logger.warning(f"Could not open video source: {self.source}")
                self.cap = None
            else:
                logger.info(f"Video capture initialized: {self.source}")
        except Exception as e:
            logger.error(f"Error initializing capture: {e}")
            self.cap = None
    
    def connect(self):
        """Connect to the receiver server."""
        try:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.connect((self.target_host, self.target_port))
            logger.info(f"Connected to Receiver at {self.target_host}:{self.target_port}")
            return True
        except ConnectionRefusedError:
            logger.error(f"Receiver not available at {self.target_host}:{self.target_port}")
            logger.info("Make sure receiver.py is running first!")
            return False
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def _get_frame(self):
        """Get next frame from video source or generate synthetic."""
        if self.cap is not None and self.cap.isOpened():
            ret, frame = self.cap.read()
            if ret:
                return frame
            else:
                # Reset to beginning if video ended
                self.cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                ret, frame = self.cap.read()
                return frame if ret else None
        
        # Generate synthetic frame with random shapes
        return self._generate_synthetic_frame()
    
    def _generate_synthetic_frame(self):
        """Generate a synthetic frame with random shapes."""
        frame = np.zeros((480, 640, 3), dtype=np.uint8)
        frame[:] = (50, 50, 50)  # Dark gray background
        
        # Add timestamp
        timestamp = datetime.now().strftime("%H:%M:%S")
        cv2.putText(frame, f"Synthetic Frame - {timestamp}", (50, 50),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
        
        # Add random rectangles to simulate people
        num_rects = np.random.randint(1, 5)
        for _ in range(num_rects):
            x = np.random.randint(50, 500)
            y = np.random.randint(100, 350)
            w = np.random.randint(40, 80)
            h = np.random.randint(100, 180)
            color = (0, 255, 0)
            cv2.rectangle(frame, (x, y), (x + w, y + h), color, 2)
        
        return frame
    
    def _encode_frame(self, frame):
        """Encode frame to base64 for transmission."""
        if frame is None:
            return None
        
        # Compress frame to JPEG
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        return base64.b64encode(buffer).decode('utf-8')
    
    def send_frames(self, num_frames=None):
        """
        Send frames to the receiver server.
        
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
        if num_frames:
            logger.info(f"Will send {num_frames} frames")
        else:
            logger.info("Press Ctrl+C to stop")
        
        try:
            while self.running:
                if num_frames and frame_count >= num_frames:
                    break
                
                start_time = time.time()
                
                # Get and encode frame
                frame = self._get_frame()
                if frame is None:
                    logger.warning("Could not get frame, using synthetic")
                    frame = self._generate_synthetic_frame()
                
                encoded = self._encode_frame(frame)
                
                # Create payload
                payload = {
                    "type": "frame",
                    "frame_id": str(uuid.uuid4()),
                    "timestamp": datetime.now().isoformat(),
                    "frame_number": frame_count,
                    "data": encoded
                }
                
                # Send frame
                message = json.dumps(payload) + "\n"
                try:
                    self.connection.send(message.encode('utf-8'))
                    frame_count += 1
                    logger.info(f"Sent frame {frame_count}")
                except BrokenPipeError:
                    logger.error("Connection closed by receiver")
                    break
                except Exception as e:
                    logger.error(f"Error sending frame: {e}")
                    break
                
                # Maintain FPS
                elapsed = time.time() - start_time
                sleep_time = frame_interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            logger.info("Stopping frame stream...")
        finally:
            self.stop()
            logger.info(f"Sent {frame_count} frames total")
    
    def stop(self):
        """Stop sending and close connections."""
        self.running = False
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
        if self.cap:
            self.cap.release()
        logger.info("Sender stopped")


def main():
    parser = argparse.ArgumentParser(description="Send video frames to receiver")
    parser.add_argument("--video", "-v", help="Video file path (default: synthetic frames)")
    parser.add_argument("--camera", "-c", type=int, help="Camera index (e.g., 0 for webcam)")
    parser.add_argument("--fps", "-f", type=int, default=2, help="Frames per second (default: 2)")
    parser.add_argument("--frames", "-n", type=int, help="Number of frames to send (default: infinite)")
    parser.add_argument("--host", default=Config.HOST, help=f"Receiver host (default: {Config.HOST})")
    parser.add_argument("--port", "-p", type=int, default=Config.RECEIVER_PORT, 
                        help=f"Receiver port (default: {Config.RECEIVER_PORT})")
    
    args = parser.parse_args()
    
    # Determine source
    source = None
    if args.video:
        source = args.video
    elif args.camera is not None:
        source = args.camera
    
    sender = FrameSender(
        source=source,
        fps=args.fps,
        target_host=args.host,
        target_port=args.port
    )
    
    sender.send_frames(num_frames=args.frames)


if __name__ == "__main__":
    main()
