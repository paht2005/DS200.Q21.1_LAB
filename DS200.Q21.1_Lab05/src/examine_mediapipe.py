"""
Examine MediaPipe - Explore MediaPipe capabilities for pose, face, and hand detection.

Usage:
    python examine_mediapipe.py [--mode pose|face|hands|all] [--input IMAGE_PATH]
"""

import argparse
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MediaPipeExplorer")

# Import dependencies
try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    logger.error("OpenCV not available. Install with: pip install opencv-python")

try:
    import mediapipe as mp
    MP_AVAILABLE = True
except ImportError:
    MP_AVAILABLE = False
    logger.error("MediaPipe not available. Install with: pip install mediapipe")


class MediaPipeExplorer:
    """Explore different MediaPipe detection capabilities."""
    
    def __init__(self):
        self.mp_drawing = None
        self.mp_pose = None
        self.mp_face_detection = None
        self.mp_hands = None
        self.mp_face_mesh = None
        
        if MP_AVAILABLE:
            self._init_mediapipe()
    
    def _init_mediapipe(self):
        """Initialize MediaPipe solutions."""
        self.mp_drawing = mp.solutions.drawing_utils
        self.mp_drawing_styles = mp.solutions.drawing_styles
        
        # Pose
        self.mp_pose = mp.solutions.pose
        
        # Face detection
        self.mp_face_detection = mp.solutions.face_detection
        
        # Hands
        self.mp_hands = mp.solutions.hands
        
        # Face mesh
        self.mp_face_mesh = mp.solutions.face_mesh
        
        logger.info("MediaPipe solutions initialized")
    
    def detect_pose(self, image, draw=True):
        """
        Detect human pose landmarks.
        
        Args:
            image: Input image (numpy array)
            draw: Whether to draw landmarks on image
            
        Returns:
            Annotated image and pose landmarks
        """
        if not MP_AVAILABLE:
            return image, None
        
        with self.mp_pose.Pose(
            static_image_mode=True,
            model_complexity=2,
            enable_segmentation=True,
            min_detection_confidence=0.5
        ) as pose:
            
            # Convert to RGB
            image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            results = pose.process(image_rgb)
            
            output = image.copy()
            
            if results.pose_landmarks and draw:
                self.mp_drawing.draw_landmarks(
                    output,
                    results.pose_landmarks,
                    self.mp_pose.POSE_CONNECTIONS,
                    landmark_drawing_spec=self.mp_drawing_styles.get_default_pose_landmarks_style()
                )
                
                # Add segmentation mask overlay
                if results.segmentation_mask is not None:
                    mask = results.segmentation_mask
                    condition = np.stack([mask] * 3, axis=-1) > 0.1
                    bg_color = np.zeros(image.shape, dtype=np.uint8)
                    bg_color[:] = (192, 192, 192)
                    output = np.where(condition, output, bg_color)
            
            landmark_data = None
            if results.pose_landmarks:
                landmark_data = {
                    "num_landmarks": len(results.pose_landmarks.landmark),
                    "landmarks": [
                        {
                            "x": lm.x,
                            "y": lm.y,
                            "z": lm.z,
                            "visibility": lm.visibility
                        }
                        for lm in results.pose_landmarks.landmark
                    ]
                }
            
            return output, landmark_data
    
    def detect_faces(self, image, draw=True):
        """
        Detect faces in image.
        
        Args:
            image: Input image (numpy array)
            draw: Whether to draw detections on image
            
        Returns:
            Annotated image and face detections
        """
        if not MP_AVAILABLE:
            return image, None
        
        with self.mp_face_detection.FaceDetection(
            model_selection=1,
            min_detection_confidence=0.5
        ) as face_detection:
            
            image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            results = face_detection.process(image_rgb)
            
            output = image.copy()
            face_data = []
            
            if results.detections:
                for detection in results.detections:
                    if draw:
                        self.mp_drawing.draw_detection(output, detection)
                    
                    bbox = detection.location_data.relative_bounding_box
                    h, w = image.shape[:2]
                    
                    face_data.append({
                        "confidence": detection.score[0],
                        "bounding_box": {
                            "x": int(bbox.xmin * w),
                            "y": int(bbox.ymin * h),
                            "width": int(bbox.width * w),
                            "height": int(bbox.height * h)
                        }
                    })
            
            return output, {"num_faces": len(face_data), "faces": face_data}
    
    def detect_hands(self, image, draw=True):
        """
        Detect hands and landmarks.
        
        Args:
            image: Input image (numpy array)
            draw: Whether to draw landmarks on image
            
        Returns:
            Annotated image and hand landmarks
        """
        if not MP_AVAILABLE:
            return image, None
        
        with self.mp_hands.Hands(
            static_image_mode=True,
            max_num_hands=2,
            min_detection_confidence=0.5
        ) as hands:
            
            image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            results = hands.process(image_rgb)
            
            output = image.copy()
            hand_data = []
            
            if results.multi_hand_landmarks:
                for hand_landmarks, handedness in zip(
                    results.multi_hand_landmarks,
                    results.multi_handedness
                ):
                    if draw:
                        self.mp_drawing.draw_landmarks(
                            output,
                            hand_landmarks,
                            self.mp_hands.HAND_CONNECTIONS,
                            self.mp_drawing_styles.get_default_hand_landmarks_style(),
                            self.mp_drawing_styles.get_default_hand_connections_style()
                        )
                    
                    hand_data.append({
                        "handedness": handedness.classification[0].label,
                        "confidence": handedness.classification[0].score,
                        "num_landmarks": len(hand_landmarks.landmark)
                    })
            
            return output, {"num_hands": len(hand_data), "hands": hand_data}
    
    def detect_all(self, image, draw=True):
        """
        Run all detections on image.
        
        Args:
            image: Input image (numpy array)
            draw: Whether to draw results on image
            
        Returns:
            Annotated image and all detection results
        """
        output = image.copy()
        results = {}
        
        # Pose
        pose_img, pose_data = self.detect_pose(image, draw=False)
        results["pose"] = pose_data
        
        # Faces
        face_img, face_data = self.detect_faces(image, draw=False)
        results["faces"] = face_data
        
        # Hands
        hand_img, hand_data = self.detect_hands(image, draw=False)
        results["hands"] = hand_data
        
        if draw:
            # Combine visualizations
            # Use pose image as base (has segmentation)
            if pose_data:
                output = pose_img
            
            # Draw face boxes
            if face_data and face_data.get("faces"):
                for face in face_data["faces"]:
                    bb = face["bounding_box"]
                    cv2.rectangle(
                        output,
                        (bb["x"], bb["y"]),
                        (bb["x"] + bb["width"], bb["y"] + bb["height"]),
                        (0, 255, 255),
                        2
                    )
        
        return output, results
    
    def process_file(self, input_path, output_dir, mode="all"):
        """
        Process an image file with specified detection mode.
        
        Args:
            input_path: Path to input image
            output_dir: Directory for output images
            mode: Detection mode (pose, face, hands, all)
        """
        if not os.path.exists(input_path):
            logger.error(f"Input file not found: {input_path}")
            return
        
        # Read image
        image = cv2.imread(input_path)
        if image is None:
            logger.error(f"Could not read image: {input_path}")
            return
        
        logger.info(f"Processing: {input_path}")
        logger.info(f"Image size: {image.shape[1]}x{image.shape[0]}")
        logger.info(f"Mode: {mode}")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        
        if mode == "pose" or mode == "all":
            output, data = self.detect_pose(image)
            out_path = os.path.join(output_dir, f"{base_name}_pose.png")
            cv2.imwrite(out_path, output)
            logger.info(f"Pose detection saved to: {out_path}")
            if data:
                logger.info(f"  → Detected {data['num_landmarks']} landmarks")
        
        if mode == "face" or mode == "all":
            output, data = self.detect_faces(image)
            out_path = os.path.join(output_dir, f"{base_name}_face.png")
            cv2.imwrite(out_path, output)
            logger.info(f"Face detection saved to: {out_path}")
            if data:
                logger.info(f"  → Detected {data['num_faces']} face(s)")
        
        if mode == "hands" or mode == "all":
            output, data = self.detect_hands(image)
            out_path = os.path.join(output_dir, f"{base_name}_hands.png")
            cv2.imwrite(out_path, output)
            logger.info(f"Hand detection saved to: {out_path}")
            if data:
                logger.info(f"  → Detected {data['num_hands']} hand(s)")
        
        if mode == "all":
            output, data = self.detect_all(image)
            out_path = os.path.join(output_dir, f"{base_name}_all.png")
            cv2.imwrite(out_path, output)
            logger.info(f"Combined detection saved to: {out_path}")


def demo():
    """Run demo with webcam or sample images."""
    print("=" * 60)
    print("  MEDIAPIPE EXPLORER DEMO")
    print("=" * 60)
    
    if not CV2_AVAILABLE or not MP_AVAILABLE:
        print("Required dependencies not available.")
        print("Install with: pip install opencv-python mediapipe")
        return
    
    explorer = MediaPipeExplorer()
    
    # Check for sample images
    sample_dirs = ["data/images", "../data/images"]
    sample_images = []
    
    for d in sample_dirs:
        if os.path.exists(d):
            for f in os.listdir(d):
                if f.lower().endswith(('.png', '.jpg', '.jpeg')):
                    sample_images.append(os.path.join(d, f))
    
    if sample_images:
        output_dir = "output/mediapipe_demo"
        os.makedirs(output_dir, exist_ok=True)
        
        for img_path in sample_images[:2]:  # Process up to 2 images
            print(f"\nProcessing: {img_path}")
            explorer.process_file(img_path, output_dir, mode="all")
        
        print(f"\nResults saved to: {output_dir}/")
    else:
        print("No sample images found.")
        print("Provide an image with: --input IMAGE_PATH")
    
    print("\nDemo complete!")


def main():
    parser = argparse.ArgumentParser(description="Explore MediaPipe detection capabilities")
    parser.add_argument("--input", "-i", help="Input image path")
    parser.add_argument("--output", "-o", default="output/mediapipe",
                        help="Output directory (default: output/mediapipe)")
    parser.add_argument("--mode", "-m", default="all",
                        choices=["pose", "face", "hands", "all"],
                        help="Detection mode (default: all)")
    parser.add_argument("--demo", "-d", action="store_true",
                        help="Run demo with sample images")
    
    args = parser.parse_args()
    
    if args.demo:
        demo()
        return
    
    if not args.input:
        parser.print_help()
        print("\nError: --input is required (or use --demo)")
        return
    
    if not CV2_AVAILABLE or not MP_AVAILABLE:
        print("Required dependencies not available.")
        print("Install with: pip install opencv-python mediapipe")
        return
    
    explorer = MediaPipeExplorer()
    explorer.process_file(args.input, args.output, mode=args.mode)


if __name__ == "__main__":
    main()
