"""
Background Remover - Remove background from images using MediaPipe/OpenCV.

Usage:
    python background_remover.py --input IMAGE_PATH [--output OUTPUT_PATH]
"""

import argparse
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("BackgroundRemover")

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
    logger.warning("MediaPipe not available. Install with: pip install mediapipe")


class BackgroundRemover:
    """Remove background from images using MediaPipe segmentation."""
    
    def __init__(self, model_selection=1):
        """
        Initialize background remover.
        
        Args:
            model_selection: 0 for general, 1 for landscape
        """
        self.model_selection = model_selection
        self.segmentor = None
        
        if MP_AVAILABLE:
            self._init_mediapipe()
        else:
            logger.warning("Using fallback OpenCV method")
    
    def _init_mediapipe(self):
        """Initialize MediaPipe selfie segmentation."""
        try:
            mp_selfie = mp.solutions.selfie_segmentation
            self.segmentor = mp_selfie.SelfieSegmentation(
                model_selection=self.model_selection
            )
            logger.info("MediaPipe Selfie Segmentation initialized")
        except Exception as e:
            logger.error(f"Error initializing MediaPipe: {e}")
            self.segmentor = None
    
    def remove_background(self, image, bg_color=(0, 255, 0), blur_bg=False):
        """
        Remove background from image.
        
        Args:
            image: Input image (numpy array)
            bg_color: Background color tuple (B, G, R) or None for transparent
            blur_bg: Whether to blur background instead of replacing
            
        Returns:
            Image with background removed/replaced
        """
        if image is None:
            logger.error("No image provided")
            return None
        
        if self.segmentor:
            return self._mediapipe_remove(image, bg_color, blur_bg)
        elif CV2_AVAILABLE:
            return self._opencv_fallback(image, bg_color)
        else:
            logger.error("No segmentation method available")
            return image
    
    def _mediapipe_remove(self, image, bg_color, blur_bg):
        """Remove background using MediaPipe."""
        # Convert BGR to RGB
        image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        
        # Process image
        results = self.segmentor.process(image_rgb)
        
        # Get mask
        mask = results.segmentation_mask
        
        # Create condition for foreground
        condition = mask > 0.5
        condition = np.stack([condition] * 3, axis=-1)
        
        if blur_bg:
            # Blur background
            blurred = cv2.GaussianBlur(image, (55, 55), 0)
            output = np.where(condition, image, blurred)
        else:
            # Replace background with color
            if bg_color is None:
                # Create transparent background (BGRA)
                output = cv2.cvtColor(image, cv2.COLOR_BGR2BGRA)
                output[:, :, 3] = (mask * 255).astype(np.uint8)
            else:
                background = np.full(image.shape, bg_color, dtype=np.uint8)
                output = np.where(condition, image, background)
        
        return output
    
    def _opencv_fallback(self, image, bg_color):
        """Fallback background removal using OpenCV (GrabCut)."""
        logger.info("Using OpenCV GrabCut (slower, less accurate)")
        
        h, w = image.shape[:2]
        
        # Initialize mask
        mask = np.zeros((h, w), np.uint8)
        
        # Background and foreground models
        bgd_model = np.zeros((1, 65), np.float64)
        fgd_model = np.zeros((1, 65), np.float64)
        
        # Rectangle for GrabCut (assume person is in center)
        rect = (int(w * 0.1), int(h * 0.05), int(w * 0.8), int(h * 0.9))
        
        # Apply GrabCut
        cv2.grabCut(image, mask, rect, bgd_model, fgd_model, 5, cv2.GC_INIT_WITH_RECT)
        
        # Create binary mask
        mask2 = np.where((mask == 2) | (mask == 0), 0, 1).astype('uint8')
        
        # Apply mask
        foreground = image * mask2[:, :, np.newaxis]
        
        # Create background
        if bg_color is not None:
            background = np.full(image.shape, bg_color, dtype=np.uint8)
            inv_mask = 1 - mask2
            background = background * inv_mask[:, :, np.newaxis]
            output = foreground + background
        else:
            output = foreground
        
        return output
    
    def process_file(self, input_path, output_path=None, bg_color=(0, 255, 0)):
        """
        Process a single image file.
        
        Args:
            input_path: Path to input image
            output_path: Path for output (default: input_nobg.png)
            bg_color: Background color or None for transparent
        """
        if not os.path.exists(input_path):
            logger.error(f"Input file not found: {input_path}")
            return None
        
        # Read image
        image = cv2.imread(input_path)
        if image is None:
            logger.error(f"Could not read image: {input_path}")
            return None
        
        logger.info(f"Processing: {input_path}")
        logger.info(f"Image size: {image.shape[1]}x{image.shape[0]}")
        
        # Remove background
        result = self.remove_background(image, bg_color)
        
        if result is None:
            return None
        
        # Determine output path
        if output_path is None:
            name, ext = os.path.splitext(input_path)
            output_path = f"{name}_nobg.png"
        
        # Save result
        cv2.imwrite(output_path, result)
        logger.info(f"Saved to: {output_path}")
        
        return output_path
    
    def close(self):
        """Clean up resources."""
        if self.segmentor:
            self.segmentor.close()


def demo():
    """Run demo with sample images."""
    import sys
    
    print("=" * 60)
    print("  BACKGROUND REMOVER DEMO")
    print("=" * 60)
    
    # Check for sample images
    sample_dirs = ["data/images", "../data/images", "../../data/images"]
    sample_images = []
    
    for d in sample_dirs:
        if os.path.exists(d):
            for f in os.listdir(d):
                if f.lower().endswith(('.png', '.jpg', '.jpeg')):
                    sample_images.append(os.path.join(d, f))
    
    if not sample_images:
        print("No sample images found in data/images/")
        print("Please provide an image with --input flag")
        return
    
    remover = BackgroundRemover()
    
    for img_path in sample_images[:3]:  # Process up to 3 images
        print(f"\nProcessing: {img_path}")
        
        # Green background
        remover.process_file(img_path, bg_color=(0, 255, 0))
    
    remover.close()
    print("\nDemo complete!")


def main():
    parser = argparse.ArgumentParser(description="Remove background from images")
    parser.add_argument("--input", "-i", help="Input image path")
    parser.add_argument("--output", "-o", help="Output image path")
    parser.add_argument("--color", "-c", default="green",
                        choices=["green", "blue", "white", "black", "transparent"],
                        help="Background color (default: green)")
    parser.add_argument("--blur", "-b", action="store_true",
                        help="Blur background instead of replacing")
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
    
    # Parse background color
    color_map = {
        "green": (0, 255, 0),
        "blue": (255, 0, 0),
        "white": (255, 255, 255),
        "black": (0, 0, 0),
        "transparent": None
    }
    bg_color = None if args.blur else color_map.get(args.color, (0, 255, 0))
    
    remover = BackgroundRemover()
    remover.process_file(args.input, args.output, bg_color)
    remover.close()


if __name__ == "__main__":
    main()
