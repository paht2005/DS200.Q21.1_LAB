"""
Demo Example - End-to-end demonstration of the person counting system.

This script demonstrates all components of the distributed person counting system:
1. Storage server (receives and stores results)
2. Detector server (performs object detection)
3. Receiver server (receives and forwards frames)
4. Sender (sends video frames)

Usage:
    python demo-example.py [--frames N] [--video PATH]
"""

import subprocess
import time
import sys
import os
import signal
import argparse
import json
from datetime import datetime
from threading import Thread

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config


class DemoRunner:
    """Runs the complete person counting demo."""
    
    def __init__(self, num_frames=10, video_path=None):
        self.num_frames = num_frames
        self.video_path = video_path
        self.processes = []
        self.src_dir = os.path.dirname(os.path.abspath(__file__))
        
    def print_banner(self):
        """Print demo banner."""
        print()
        print("=" * 70)
        print("  REAL-TIME PERSON COUNTING SYSTEM - DEMO")
        print("=" * 70)
        print(f"  Course: DS200.Q21.1 - Big Data Lab 05")
        print(f"  Student ID: 23521143")
        print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        print()
    
    def print_architecture(self):
        """Print system architecture."""
        print("System Architecture:")
        print()
        print("  ┌─────────────┐     TCP      ┌──────────────┐")
        print("  │   Video/    │ ──────────► │   Receiver   │")
        print("  │   Camera    │   frames    │  (port 6100) │")
        print("  │  (sender)   │             └──────┬───────┘")
        print("  └─────────────┘                    │")
        print("                                     │ forward")
        print("                                     ▼")
        print("                            ┌──────────────┐")
        print("                            │   Detector   │")
        print("                            │  (port 6200) │")
        print("                            └──────┬───────┘")
        print("                                   │ results")
        print("                                   ▼")
        print("                            ┌──────────────┐")
        print("                            │   Storage    │")
        print("                            │  (port 6300) │")
        print("                            └──────────────┘")
        print("                                   │")
        print("                                   ▼")
        print("                       output/detections.json")
        print()
    
    def start_server(self, script_name, delay=1):
        """Start a server script in background."""
        script_path = os.path.join(self.src_dir, script_name)
        
        if not os.path.exists(script_path):
            print(f"  ✗ Script not found: {script_path}")
            return None
        
        print(f"  → Starting {script_name}...")
        
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.src_dir
        )
        
        self.processes.append((script_name, process))
        time.sleep(delay)
        
        if process.poll() is None:
            print(f"  ✓ {script_name} started (PID: {process.pid})")
            return process
        else:
            print(f"  ✗ {script_name} failed to start")
            return None
    
    def run_sender(self):
        """Run the sender to send frames."""
        script_path = os.path.join(self.src_dir, "sender.py")
        
        cmd = [sys.executable, script_path, "--frames", str(self.num_frames)]
        
        if self.video_path:
            cmd.extend(["--video", self.video_path])
        
        print(f"\n  → Sending {self.num_frames} frames...")
        print()
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self.src_dir,
            text=True
        )
        
        # Stream output
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line:
                print(f"    {line.rstrip()}")
        
        process.wait()
        return process.returncode == 0
    
    def stop_servers(self):
        """Stop all running servers."""
        print("\n  Stopping servers...")
        
        for name, process in self.processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                    print(f"  ✓ {name} stopped")
                except subprocess.TimeoutExpired:
                    process.kill()
                    print(f"  ✓ {name} killed")
        
        self.processes.clear()
    
    def check_results(self):
        """Check and display results."""
        results_file = os.path.join(
            self.src_dir, "..", "data", "results", "detections.json"
        )
        
        # Alternative location
        if not os.path.exists(results_file):
            results_file = os.path.join(
                self.src_dir, "..", "output", "detections.json"
            )
        
        print("\n" + "=" * 70)
        print("  RESULTS")
        print("=" * 70)
        
        if os.path.exists(results_file):
            try:
                with open(results_file, 'r') as f:
                    results = json.load(f)
                
                print(f"\n  Results file: {results_file}")
                print(f"  Total detections: {len(results)}")
                
                if results:
                    # Show last few results
                    print("\n  Recent detections:")
                    for result in results[-5:]:
                        frame_num = result.get("frame_number", "?")
                        person_count = result.get("detection", {}).get("person_count", 0)
                        method = result.get("detection", {}).get("detection_method", "?")
                        proc_time = result.get("processing_time_ms", 0)
                        
                        print(f"    Frame #{frame_num}: {person_count} person(s) "
                              f"[{method}] ({proc_time}ms)")
                    
                    # Summary
                    total_persons = sum(
                        r.get("detection", {}).get("person_count", 0) 
                        for r in results
                    )
                    avg_count = total_persons / len(results) if results else 0
                    
                    print(f"\n  Summary:")
                    print(f"    Total frames processed: {len(results)}")
                    print(f"    Total persons detected: {total_persons}")
                    print(f"    Average per frame: {avg_count:.1f}")
                    
            except json.JSONDecodeError:
                print("  ✗ Could not parse results file")
            except Exception as e:
                print(f"  ✗ Error reading results: {e}")
        else:
            print("  ✗ No results file found")
            print(f"    Expected: {results_file}")
    
    def run(self):
        """Run the complete demo."""
        self.print_banner()
        self.print_architecture()
        
        print("Starting Demo...")
        print("-" * 70)
        
        try:
            # Start servers in order
            print("\n[1/4] Starting Storage Server...")
            storage = self.start_server("storage_server.py", delay=2)
            
            print("\n[2/4] Starting Detector Server...")
            detector = self.start_server("detect_object.py", delay=2)
            
            print("\n[3/4] Starting Receiver Server...")
            receiver = self.start_server("receiver.py", delay=2)
            
            if not all([storage, detector, receiver]):
                print("\n✗ Failed to start all servers. Aborting.")
                self.stop_servers()
                return False
            
            print("\n[4/4] Starting Sender...")
            success = self.run_sender()
            
            # Wait for processing to complete
            print("\n  Waiting for processing to complete...")
            time.sleep(3)
            
            # Check results
            self.check_results()
            
            return success
            
        except KeyboardInterrupt:
            print("\n\nDemo interrupted by user")
            return False
        finally:
            self.stop_servers()
            print("\n" + "=" * 70)
            print("  Demo Complete!")
            print("=" * 70)


def quick_test():
    """Quick test of individual components."""
    print("Quick Component Test")
    print("-" * 40)
    
    src_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Test imports
    print("\n1. Testing imports...")
    
    tests = [
        ("config", "Configuration"),
        ("sender", "Sender"),
        ("receiver", "Receiver"),
        ("detect_object", "Detector"),
        ("storage_server", "Storage"),
    ]
    
    for module, name in tests:
        try:
            __import__(module)
            print(f"   ✓ {name} module OK")
        except ImportError as e:
            print(f"   ✗ {name} module FAILED: {e}")
    
    # Test dependencies
    print("\n2. Testing dependencies...")
    
    deps = [
        ("cv2", "OpenCV"),
        ("numpy", "NumPy"),
        ("mediapipe", "MediaPipe"),
        ("ultralytics", "YOLO"),
    ]
    
    for module, name in deps:
        try:
            __import__(module)
            print(f"   ✓ {name} available")
        except ImportError:
            print(f"   ⚠ {name} not installed (optional)")
    
    print("\n3. Testing configuration...")
    from config import Config
    print(f"   Receiver port: {Config.RECEIVER_PORT}")
    print(f"   Detector port: {Config.PROCESSING_PORT}")
    print(f"   Storage port: {Config.STORAGE_PORT}")
    
    print("\nQuick test complete!")


def main():
    parser = argparse.ArgumentParser(
        description="End-to-end demo of person counting system"
    )
    parser.add_argument("--frames", "-n", type=int, default=10,
                        help="Number of frames to process (default: 10)")
    parser.add_argument("--video", "-v", help="Video file to process")
    parser.add_argument("--test", "-t", action="store_true",
                        help="Run quick component test only")
    
    args = parser.parse_args()
    
    if args.test:
        quick_test()
        return
    
    # Convert video path to absolute path if provided
    video_path = args.video
    if video_path and not os.path.isabs(video_path):
        video_path = os.path.abspath(video_path)
    
    demo = DemoRunner(
        num_frames=args.frames,
        video_path=video_path
    )
    
    success = demo.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
