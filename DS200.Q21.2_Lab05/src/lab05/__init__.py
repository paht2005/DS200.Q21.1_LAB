"""
Lab05 - Optional Python Implementation

This package contains the optional Python implementation of the
Real-Time Person Counting System. The primary implementation is
in Java with Spark Streaming (see spark/java/lab05-streaming/).

Modules:
- config.py: Configuration settings
- receiver_server.py: Frame receiver server
- processing_server.py: Object detection with PySpark
- storage_server.py: Results storage
- video_source.py: Test video/image source

Usage:
    # Start servers in separate terminals
    python -m lab05.storage_server
    python -m lab05.processing_server
    python -m lab05.receiver_server
    python -m lab05.video_source
"""
