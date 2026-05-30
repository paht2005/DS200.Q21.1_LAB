#!/bin/bash
# ==============================================================================
# Lab05 - Real-time Person Counting System with Java Spark Streaming
# Build and run script for local execution
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAB_DIR="$(dirname "$SCRIPT_DIR")"
SPARK_PROJECT="$LAB_DIR/spark/java/lab05-streaming"
OUTPUT_DIR="$LAB_DIR/output/results"
JAR_FILE="$SPARK_PROJECT/target/lab05-streaming-1.0-SNAPSHOT.jar"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║      Lab05 - Real-time Person Counting System (Java)         ║"
    echo "║                  with Spark Streaming                        ║"
    echo "║                                                              ║"
    echo "║  Student ID: 23521143                                        ║"
    echo "║  Course: DS200.Q21.2 - Big Data                              ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

check_requirements() {
    echo -e "${YELLOW}Checking requirements...${NC}"
    
    # Check Java
    if ! command -v java &> /dev/null; then
        echo -e "${RED}Error: Java is not installed${NC}"
        exit 1
    fi
    echo -e "  ${GREEN}✓${NC} Java $(java -version 2>&1 | head -1)"
    
    # Check Maven
    if ! command -v mvn &> /dev/null; then
        echo -e "${RED}Error: Maven is not installed${NC}"
        exit 1
    fi
    echo -e "  ${GREEN}✓${NC} Maven $(mvn -v | head -1)"
    
    # Check Spark (optional)
    if command -v spark-submit &> /dev/null; then
        echo -e "  ${GREEN}✓${NC} Spark $(spark-submit --version 2>&1 | head -3 | tail -1)"
    else
        echo -e "  ${YELLOW}!${NC} Spark not found (will use embedded)"
    fi
    
    echo ""
}

build_project() {
    echo -e "${YELLOW}Building Maven project...${NC}"
    cd "$SPARK_PROJECT"
    mvn clean package -DskipTests -q
    echo -e "${GREEN}Build successful!${NC}"
    echo -e "JAR file: ${JAR_FILE}"
    echo ""
}

run_component() {
    local component=$1
    echo -e "${YELLOW}Starting ${component}...${NC}"
    
    case $component in
        storage)
            java -cp "$JAR_FILE" lab05.StorageServer
            ;;
        processing)
            java -cp "$JAR_FILE" lab05.ProcessingServer
            ;;
        receiver)
            java -cp "$JAR_FILE" lab05.FrameReceiverServer
            ;;
        source)
            shift
            java -cp "$JAR_FILE" lab05.VideoSource "$@"
            ;;
        *)
            echo -e "${RED}Unknown component: ${component}${NC}"
            print_usage
            exit 1
            ;;
    esac
}

print_usage() {
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  build      - Build the Maven project"
    echo "  storage    - Start Storage Server (port 6300)"
    echo "  processing - Start Processing Server with Spark (port 6200)"
    echo "  receiver   - Start Frame Receiver Server (port 6100)"
    echo "  source     - Start Video Source Simulator"
    echo "  demo       - Print instructions for full demo"
    echo ""
    echo "Example: Run in 4 terminals in order:"
    echo "  Terminal 1: $0 storage"
    echo "  Terminal 2: $0 processing"
    echo "  Terminal 3: $0 receiver"
    echo "  Terminal 4: $0 source"
    echo ""
}

print_demo() {
    echo -e "${GREEN}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                    DEMO INSTRUCTIONS                         ║"
    echo "╠══════════════════════════════════════════════════════════════╣"
    echo "║                                                              ║"
    echo "║  1. Open 4 terminal windows                                  ║"
    echo "║                                                              ║"
    echo "║  2. In Terminal 1 (Storage):                                 ║"
    echo "║     cd $LAB_DIR"
    echo "║     ./scripts/run_java_streaming_local.sh storage            ║"
    echo "║                                                              ║"
    echo "║  3. In Terminal 2 (Processing + Spark):                      ║"
    echo "║     cd $LAB_DIR"
    echo "║     ./scripts/run_java_streaming_local.sh processing         ║"
    echo "║                                                              ║"
    echo "║  4. In Terminal 3 (Receiver):                                ║"
    echo "║     cd $LAB_DIR"
    echo "║     ./scripts/run_java_streaming_local.sh receiver           ║"
    echo "║                                                              ║"
    echo "║  5. In Terminal 4 (Source - test frames):                    ║"
    echo "║     cd $LAB_DIR"
    echo "║     ./scripts/run_java_streaming_local.sh source             ║"
    echo "║                                                              ║"
    echo "║  Results saved to: output/results/detections.json            ║"
    echo "║                                                              ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Main
cd "$LAB_DIR"
print_header

case "${1:-}" in
    build)
        check_requirements
        build_project
        ;;
    storage|processing|receiver)
        if [ ! -f "$JAR_FILE" ]; then
            check_requirements
            build_project
        fi
        run_component "$@"
        ;;
    source)
        if [ ! -f "$JAR_FILE" ]; then
            check_requirements
            build_project
        fi
        run_component "$@"
        ;;
    demo)
        print_demo
        ;;
    *)
        print_usage
        ;;
esac
