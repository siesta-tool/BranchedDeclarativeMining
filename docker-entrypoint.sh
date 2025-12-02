#!/bin/bash
set -e

# Default Spark configuration
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-10g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
LOG_TO_FILE="${LOG_TO_FILE:-false}"

# Default S3 configuration
export s3accessKeyAws="${s3accessKeyAws:-minioadmin}"
export s3secretKeyAws="${s3secretKeyAws:-minioadmin}"
export s3endPointLoc="${s3endPointLoc:-http://minio:9000}"
export s3ConnectionTimeout="${s3ConnectionTimeout:-600000}"

# Default mining parameters
LOG_NAME="${LOG_NAME:-}"
SUPPORT="${SUPPORT:-0.1}"
BRANCHING_POLICY="${BRANCHING_POLICY:-}"
BRANCHING_TYPE="${BRANCHING_TYPE:-TARGET}"
BRANCHING_BOUND="${BRANCHING_BOUND:-3}"
OUTPUT_PATH="${OUTPUT_PATH:-/app/output}"
HARD_MODE="${HARD_MODE:-false}"
FILTER_UNDERBOUND="${FILTER_UNDERBOUND:-false}"

# Build the command arguments
ARGS=()

# Add log name if provided
if [ -n "$LOG_NAME" ]; then
    ARGS+=("-l" "$LOG_NAME")
fi

# Add support threshold
if [ -n "$SUPPORT" ]; then
    ARGS+=("--support" "$SUPPORT")
fi

# Add output path
if [ -n "$OUTPUT_PATH" ]; then
    ARGS+=("--outputPath" "$OUTPUT_PATH")
fi

# Add hard mode
if [ "$HARD_MODE" = "true" ]; then
    ARGS+=("-h" "true")
fi

# Add branching parameters if policy is set
if [ -n "$BRANCHING_POLICY" ]; then
    ARGS+=("-p" "$BRANCHING_POLICY")
    ARGS+=("-t" "$BRANCHING_TYPE")
    ARGS+=("-b" "$BRANCHING_BOUND")
fi

# Add filter underbound
if [ "$FILTER_UNDERBOUND" = "true" ]; then
    ARGS+=("-u" "true")
fi

# If no arguments provided and no LOG_NAME, print usage
if [ $# -eq 0 ] && [ -z "$LOG_NAME" ]; then
    cat <<EOF
cbdeclare - Incremental Declare Constraint Miner

Usage: Configure via environment variables or pass arguments directly

Environment Variables:
  S3 Configuration:
    s3accessKeyAws          S3/MinIO access key (default: minioadmin)
    s3secretKeyAws          S3/MinIO secret key (default: minioadmin)
    s3endPointLoc           S3/MinIO endpoint (default: http://minio:9000)
    s3ConnectionTimeout     Connection timeout (default: 600000)

  Spark Configuration:
    SPARK_MASTER            Spark master (default: local[*])
    SPARK_DRIVER_MEMORY     Driver memory (default: 10g)
    SPARK_EXECUTOR_MEMORY   Executor memory (default: 4g)

  Mining Parameters:
    LOG_NAME                Log name to process (required)
    SUPPORT                 Support threshold (default: 0.1)
    BRANCHING_POLICY        Branching policy: AND, OR, XOR (optional)
    BRANCHING_TYPE          Branching type: SOURCE, TARGET (default: TARGET)
    BRANCHING_BOUND         Max branching bound (default: 3)
    OUTPUT_PATH             Output path (default: /app/output)
    HARD_MODE               Hard rediscovery mode (default: false)
    FILTER_UNDERBOUND       Filter underbound constraints (default: false)

Examples:
  1. Simple run with environment variables:
     docker run -e LOG_NAME=bpic2012 -e SUPPORT=0.1 cbdeclare

  2. With branching:
     docker run -e LOG_NAME=bpic2012 -e BRANCHING_POLICY=OR -e BRANCHING_BOUND=3 cbdeclare

  3. Direct arguments:
     docker run cbdeclare -l bpic2012 --support 0.1 -p AND -t TARGET -b 3

  4. Interactive mode:
     docker run -it cbdeclare bash

EOF
    exit 0
fi

# If arguments are provided, use them directly
if [ $# -gt 0 ]; then
    # Check if first arg is a shell
    if [ "$1" = "bash" ] || [ "$1" = "sh" ] || [ "$1" = "/bin/bash" ] || [ "$1" = "/bin/sh" ]; then
        exec "$@"
    fi
    
    # If it's a spark-submit command, execute it
    if [[ "$1" == *"spark-submit"* ]] || [ "$1" = "spark-submit" ]; then
        exec "$@"
    fi
    
    # Otherwise, assume they're application arguments
    ARGS=("$@")
fi

# Create log file name with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="/app/logs/cbdeclare_${LOG_NAME:-job}_${TIMESTAMP}.log"

# Execute spark-submit with all configurations
if [ "$LOG_TO_FILE" = "true" ]; then
    echo "Logging to: $LOG_FILE"
    exec /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        --driver-memory "$SPARK_DRIVER_MEMORY" \
        --executor-memory "$SPARK_EXECUTOR_MEMORY" \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=/tmp/spark-events \
        --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:///app/log4j.properties" \
        --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:///app/log4j.properties" \
        /app/declare.jar \
        "${ARGS[@]}" 2>&1 | tee "$LOG_FILE"
else
    exec /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        --driver-memory "$SPARK_DRIVER_MEMORY" \
        --executor-memory "$SPARK_EXECUTOR_MEMORY" \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=/tmp/spark-events \
        --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:///app/log4j.properties" \
        --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file:///app/log4j.properties" \
        /app/declare.jar \
        "${ARGS[@]}"
fi
