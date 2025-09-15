#!/bin/bash

# Simple wrapper script for running the application with environment variables
# Usage: ./run-app.sh <logname> [other-options...]
# Example: ./run-app.sh log_t5e5 --support 0.5 --outputPath ./output

# Load environment variables from .env file
if [ -f .env ]; then
    set -a  # automatically export all variables
    source .env
    set +a
    echo "Environment variables loaded from .env file"
else
    echo "Warning: .env file not found"
fi

# Check if logname is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <logname> [other-options...]"
    echo "Example: $0 log_t5e5"
    echo "Example: $0 log_t5e5 --support 0.5"
    echo "Example: $0 log_t5e5 --support 0.5 --outputPath ./my-output"
    echo ""
    echo "Available options:"
    echo "  --support <value>          Support threshold (default: 0)"
    echo "  --branchingPolicy <policy> Branching policy (e.g., AND, OR)"
    echo "  --branchingType <type>     Branching type (default: TARGET)"
    echo "  --branchingBound <bound>   Branching bound (default: 0)"
    echo "  --outputPath <path>        Output directory (default: ./output)"
    echo "  --filterRare <bool>        Filter rare events"
    echo "  --filterUnderBound <bool>  Filter under-bound templates"
    echo "  --hardRediscover <bool>    Hard rediscovery"
    echo "  --quickMining <bool>       Quick mining"
    exit 1
fi

# Create default output directory if not specified
DEFAULT_OUTPUT="./output"
if [[ "$*" != *"--outputPath"* ]] && [[ "$*" != *"-o"* ]]; then
    mkdir -p "$DEFAULT_OUTPUT"
    echo "Using default output directory: $DEFAULT_OUTPUT"
fi

# Build the run command
LOGNAME="$1"
shift  # Remove first argument (logname)
OTHER_ARGS="$@"

echo "Running application with logname: $LOGNAME"
if [ -n "$OTHER_ARGS" ]; then
    echo "Additional arguments: $OTHER_ARGS"
    sbt -Djline.terminal=none "runMain auth.datalab.siesta.Main --logname $LOGNAME $OTHER_ARGS"
else
    sbt -Djline.terminal=none "runMain auth.datalab.siesta.Main --logname $LOGNAME --outputPath $DEFAULT_OUTPUT"
fi
