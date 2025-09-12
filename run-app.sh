#!/bin/bash

# Simple wrapper script for running the application with environment variables
# Usage: ./run-app.sh <logname> [other-options...]

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
    exit 1
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
    sbt -Djline.terminal=none "runMain auth.datalab.siesta.Main --logname $LOGNAME"
fi
