#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
    echo "Environment variables loaded from .env file"
else
    echo "Warning: .env file not found"
fi

# Check if arguments are provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 [sbt-command] [arguments...]"
    echo "Example: $0 run --logname log_t5e5"
    echo "Example: $0 compile"
    echo "Example: $0 \"run --logname log_t5e5\""
    exit 1
fi

# If the first argument contains spaces, it's likely a quoted command
if [[ "$1" == *" "* ]]; then
    echo "Running: sbt '$1'"
    sbt "$1"
else
    echo "Running: sbt $*"
    sbt "$@"
fi
