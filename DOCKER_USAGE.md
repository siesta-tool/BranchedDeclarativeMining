# cbdeclare Docker Image Usage Guide

**DockerHub:** `balaktsis/siesta-cbdeclare:latest`

## Building and Pushing to DockerHub

### Prerequisites
1. Docker installed and running
2. DockerHub account
3. Logged in to DockerHub: `docker login`

### Quick Start

#### 1. Set your DockerHub username
```bash
export DOCKERHUB_USERNAME=your-dockerhub-username
```

#### 2. Build and push
```bash
# Build and push with version tag
./build-and-push-docker.sh 0.1.0

# Build and push as latest only
./build-and-push-docker.sh latest
```

### Manual Build Process

If you prefer to build manually:

```bash
# Build the image
docker build -t balaktsis/siesta-cbdeclare:latest \
  --target execution \
  -f Dockerfile .

# Tag with version
docker tag balaktsis/siesta-cbdeclare:latest \
  balaktsis/siesta-cbdeclare:0.1.0

# Push to DockerHub
docker push balaktsis/siesta-cbdeclare:latest
docker push balaktsis/siesta-cbdeclare:0.1.0
```

## Using the Image from DockerHub

### Pull the image
```bash
docker pull balaktsis/siesta-cbdeclare:latest
```

### Simple Run with Environment Variables (Recommended)

The image comes with sensible defaults and supports configuration via environment variables:

```bash
docker run --rm \
  -v $(pwd)/output:/app/output \
  -e LOG_NAME=bpic2012 \
  -e SUPPORT=0.1 \
  -e s3endPointLoc=http://minio:9000 \
  --network siesta-net \
  balaktsis/siesta-cbdeclare:latest
```

### Run with Branching
```bash
docker run --rm \
  -v $(pwd)/output:/app/output \
  -e LOG_NAME=bpic2012 \
  -e SUPPORT=0.0 \
  -e BRANCHING_POLICY=OR \
  -e BRANCHING_TYPE=TARGET \
  -e BRANCHING_BOUND=3 \
  -e s3endPointLoc=http://minio:9000 \
  --network siesta-net \
  balaktsis/siesta-cbdeclare:latest
```

### Run with Direct Arguments
You can still pass arguments directly:

```bash
docker run --rm \
  -v $(pwd)/output:/app/output \
  -e s3endPointLoc=http://minio:9000 \
  --network siesta-net \
  balaktsis/siesta-cbdeclare:latest \
  -l bpic2012 --support 0.1 -p OR -t TARGET -b 3
```

### Interactive Mode for Debugging
```bash
docker run -it --rm \
  -v $(pwd)/output:/app/output \
  -e s3endPointLoc=http://minio:9000 \
  --network siesta-net \
  balaktsis/siesta-cbdeclare:latest \
  bash
```

### Accessing Logs

#### Option 1: View logs with docker logs (default)
```bash
# Run container in background
docker run -d --name cbdeclare-job \
  -e LOG_NAME=bpic2012 \
  balaktsis/siesta-cbdeclare:latest

# View logs in real-time
docker logs -f cbdeclare-job

# View logs after completion
docker logs cbdeclare-job
```

#### Option 2: Save logs to file (recommended for production)
```bash
# Enable file logging and mount logs directory
docker run --rm \
  -v $(pwd)/logs:/app/logs \
  -e LOG_NAME=bpic2012 \
  -e LOG_TO_FILE=true \
  balaktsis/siesta-cbdeclare:latest

# Logs will be saved to: logs/cbdeclare_bpic2012_YYYYMMDD_HHMMSS.log
```

#### Option 3: In docker-compose
```yaml
cbdeclare:
  image: balaktsis/siesta-cbdeclare:latest
  volumes:
    - ./output:/app/output
    - ./logs:/app/logs  # Mount logs directory
  environment:
    LOG_NAME: bpic2012
    LOG_TO_FILE: true  # Enable file logging
```

### Using in docker-compose.yml

See `docker-compose.example.yml` for complete examples. Basic usage:

```yaml
services:
  cbdeclare:
    image: balaktsis/siesta-cbdeclare:latest
    container_name: cbdeclare
    networks:
      - siesta-net
    # volumes:  # Optional - image has built-in volume
    #   - ./output:/app/output  # Uncomment to access output on host
    environment:
      # S3 config (defaults are pre-configured for MinIO)
      s3endPointLoc: http://minio:9000
      # Mining parameters
      LOG_NAME: bpic2012
      SUPPORT: 0.1
      BRANCHING_POLICY: OR
      BRANCHING_TYPE: TARGET
      BRANCHING_BOUND: 3
    depends_on:
      - minio
```

## Image Details

### Multi-stage Build
The Dockerfile uses a three-stage build process:
1. **builder**: Base image with JDK and sbt
2. **declare**: Builds the Scala application and creates the JAR
3. **execution**: Final lightweight image with only runtime dependencies

### What's Included
- Eclipse Temurin JDK 17
- Apache Spark 3.5.6 with Hadoop 3
- cbdeclare JAR (`declare.jar`)
- Required system utilities (gnupg2, curl, procps)

### Image Size Optimization
The `.dockerignore` file excludes:
- Build artifacts (`target/`)
- IDE files (`.idea/`, `.vscode/`)
- Evaluation data
- Documentation files
- Local configuration

## Environment Variables

### S3/MinIO Configuration
All have sensible defaults for local MinIO:
- `s3accessKeyAws`: S3/MinIO access key (default: `minioadmin`)
- `s3secretKeyAws`: S3/MinIO secret key (default: `minioadmin`)
- `s3endPointLoc`: S3/MinIO endpoint URL (default: `http://minio:9000`)
- `s3ConnectionTimeout`: Connection timeout (default: `600000`)

### Spark Configuration
- `SPARK_MASTER`: Spark master URL (default: `local[*]`)
- `SPARK_DRIVER_MEMORY`: Driver memory allocation (default: `10g`)
- `SPARK_EXECUTOR_MEMORY`: Executor memory allocation (default: `4g`)

### Mining Parameters
- `LOG_NAME`: Name of the log to process (required if not using direct args)
- `SUPPORT`: Support threshold (default: `0.1`)
- `BRANCHING_POLICY`: Branching policy - `AND`, `OR`, or `XOR` (optional)
- `BRANCHING_TYPE`: Branching type - `SOURCE` or `TARGET` (default: `TARGET`)
- `BRANCHING_BOUND`: Maximum branching bound (default: `3`)
- `OUTPUT_PATH`: Output directory path (default: `/app/output`)
- `HARD_MODE`: Hard rediscovery mode - `true` or `false` (default: `false`)
- `FILTER_UNDERBOUND`: Filter underbound constraints - `true` or `false` (default: `false`)
- `LOG_TO_FILE`: Save logs to file in `/app/logs` - `true` or `false` (default: `false`)

### Configuration Priority
1. Direct command arguments (highest priority)
2. Environment variables
3. Built-in defaults (lowest priority)

## Versioning Strategy

Recommended versioning approach:
- Use semantic versioning: `major.minor.patch` (e.g., `0.1.0`, `1.0.0`)
- Always maintain a `latest` tag for the most recent stable version
- Tag commits with version numbers: `git tag v0.1.0`

Example workflow:
```bash
# Build version 0.1.0
./build-and-push-docker.sh 0.1.0

# This creates two tags:
#   - balaktsis/siesta-cbdeclare:0.1.0
#   - balaktsis/siesta-cbdeclare:latest
```
