#!/bin/bash

# Script to build and push cbdeclare image to DockerHub
# Usage: ./build-and-push-docker.sh [version]

set -e

# Configuration
IMAGE_NAME="siesta-cbdeclare"
VERSION="${1:-latest}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Building cbdeclare Docker image...${NC}"
echo "Image: ${DOCKERHUB_USERNAME:-sista-tool}/${IMAGE_NAME}:${VERSION}"
echo ""

# Build the image
echo -e "${BLUE}Step 1/3: Building image...${NC}"
docker build -t ${DOCKERHUB_USERNAME:-sista-tool}/${IMAGE_NAME}:${VERSION} \
  --target execution \
  -f Dockerfile .

# Also tag as latest if a specific version was provided
if [ "$VERSION" != "latest" ]; then
  echo -e "${BLUE}Tagging as latest...${NC}"
  docker tag ${DOCKERHUB_USERNAME:-sista-tool}/${IMAGE_NAME}:${VERSION} ${DOCKERHUB_USERNAME:-sista-tool}/${IMAGE_NAME}:latest
fi

echo -e "${GREEN}Build completed successfully!${NC}"
echo ""

# Check if user is logged in to DockerHub
echo -e "${BLUE}Step 2/3: Checking DockerHub login...${NC}"
if ! docker info | grep -q "Username"; then
  echo -e "${RED}Not logged in to DockerHub. Please run: docker login${NC}"
  exit 1
fi

echo -e "${GREEN}Logged in to DockerHub${NC}"
echo ""

# Push the image
echo -e "${BLUE}Step 3/3: Pushing to DockerHub...${NC}"
docker push ${DOCKERHUB_USERNAME:-sista-tool}/${IMAGE_NAME}:${VERSION}

if [ "$VERSION" != "latest" ]; then
  docker push ${DOCKERHUB_USERNAME:-sista-tool}/${IMAGE_NAME}:latest
fi

echo ""
echo -e "${GREEN}Successfully pushed to DockerHub!${NC}"
echo ""
echo "Images available at:"
echo "  docker pull ${DOCKERHUB_USERNAME:-sista-tool}/${IMAGE_NAME}:${VERSION}"
if [ "$VERSION" != "latest" ]; then
  echo "  docker pull ${DOCKERHUB_USERNAME:-sista-tool}/${IMAGE_NAME}:latest"
fi
