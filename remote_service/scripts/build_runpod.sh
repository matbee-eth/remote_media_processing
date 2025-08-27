#!/bin/bash
# Build and push RemoteMedia gRPC service image for RunPod Pod deployment

set -e

# Configuration
IMAGE_NAME="acidhax/remotemedia-service"
TAG="${TAG:-latest}"

echo "Building RemoteMedia gRPC service for RunPod..."

# Build the image using Dockerfile.simple (optimized for RunPod)
docker build \
    -f remote_service/Dockerfile.simple \
    -t "$IMAGE_NAME:$TAG" \
    .

echo "✅ Image built: $IMAGE_NAME:$TAG"

# Push if requested
if [[ "$1" == "--push" ]]; then
    echo "Pushing to Docker Hub..."
    docker push "$IMAGE_NAME:$TAG"
    echo "✅ Image pushed to Docker Hub"
    echo "RunPod can now use: $IMAGE_NAME:$TAG"
fi

echo "Done! Use this image in RunPod Pod configuration:"
echo "  Image: $IMAGE_NAME:$TAG"
echo "  Port: 50051 (gRPC)"