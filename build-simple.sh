#!/bin/bash

# Simple Docker build script without advanced features
set -e

# Configuration
IMAGE_NAME="ipfs-service-validator"
REGISTRY="registry.starkleytech.com/library"
TAG=${1:-$(git rev-parse --short HEAD)}

echo "🐳 Building Docker image (simple mode)..."
echo "   Image: ${REGISTRY}/${IMAGE_NAME}:${TAG}"

# Try Alpine first (smallest)
echo "📦 Building Alpine-based image..."
if docker build \
    --file Dockerfile.alpine \
    --tag ${REGISTRY}/${IMAGE_NAME}:${TAG}-alpine \
    . && \
   docker push ${REGISTRY}/${IMAGE_NAME}:${TAG}-alpine; then
    echo "✅ Alpine build and push successful!"
    echo "   Image: ${REGISTRY}/${IMAGE_NAME}:${TAG}-alpine"
    exit 0
fi

echo "⚠️ Alpine build failed, trying optimized Debian..."

# Fallback to Debian
echo "📦 Building Debian-based image..."
if docker build \
    --file Dockerfile \
    --tag ${REGISTRY}/${IMAGE_NAME}:${TAG} \
    . && \
   docker push ${REGISTRY}/${IMAGE_NAME}:${TAG}; then
    echo "✅ Debian build and push successful!"
    echo "   Image: ${REGISTRY}/${IMAGE_NAME}:${TAG}"
    exit 0
fi

echo "❌ Both builds failed!"
exit 1 