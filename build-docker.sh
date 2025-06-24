#!/bin/bash

# Docker build script with compression and optimization
set -e

# Configuration
IMAGE_NAME="ipfs-service-validator"
REGISTRY="registry.starkleytech.com/library"
TAG=${1:-$(git rev-parse --short HEAD)}

echo "🐳 Building optimized Docker image..."
echo "   Image: ${REGISTRY}/${IMAGE_NAME}:${TAG}"

# Build with Alpine (smallest) - try this first
echo "📦 Building Alpine-based image (smallest)..."
if docker buildx build \
    --platform linux/amd64 \
    --file Dockerfile.alpine \
    --tag ${REGISTRY}/${IMAGE_NAME}:${TAG}-alpine \
    --compress \
    --squash \
    --push \
    .; then
    echo "✅ Alpine build successful!"
    echo "   Image: ${REGISTRY}/${IMAGE_NAME}:${TAG}-alpine"
    exit 0
fi

echo "⚠️ Alpine build failed, trying optimized Debian..."

# Build with optimized Debian (fallback)
echo "📦 Building optimized Debian-based image..."
if docker buildx build \
    --platform linux/amd64 \
    --file Dockerfile \
    --tag ${REGISTRY}/${IMAGE_NAME}:${TAG} \
    --compress \
    --squash \
    --push \
    .; then
    echo "✅ Debian build successful!"
    echo "   Image: ${REGISTRY}/${IMAGE_NAME}:${TAG}"
    exit 0
fi

echo "❌ Both builds failed!"
exit 1 