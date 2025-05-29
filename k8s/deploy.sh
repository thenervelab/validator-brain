#!/bin/bash

# IPFS Service Validator Deployment Script
# This script deploys all components with high-scale PostgreSQL configuration built-in

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

# Build the Docker image
echo "🏗️  Building Docker image..."
cd ..
docker build -t ipfs-service-validator:latest .
cd k8s

log "🚀 Deploying IPFS Service Validator with High-Scale PostgreSQL"

# Apply ConfigMap
log "⚙️  Applying ConfigMap..."
kubectl apply -f k8s/configmap.yaml

# Deploy PostgreSQL with built-in high-scale configuration
log "🐘 Deploying PostgreSQL with high-scale configuration (1000 connections)..."
kubectl apply -f k8s/postgres.yaml

# Wait for PostgreSQL to be ready
log "⏳ Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres --timeout=300s

# Deploy RabbitMQ
log "🐰 Deploying RabbitMQ..."
kubectl apply -f k8s/rabbitmq.yaml

# Wait for RabbitMQ to be ready
log "⏳ Waiting for RabbitMQ to be ready..."
kubectl wait --for=condition=ready pod -l app=rabbitmq --timeout=300s

# Deploy IPFS
log "📦 Deploying IPFS StatefulSet..."
kubectl apply -f k8s/ipfs.yaml

# Deploy IPFS HPA
log "📈 Deploying IPFS HPA..."
kubectl apply -f k8s/ipfs-hpa.yaml

# Wait for IPFS to be ready
log "⏳ Waiting for IPFS to be ready..."
kubectl wait --for=condition=ready pod -l app=ipfs --timeout=300s

# Deploy consumers
log "🔄 Deploying consumers..."
kubectl apply -f k8s/consumers.yaml

# Deploy epoch orchestrator
log "🎭 Deploying epoch orchestrator..."
kubectl apply -f k8s/epoch-orchestrator.yaml

# Wait for all deployments to be ready
log "⏳ Waiting for all deployments to be ready..."
kubectl wait --for=condition=available deployment --all --timeout=600s

log "✅ Deployment complete!"

echo ""
echo -e "${BLUE}📊 Deployment Status:${NC}"
kubectl get all

echo ""
echo -e "${GREEN}🎉 High-Scale PostgreSQL Configuration:${NC}"
echo "  ✅ Max connections: 1000"
echo "  ✅ Shared buffers: 512MB"
echo "  ✅ Effective cache size: 2GB"
echo "  ✅ Work memory: 8MB"
echo "  ✅ WAL buffers: 32MB"
echo "  ✅ Max WAL size: 8GB"
echo "  ✅ Worker processes: 16"
echo "  ✅ Parallel workers: 16"

echo ""
echo -e "${BLUE}💡 Useful Commands:${NC}"
echo "  # Check all resources:"
echo "  kubectl get all"
echo ""
echo "  # Check PostgreSQL logs:"
echo "  kubectl logs -l app=postgres --tail=50"
echo ""
echo "  # Check PostgreSQL configuration:"
echo "  kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SHOW max_connections;\""
echo ""
echo "  # Monitor PostgreSQL connections:"
echo "  kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SELECT count(*) FROM pg_stat_activity;\""
echo ""
echo "  # Check epoch orchestrator logs:"
echo "  kubectl logs -l app=epoch-orchestrator --tail=50"
echo ""
echo "  # Scale IPFS:"
echo "  kubectl scale statefulset ipfs --replicas=3"

echo ""
echo -e "${YELLOW}📋 Connection Capacity:${NC}"
echo "  • 9 consumers × 8 max connections = 72 connections"
echo "  • 1 epoch orchestrator × 8 max connections = 8 connections"
echo "  • Reserve for scaling: ~920 connections available"
echo "  • Total capacity: 1000 connections"

echo ""
echo -e "${GREEN}🎉 Ready for high-scale operations!${NC}" 