#!/bin/bash

set -e

echo "🚀 Deploying IPFS Service Validator to Minikube..."

# Check if minikube is running
if ! minikube status > /dev/null 2>&1; then
    echo "❌ Minikube is not running. Please start it with: minikube start"
    exit 1
fi

# Configure Docker to use Minikube's daemon
echo "🔧 Configuring Docker to use Minikube's daemon..."
eval $(minikube docker-env)

# Build the Docker image
echo "🏗️  Building Docker image..."
cd ..
docker build -t ipfs-service-validator:latest .
cd k8s

# Create migrations ConfigMap
echo "📦 Creating migrations ConfigMap..."
chmod +x create-migrations-configmap.sh
./create-migrations-configmap.sh

# Apply Kubernetes manifests
echo "☸️  Applying Kubernetes manifests..."

echo "  - ConfigMap..."
kubectl apply -f configmap.yaml

echo "  - PersistentVolumeClaims..."
kubectl apply -f persistent-volumes.yaml

echo "  - PostgreSQL..."
kubectl apply -f postgres.yaml

echo "  - RabbitMQ..."
kubectl apply -f rabbitmq.yaml

echo "  - IPFS..."
kubectl apply -f ipfs.yaml

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres --timeout=120s
kubectl wait --for=condition=ready pod -l app=rabbitmq --timeout=120s
kubectl wait --for=condition=ready pod -l app=ipfs --timeout=120s

echo "  - Migrations ConfigMap..."
kubectl apply -f migrations-configmap.yaml

echo "  - Running database migrations..."
kubectl apply -f dbmate-job.yaml
kubectl wait --for=condition=complete job/dbmate-migrations --timeout=60s

echo "  - Deploying consumers..."
kubectl apply -f consumers.yaml

echo "  - Creating NodePort services..."
kubectl apply -f nodeports.yaml

# Get Minikube IP
MINIKUBE_IP=$(minikube ip)

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📋 Service URLs:"
echo "  - PostgreSQL: $MINIKUBE_IP:30432"
echo "  - RabbitMQ Management: http://$MINIKUBE_IP:30672 (admin/admin)"
echo "  - IPFS Gateway: http://$MINIKUBE_IP:30080"
echo "  - IPFS API: http://$MINIKUBE_IP:30501"
echo ""
echo "💡 Tips:"
echo "  - Mount your code for development: minikube mount $(dirname $(pwd)):/app"
echo "  - Check pod status: kubectl get pods"
echo "  - View logs: kubectl logs <pod-name>"
echo "  - Run processors: kubectl run processor --image=ipfs-service-validator:latest --rm -it -- bash" 