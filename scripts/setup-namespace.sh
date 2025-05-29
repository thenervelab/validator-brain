#!/bin/bash

# Namespace Setup Script for IPFS Service Validator
# This script adds namespace support to all Kubernetes resources

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

NAMESPACE="ipfs-validator"

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

header() {
    echo -e "${CYAN}================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}================================${NC}"
}

# Function to add namespace to a YAML file
add_namespace_to_file() {
    local file="$1"
    local temp_file="${file}.tmp"
    
    if [[ ! -f "$file" ]]; then
        warn "File $file not found, skipping..."
        return
    fi
    
    log "Adding namespace to $file..."
    
    # Use sed to add namespace after metadata: name: lines
    sed '/^metadata:/,/^[[:space:]]*name:/ {
        /^[[:space:]]*name:/ a\
  namespace: '"$NAMESPACE"'
    }' "$file" > "$temp_file"
    
    # Only replace if the sed command was successful and the file changed
    if [[ -s "$temp_file" ]] && ! cmp -s "$file" "$temp_file"; then
        mv "$temp_file" "$file"
        log "✅ Updated $file with namespace"
    else
        rm -f "$temp_file"
        log "ℹ️  $file already has namespace or no changes needed"
    fi
}

# Function to create PVC with namespace
create_pvc_with_namespace() {
    log "Creating PostgreSQL PVC with namespace..."
    
    cat > k8s/postgres-pvc.yaml << EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-pvc
  namespace: $NAMESPACE
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
  storageClassName: standard
EOF
    
    log "✅ Created k8s/postgres-pvc.yaml with namespace"
}

# Function to update deploy script for namespace
update_deploy_script() {
    log "Updating deploy script for namespace support..."
    
    # Backup original deploy script
    if [[ -f "k8s/deploy.sh" ]]; then
        cp "k8s/deploy.sh" "k8s/deploy.sh.backup"
    fi
    
    cat > k8s/deploy.sh << 'EOF'
#!/bin/bash

# IPFS Service Validator Deployment Script with Namespace Support
# This script deploys all components to the ipfs-validator namespace

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

NAMESPACE="ipfs-validator"

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

log "🚀 Deploying IPFS Service Validator to namespace: $NAMESPACE"

# Create namespace
log "📁 Creating namespace..."
kubectl apply -f k8s/namespace.yaml

# Apply ConfigMap
log "⚙️  Applying ConfigMap..."
kubectl apply -f k8s/configmap.yaml

# Create PostgreSQL PVC
log "💾 Creating PostgreSQL PVC..."
kubectl apply -f k8s/postgres-pvc.yaml

# Deploy PostgreSQL
log "🐘 Deploying PostgreSQL..."
kubectl apply -f k8s/postgres.yaml

# Wait for PostgreSQL to be ready
log "⏳ Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres -n $NAMESPACE --timeout=300s

# Deploy RabbitMQ
log "🐰 Deploying RabbitMQ..."
kubectl apply -f k8s/rabbitmq.yaml

# Wait for RabbitMQ to be ready
log "⏳ Waiting for RabbitMQ to be ready..."
kubectl wait --for=condition=ready pod -l app=rabbitmq -n $NAMESPACE --timeout=300s

# Deploy IPFS
log "📦 Deploying IPFS StatefulSet..."
kubectl apply -f k8s/ipfs.yaml

# Deploy IPFS HPA
log "📈 Deploying IPFS HPA..."
kubectl apply -f k8s/ipfs-hpa.yaml

# Wait for IPFS to be ready
log "⏳ Waiting for IPFS to be ready..."
kubectl wait --for=condition=ready pod -l app=ipfs -n $NAMESPACE --timeout=300s

# Deploy consumers
log "🔄 Deploying consumers..."
kubectl apply -f k8s/consumers.yaml

# Deploy epoch orchestrator
log "🎭 Deploying epoch orchestrator..."
kubectl apply -f k8s/epoch-orchestrator.yaml

# Wait for all deployments to be ready
log "⏳ Waiting for all deployments to be ready..."
kubectl wait --for=condition=available deployment --all -n $NAMESPACE --timeout=600s

log "✅ Deployment complete!"

echo ""
echo -e "${BLUE}📊 Deployment Status:${NC}"
kubectl get all -n $NAMESPACE

echo ""
echo -e "${BLUE}💡 Useful Commands:${NC}"
echo "  # Check all resources in namespace:"
echo "  kubectl get all -n $NAMESPACE"
echo ""
echo "  # Check logs:"
echo "  kubectl logs -l app=epoch-orchestrator -n $NAMESPACE --tail=50"
echo ""
echo "  # Monitor PostgreSQL connections:"
echo "  bash scripts/monitor-postgres.sh"
echo ""
echo "  # Scale IPFS:"
echo "  kubectl scale statefulset ipfs --replicas=3 -n $NAMESPACE"
EOF

    chmod +x k8s/deploy.sh
    log "✅ Updated k8s/deploy.sh with namespace support"
}

# Function to update monitoring scripts for namespace
update_monitoring_scripts() {
    log "Updating monitoring scripts for namespace support..."
    
    # Update fix-db-connections.sh
    sed -i.backup "s/kubectl exec -it deployment\/postgres/kubectl exec -it deployment\/postgres -n $NAMESPACE/g" scripts/fix-db-connections.sh
    sed -i "s/kubectl rollout status deployment/kubectl rollout status deployment -n $NAMESPACE/g" scripts/fix-db-connections.sh
    sed -i "s/kubectl rollout restart deployment/kubectl rollout restart deployment -n $NAMESPACE/g" scripts/fix-db-connections.sh
    sed -i "s/kubectl logs -l app=/kubectl logs -l app= -n $NAMESPACE/g" scripts/fix-db-connections.sh
    
    # Update monitor-postgres.sh
    sed -i.backup "s/kubectl exec -it deployment\/postgres/kubectl exec -it deployment\/postgres -n $NAMESPACE/g" scripts/monitor-postgres.sh
    
    log "✅ Updated monitoring scripts with namespace support"
}

# Main execution
header "🏗️  SETTING UP NAMESPACE SUPPORT"

# Files that need namespace added
YAML_FILES=(
    "k8s/rabbitmq.yaml"
    "k8s/ipfs.yaml"
    "k8s/ipfs-hpa.yaml"
    "k8s/consumers.yaml"
    "k8s/epoch-orchestrator.yaml"
)

# Add namespace to existing files
for file in "${YAML_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        add_namespace_to_file "$file"
    else
        warn "File $file not found, will need to be created with namespace"
    fi
done

# Create PVC with namespace
create_pvc_with_namespace

# Update deploy script
update_deploy_script

# Update monitoring scripts
update_monitoring_scripts

echo ""
header "✅ NAMESPACE SETUP COMPLETE"

echo ""
echo -e "${BLUE}📋 Summary:${NC}"
echo "  • Namespace: $NAMESPACE"
echo "  • Updated YAML files with namespace"
echo "  • Created PostgreSQL PVC with namespace"
echo "  • Updated deployment script"
echo "  • Updated monitoring scripts"
echo ""
echo -e "${YELLOW}🚀 Next Steps:${NC}"
echo "  1. Deploy to namespace:"
echo "     bash k8s/deploy.sh"
echo ""
echo "  2. Monitor deployment:"
echo "     kubectl get all -n $NAMESPACE"
echo ""
echo "  3. Check PostgreSQL connections:"
echo "     bash scripts/monitor-postgres.sh"
echo ""
echo -e "${CYAN}💡 Namespace Commands:${NC}"
echo "  # Switch to namespace context:"
echo "  kubectl config set-context --current --namespace=$NAMESPACE"
echo ""
echo "  # View all resources in namespace:"
echo "  kubectl get all -n $NAMESPACE"
echo ""
echo "  # Delete entire namespace (if needed):"
echo "  kubectl delete namespace $NAMESPACE" 