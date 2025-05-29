#!/bin/bash

# Kubernetes Cluster Initialization Script
# Run this script after installing Kubernetes to initialize the cluster
# Usage: bash scripts/init-k8s-cluster.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
    exit 1
}

header() {
    echo -e "${CYAN}================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}================================${NC}"
}

# Check if running as root for cluster init
if [[ $EUID -ne 0 ]]; then
   error "This script must be run as root for cluster initialization (use sudo)"
fi

header "🚀 KUBERNETES CLUSTER INITIALIZATION"

# Check if kubeadm is installed
if ! command -v kubeadm &> /dev/null; then
    error "kubeadm is not installed. Please run the installation script first."
fi

# Check if cluster is already initialized
if kubectl cluster-info &> /dev/null; then
    warn "Kubernetes cluster appears to be already initialized"
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log "Exiting..."
        exit 0
    fi
fi

# Initialize the cluster
log "🏗️  Initializing Kubernetes cluster..."
if [[ -f "/tmp/kubeadm-config.yaml" ]]; then
    log "Using existing kubeadm configuration..."
    kubeadm init --config=/tmp/kubeadm-config.yaml
else
    log "Using default kubeadm configuration..."
    kubeadm init --pod-network-cidr=10.244.0.0/16 --cri-socket=unix:///run/containerd/containerd.sock
fi

# Setup kubectl for root user
log "🔧 Setting up kubectl for root user..."
mkdir -p /root/.kube
cp -i /etc/kubernetes/admin.conf /root/.kube/config
chown root:root /root/.kube/config

# Wait for API server to be ready
log "⏳ Waiting for API server to be ready..."
sleep 10

# Install Flannel CNI
log "🌐 Installing Flannel CNI plugin..."
kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml

# Wait for Flannel to be ready
log "⏳ Waiting for Flannel pods to be ready..."
kubectl wait --for=condition=ready pod -l app=flannel -n kube-flannel --timeout=300s

# Install local-path storage provisioner
log "💾 Installing local-path storage provisioner..."
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml

# Wait for local-path provisioner to be ready
log "⏳ Waiting for local-path provisioner to be ready..."
kubectl wait --for=condition=ready pod -l app=local-path-provisioner -n local-path-storage --timeout=300s

# Set local-path as default storage class
log "🔧 Setting local-path as default storage class..."
kubectl patch storageclass local-path -p '{"metadata": {"annotations": {"storageclass.kubernetes.io/is-default-class": "true"}}}'

# Remove taint from control plane for single-node setup
log "🏷️  Removing taint from control plane (single-node setup)..."
kubectl taint nodes --all node-role.kubernetes.io/control-plane- || true

# Create kubectl config for regular users
log "👤 Creating kubectl setup instructions for regular users..."
cat > /tmp/setup-kubectl-user.sh << 'EOF'
#!/bin/bash

# Setup kubectl for regular user
# Run this script as your regular user (not root)

if [[ $EUID -eq 0 ]]; then
   echo "❌ Do not run this script as root. Run as your regular user."
   exit 1
fi

echo "🔧 Setting up kubectl for user: $(whoami)"

# Create .kube directory
mkdir -p $HOME/.kube

# Copy config (requires sudo)
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

echo "✅ kubectl configured for user: $(whoami)"
echo ""
echo "Test with: kubectl get nodes"
EOF

chmod +x /tmp/setup-kubectl-user.sh

# Wait a bit for everything to settle
log "⏳ Waiting for cluster to stabilize..."
sleep 30

# Verify installation
header "🔍 VERIFYING INSTALLATION"

log "📊 Cluster status:"
kubectl cluster-info

echo ""
log "📋 Node status:"
kubectl get nodes -o wide

echo ""
log "📦 System pods status:"
kubectl get pods --all-namespaces

echo ""
log "💾 Storage classes:"
kubectl get storageclass

echo ""
log "🌐 CNI pods status:"
kubectl get pods -n kube-flannel

echo ""
log "💾 Storage provisioner status:"
kubectl get pods -n local-path-storage

# Test storage provisioner
log "🧪 Testing storage provisioner..."
cat > /tmp/test-pvc.yaml << EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
EOF

kubectl apply -f /tmp/test-pvc.yaml
sleep 5

PVC_STATUS=$(kubectl get pvc test-pvc -o jsonpath='{.status.phase}')
if [[ "$PVC_STATUS" == "Bound" ]]; then
    log "✅ Storage provisioner test successful!"
    kubectl delete pvc test-pvc
else
    warn "⚠️  Storage provisioner test failed. PVC status: $PVC_STATUS"
fi

rm -f /tmp/test-pvc.yaml

header "✅ CLUSTER INITIALIZATION COMPLETE"

echo ""
echo -e "${BLUE}🎉 Cluster Summary:${NC}"
echo "  ✅ Kubernetes cluster initialized"
echo "  ✅ Flannel CNI installed and ready"
echo "  ✅ Local-path storage provisioner installed"
echo "  ✅ Local-path set as default storage class"
echo "  ✅ Control plane taint removed (single-node ready)"

echo ""
echo -e "${YELLOW}👤 For Regular Users:${NC}"
echo "  Run this command as your regular user (not root):"
echo "  bash /tmp/setup-kubectl-user.sh"

echo ""
echo -e "${BLUE}🔧 Useful Commands:${NC}"
echo "  # Check cluster status:"
echo "  kubectl get nodes"
echo "  kubectl get pods --all-namespaces"
echo ""
echo "  # Check storage:"
echo "  kubectl get storageclass"
echo "  kubectl get pv"
echo ""
echo "  # Deploy a test application:"
echo "  kubectl create deployment nginx --image=nginx"
echo "  kubectl expose deployment nginx --port=80 --type=NodePort"

echo ""
echo -e "${CYAN}🚀 Ready to deploy applications!${NC}"
echo ""
echo -e "${YELLOW}💡 Next Steps:${NC}"
echo "  1. Setup kubectl for regular users: bash /tmp/setup-kubectl-user.sh"
echo "  2. Deploy your applications with persistent storage"
echo "  3. Use 'kubectl get storageclass' to verify default storage"

# Save join command for worker nodes
KUBEADM_JOIN_CMD=$(kubeadm token create --print-join-command 2>/dev/null || echo "Failed to generate join command")
if [[ "$KUBEADM_JOIN_CMD" != "Failed to generate join command" ]]; then
    echo ""
    echo -e "${BLUE}🔗 Worker Node Join Command:${NC}"
    echo "  To add worker nodes to this cluster, run the following on each worker:"
    echo "  $KUBEADM_JOIN_CMD"
    
    # Save to file
    echo "$KUBEADM_JOIN_CMD" > /tmp/kubeadm-join-command.txt
    echo ""
    echo "  Join command saved to: /tmp/kubeadm-join-command.txt"
fi

echo ""
echo -e "${GREEN}🎉 Kubernetes cluster is ready for use!${NC}" 