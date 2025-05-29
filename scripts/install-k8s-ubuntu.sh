#!/bin/bash

# Kubernetes Installation Script for Ubuntu 24.04
# This script installs Kubernetes with kubeadm, including all dependencies
# Run with: sudo bash install-k8s-ubuntu.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
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

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   error "This script must be run as root (use sudo)"
fi

# Check Ubuntu version
if ! grep -q "24.04" /etc/os-release; then
    warn "This script is designed for Ubuntu 24.04. Proceeding anyway..."
fi

log "🚀 Starting Kubernetes installation on Ubuntu 24.04"

# Update system
log "📦 Updating system packages..."
apt-get update -y
apt-get upgrade -y

# Install required packages
log "📦 Installing required packages..."
apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    software-properties-common \
    wget \
    gpg

# Disable swap permanently
log "💾 Disabling swap..."
swapoff -a
sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab

# Configure kernel modules
log "🔧 Configuring kernel modules..."
cat <<EOF | tee /etc/modules-load.d/k8s.conf
overlay
br_netfilter
EOF

modprobe overlay
modprobe br_netfilter

# Configure sysctl parameters
log "🔧 Configuring sysctl parameters..."
cat <<EOF | tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF

sysctl --system

# Install containerd
log "🐳 Installing containerd..."
apt-get update -y
apt-get install -y containerd

# Configure containerd
log "🔧 Configuring containerd..."
mkdir -p /etc/containerd
containerd config default | tee /etc/containerd/config.toml

# Enable SystemdCgroup in containerd config
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml

# Restart and enable containerd
systemctl restart containerd
systemctl enable containerd

# Add Kubernetes APT repository
log "📦 Adding Kubernetes APT repository..."
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /' | tee /etc/apt/sources.list.d/kubernetes.list

# Update package index
apt-get update -y

# Install Kubernetes components
log "☸️ Installing Kubernetes components (kubeadm, kubelet, kubectl)..."
apt-get install -y kubelet kubeadm kubectl
apt-mark hold kubelet kubeadm kubectl

# Enable and start kubelet
systemctl enable kubelet

# Configure crictl
log "🔧 Configuring crictl..."
cat <<EOF | tee /etc/crictl.yaml
runtime-endpoint: unix:///run/containerd/containerd.sock
image-endpoint: unix:///run/containerd/containerd.sock
timeout: 2
debug: false
pull-image-on-create: false
EOF

# Install additional useful tools
log "🛠️ Installing additional tools..."
apt-get install -y \
    bash-completion \
    vim \
    htop \
    net-tools \
    jq

# Setup kubectl bash completion
log "🔧 Setting up kubectl bash completion..."
kubectl completion bash | tee /etc/bash_completion.d/kubectl > /dev/null
echo 'alias k=kubectl' >> /etc/bash.bashrc
echo 'complete -o default -F __start_kubectl k' >> /etc/bash.bashrc

# Create kubeadm init script
log "📝 Creating kubeadm initialization script..."
cat <<'EOF' > /root/init-cluster.sh
#!/bin/bash

# Kubernetes Cluster Initialization Script
# Run this script to initialize the cluster after installation

set -e

echo "🚀 Initializing Kubernetes cluster..."

# Initialize the cluster
kubeadm init --pod-network-cidr=10.244.0.0/16 --cri-socket=unix:///run/containerd/containerd.sock

# Setup kubectl for root user
mkdir -p /root/.kube
cp -i /etc/kubernetes/admin.conf /root/.kube/config
chown root:root /root/.kube/config

echo "✅ Cluster initialized successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Install a CNI plugin (e.g., Flannel):"
echo "   kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml"
echo ""
echo "2. To join worker nodes, run the kubeadm join command that was displayed above"
echo ""
echo "3. To setup kubectl for a regular user:"
echo "   mkdir -p \$HOME/.kube"
echo "   sudo cp -i /etc/kubernetes/admin.conf \$HOME/.kube/config"
echo "   sudo chown \$(id -u):\$(id -g) \$HOME/.kube/config"
echo ""
echo "4. To remove the taint from master node (single-node cluster):"
echo "   kubectl taint nodes --all node-role.kubernetes.io/control-plane-"
EOF

chmod +x /root/init-cluster.sh

# Create worker node join script template
log "📝 Creating worker node join script template..."
cat <<'EOF' > /root/join-worker.sh
#!/bin/bash

# Worker Node Join Script Template
# Replace the kubeadm join command below with the actual command from your master node

set -e

echo "🔗 Joining worker node to cluster..."

# Replace this with the actual join command from your master node
# Example:
# kubeadm join <master-ip>:6443 --token <token> --discovery-token-ca-cert-hash sha256:<hash>

echo "⚠️  Please replace the kubeadm join command in this script with the actual command from your master node"
echo "The join command is displayed when you run 'kubeadm init' on the master node"
EOF

chmod +x /root/join-worker.sh

# Create useful aliases and functions
log "🔧 Creating useful aliases and functions..."
cat <<'EOF' >> /root/.bashrc

# Kubernetes aliases
alias k='kubectl'
alias kgp='kubectl get pods'
alias kgs='kubectl get services'
alias kgn='kubectl get nodes'
alias kd='kubectl describe'
alias kl='kubectl logs'
alias ke='kubectl exec -it'

# Useful functions
kns() {
    kubectl config set-context --current --namespace=$1
}

kpods() {
    kubectl get pods -o wide --all-namespaces
}
EOF

# Verify installation
log "🔍 Verifying installation..."
echo "Kubernetes version:"
kubeadm version
echo ""
echo "Kubelet version:"
kubelet --version
echo ""
echo "Kubectl version:"
kubectl version --client
echo ""
echo "Containerd version:"
containerd --version

log "✅ Kubernetes installation completed successfully!"
echo ""
echo -e "${BLUE}📋 Next Steps:${NC}"
echo "1. Reboot the system to ensure all changes take effect:"
echo "   sudo reboot"
echo ""
echo "2. After reboot, initialize the cluster (on master node):"
echo "   sudo /root/init-cluster.sh"
echo ""
echo "3. Install a CNI plugin (after cluster init):"
echo "   kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml"
echo ""
echo "4. For worker nodes, use the join command displayed after cluster init"
echo ""
echo -e "${YELLOW}📁 Useful files created:${NC}"
echo "- /root/init-cluster.sh - Initialize the cluster"
echo "- /root/join-worker.sh - Template for joining worker nodes"
echo ""
echo -e "${GREEN}🎉 Installation complete! Please reboot and then initialize your cluster.${NC}" 