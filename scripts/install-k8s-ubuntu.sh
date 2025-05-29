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
if ! grep -q "Ubuntu 24.04" /etc/os-release; then
    warn "This script is designed for Ubuntu 24.04. Proceeding anyway..."
fi

log "🚀 Starting Kubernetes installation on Ubuntu 24.04"

# Update system packages
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
cat > /etc/modules-load.d/k8s.conf << EOF
overlay
br_netfilter
EOF

modprobe overlay
modprobe br_netfilter

# Configure sysctl parameters
log "🔧 Configuring sysctl parameters..."
cat > /etc/sysctl.d/k8s.conf << EOF
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
containerd config default > /etc/containerd/config.toml

# Enable SystemdCgroup in containerd
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml

# Restart and enable containerd
systemctl restart containerd
systemctl enable containerd

# Add Kubernetes APT repository
log "📦 Adding Kubernetes APT repository..."
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /' > /etc/apt/sources.list.d/kubernetes.list

# Install Kubernetes components
log "☸️  Installing Kubernetes components..."
apt-get update -y
apt-get install -y kubelet kubeadm kubectl

# Hold Kubernetes packages to prevent automatic updates
apt-mark hold kubelet kubeadm kubectl

# Enable kubelet service
systemctl enable kubelet

# Configure crictl
log "🔧 Configuring crictl..."
cat > /etc/crictl.yaml << EOF
runtime-endpoint: unix:///run/containerd/containerd.sock
image-endpoint: unix:///run/containerd/containerd.sock
timeout: 2
debug: false
pull-image-on-create: false
EOF

# Install additional networking tools
log "🌐 Installing networking tools..."
apt-get install -y \
    iptables \
    arptables \
    ebtables

# Switch to legacy versions for compatibility
update-alternatives --set iptables /usr/sbin/iptables-legacy
update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy
update-alternatives --set arptables /usr/sbin/arptables-legacy
update-alternatives --set ebtables /usr/sbin/ebtables-legacy

# Create kubeadm configuration
log "📝 Creating kubeadm configuration..."
cat > /tmp/kubeadm-config.yaml << EOF
apiVersion: kubeadm.k8s.io/v1beta3
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: $(hostname -I | awk '{print $1}')
  bindPort: 6443
nodeRegistration:
  criSocket: unix:///run/containerd/containerd.sock
  kubeletExtraArgs:
    cgroup-driver: systemd
---
apiVersion: kubeadm.k8s.io/v1beta3
kind: ClusterConfiguration
kubernetesVersion: v1.31.0
controlPlaneEndpoint: $(hostname -I | awk '{print $1}'):6443
networking:
  serviceSubnet: 10.96.0.0/12
  podSubnet: 10.244.0.0/16
  dnsDomain: cluster.local
apiServer:
  bindPort: 6443
controllerManager: {}
scheduler: {}
etcd:
  local:
    dataDir: /var/lib/etcd
---
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
cgroupDriver: systemd
EOF

log "✅ Kubernetes installation completed successfully!"

echo ""
echo -e "${BLUE}🎉 Installation Summary:${NC}"
echo "  ✅ System packages updated"
echo "  ✅ Swap disabled permanently"
echo "  ✅ Kernel modules configured"
echo "  ✅ Containerd installed and configured"
echo "  ✅ Kubernetes components installed (kubelet, kubeadm, kubectl)"
echo "  ✅ Networking tools configured"
echo "  ✅ Kubeadm configuration created"

echo ""
echo -e "${YELLOW}🔄 Next Steps:${NC}"
echo ""
echo -e "${BLUE}1. Reboot the system:${NC}"
echo "   sudo reboot"
echo ""
echo -e "${BLUE}2. Initialize the cluster (after reboot):${NC}"
echo "   sudo kubeadm init --config=/tmp/kubeadm-config.yaml"
echo ""
echo -e "${BLUE}3. Configure kubectl for regular user:${NC}"
echo "   mkdir -p \$HOME/.kube"
echo "   sudo cp -i /etc/kubernetes/admin.conf \$HOME/.kube/config"
echo "   sudo chown \$(id -u):\$(id -g) \$HOME/.kube/config"
echo ""
echo -e "${BLUE}4. Install CNI plugin (Flannel):${NC}"
echo "   kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml"
echo ""
echo -e "${BLUE}5. Install local-path storage provisioner:${NC}"
echo "   kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml"
echo ""
echo -e "${BLUE}6. Set local-path as default storage class:${NC}"
echo "   kubectl patch storageclass local-path -p '{\"metadata\": {\"annotations\": {\"storageclass.kubernetes.io/is-default-class\": \"true\"}}}'"
echo ""
echo -e "${BLUE}7. Remove taint from control plane (for single-node setup):${NC}"
echo "   kubectl taint nodes --all node-role.kubernetes.io/control-plane-"
echo ""
echo -e "${BLUE}8. Verify installation:${NC}"
echo "   kubectl get nodes"
echo "   kubectl get pods --all-namespaces"
echo "   kubectl get storageclass"

echo ""
echo -e "${GREEN}🎉 Installation complete! Please reboot and then initialize your cluster.${NC}" 