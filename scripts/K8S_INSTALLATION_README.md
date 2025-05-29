# Kubernetes Installation Script for Ubuntu 24.04

This script provides a complete, automated installation of Kubernetes using kubeadm on Ubuntu 24.04, including all necessary dependencies and configurations.

## 🚀 Quick Start

### Prerequisites
- Ubuntu 24.04 LTS server
- Root or sudo access
- At least 2GB RAM and 2 CPU cores
- Network connectivity

### Installation

1. **Download and run the installation script:**
   ```bash
   # Download the script
   wget https://raw.githubusercontent.com/your-repo/ipfs-service-validator/main/scripts/install-k8s-ubuntu.sh
   
   # Or if you have the repo cloned:
   cd ipfs-service-validator
   
   # Make executable and run
   sudo bash scripts/install-k8s-ubuntu.sh
   ```

2. **Reboot the system:**
   ```bash
   sudo reboot
   ```

3. **Initialize the cluster (Master Node only):**
   ```bash
   sudo /root/init-cluster.sh
   ```

4. **Install a CNI plugin (e.g., Flannel):**
   ```bash
   kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml
   ```

## 📋 What the Script Does

### System Configuration
- ✅ Updates system packages
- ✅ Disables swap permanently
- ✅ Configures kernel modules (overlay, br_netfilter)
- ✅ Sets up sysctl parameters for Kubernetes networking
- ✅ Installs required dependencies

### Container Runtime
- ✅ Installs and configures containerd
- ✅ Enables SystemdCgroup for proper cgroup management
- ✅ Configures crictl for container debugging

### Kubernetes Components
- ✅ Adds official Kubernetes APT repository
- ✅ Installs kubeadm, kubelet, and kubectl (v1.31)
- ✅ Holds packages to prevent automatic updates
- ✅ Enables kubelet service

### Additional Tools & Configuration
- ✅ Installs useful tools (bash-completion, vim, htop, jq)
- ✅ Sets up kubectl bash completion and aliases
- ✅ Creates helper scripts for cluster initialization

## 🛠️ Post-Installation Scripts

The installation creates several helper scripts in `/root/`:

### `/root/init-cluster.sh`
Initializes the Kubernetes cluster on the master node:
```bash
sudo /root/init-cluster.sh
```

### `/root/join-worker.sh`
Template for joining worker nodes (edit with actual join command):
```bash
# Edit the script with the actual join command from master
sudo nano /root/join-worker.sh
sudo /root/join-worker.sh
```

## 🔧 Useful Aliases & Functions

The script adds these aliases to your shell:
```bash
alias k='kubectl'
alias kgp='kubectl get pods'
alias kgs='kubectl get services'
alias kgn='kubectl get nodes'
alias kd='kubectl describe'
alias kl='kubectl logs'
alias ke='kubectl exec -it'

# Functions
kns <namespace>    # Switch to namespace
kpods             # Get all pods across namespaces
```

## 🌐 Setting Up a Multi-Node Cluster

### Master Node Setup
1. Run the installation script
2. Reboot
3. Initialize cluster: `sudo /root/init-cluster.sh`
4. Install CNI plugin
5. Save the join command displayed after initialization

### Worker Node Setup
1. Run the installation script on each worker node
2. Reboot
3. Use the join command from master node:
   ```bash
   sudo kubeadm join <master-ip>:6443 --token <token> --discovery-token-ca-cert-hash sha256:<hash>
   ```

## 🔧 Single-Node Cluster (Development)

For a single-node cluster, remove the taint from the master node:
```bash
kubectl taint nodes --all node-role.kubernetes.io/control-plane-
```

## 📊 Verification Commands

After installation, verify your cluster:
```bash
# Check node status
kubectl get nodes

# Check system pods
kubectl get pods -n kube-system

# Check cluster info
kubectl cluster-info

# Check component status
kubectl get componentstatuses
```

## 🐛 Troubleshooting

### Common Issues

1. **Swap not disabled:**
   ```bash
   sudo swapoff -a
   sudo sed -i '/ swap / s/^\(.*\)$/#\1/g' /etc/fstab
   ```

2. **Containerd not running:**
   ```bash
   sudo systemctl restart containerd
   sudo systemctl enable containerd
   ```

3. **Kubelet not starting:**
   ```bash
   sudo systemctl status kubelet
   sudo journalctl -xeu kubelet
   ```

4. **CNI plugin issues:**
   ```bash
   # Reinstall Flannel
   kubectl delete -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml
   kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml
   ```

### Log Locations
- Kubelet logs: `journalctl -u kubelet`
- Containerd logs: `journalctl -u containerd`
- Pod logs: `kubectl logs <pod-name> -n <namespace>`

## 🔄 Uninstalling Kubernetes

If you need to remove Kubernetes:
```bash
# Reset kubeadm
sudo kubeadm reset -f

# Remove packages
sudo apt-get purge -y kubeadm kubectl kubelet kubernetes-cni kube*
sudo apt-get autoremove -y

# Remove configuration
sudo rm -rf ~/.kube
sudo rm -rf /etc/kubernetes
sudo rm -rf /var/lib/kubelet
sudo rm -rf /var/lib/etcd

# Re-enable swap if needed
sudo sed -i '/^#.*swap/s/^#//' /etc/fstab
```

## 📚 Additional Resources

- [Official Kubernetes Documentation](https://kubernetes.io/docs/)
- [kubeadm Documentation](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/)
- [Container Runtime Documentation](https://kubernetes.io/docs/setup/production-environment/container-runtimes/)
- [CNI Plugins](https://kubernetes.io/docs/concepts/cluster-administration/addons/)

## 🆘 Support

If you encounter issues:
1. Check the troubleshooting section above
2. Review system logs: `journalctl -u kubelet` and `journalctl -u containerd`
3. Verify network connectivity and DNS resolution
4. Ensure system meets minimum requirements

## 📝 Version Information

- **Kubernetes Version:** v1.31 (stable)
- **Container Runtime:** containerd
- **CNI Plugin:** Flannel (recommended)
- **Supported OS:** Ubuntu 24.04 LTS

---

**Note:** This script is designed for Ubuntu 24.04. For other Ubuntu versions, you may need to adjust the repository URLs and package versions. 