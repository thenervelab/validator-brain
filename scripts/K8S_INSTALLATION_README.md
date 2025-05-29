# Kubernetes Installation Script for Ubuntu 24.04

This script provides a complete, automated installation of Kubernetes using kubeadm on Ubuntu 24.04, including all necessary dependencies, configurations, and storage provisioner setup.

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

3. **Initialize the cluster (after reboot):**
   ```bash
   # Run the cluster initialization script
   sudo bash scripts/init-k8s-cluster.sh
   ```

4. **Setup kubectl for regular user:**
   ```bash
   # Run as your regular user (not root)
   bash /tmp/setup-kubectl-user.sh
   ```

## 📦 What Gets Installed

### **System Components**
- ✅ **Container Runtime**: containerd with proper configuration
- ✅ **Kubernetes Components**: kubelet, kubeadm, kubectl (v1.31)
- ✅ **Networking**: Flannel CNI plugin
- ✅ **Storage**: local-path-provisioner (set as default)

### **System Configuration**
- ✅ **Swap**: Permanently disabled
- ✅ **Kernel Modules**: overlay, br_netfilter loaded
- ✅ **Sysctl**: IP forwarding and bridge netfilter enabled
- ✅ **Networking Tools**: iptables, arptables, ebtables (legacy versions)

### **Storage Setup**
- ✅ **Local Path Provisioner**: Installed and configured
- ✅ **Default Storage Class**: local-path set as default
- ✅ **Persistent Volumes**: Ready for dynamic provisioning

## 🔧 Manual Installation Steps

If you prefer to run the steps manually:

### 1. Install Kubernetes
```bash
sudo bash scripts/install-k8s-ubuntu.sh
sudo reboot
```

### 2. Initialize Cluster
```bash
sudo kubeadm init --config=/tmp/kubeadm-config.yaml
```

### 3. Setup kubectl
```bash
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
```

### 4. Install CNI Plugin
```bash
kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml
```

### 5. Install Storage Provisioner
```bash
# Install local-path provisioner
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml

# Set as default storage class
kubectl patch storageclass local-path -p '{"metadata": {"annotations": {"storageclass.kubernetes.io/is-default-class": "true"}}}'
```

### 6. Remove Control Plane Taint (Single Node)
```bash
kubectl taint nodes --all node-role.kubernetes.io/control-plane-
```

## 🧪 Verification

### Check Cluster Status
```bash
# Check nodes
kubectl get nodes -o wide

# Check all pods
kubectl get pods --all-namespaces

# Check storage classes
kubectl get storageclass
```

### Test Storage Provisioner
```bash
# Create test PVC
cat <<EOF | kubectl apply -f -
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

# Check PVC status
kubectl get pvc test-pvc

# Cleanup
kubectl delete pvc test-pvc
```

### Deploy Test Application
```bash
# Deploy nginx with persistent storage
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-test
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx-test
  template:
    metadata:
      labels:
        app: nginx-test
    spec:
      containers:
      - name: nginx
        image: nginx:latest
        ports:
        - containerPort: 80
        volumeMounts:
        - name: nginx-storage
          mountPath: /usr/share/nginx/html
      volumes:
      - name: nginx-storage
        persistentVolumeClaim:
          claimName: nginx-pvc
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: nginx-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: nginx-service
spec:
  selector:
    app: nginx-test
  ports:
  - port: 80
    targetPort: 80
    nodePort: 30080
  type: NodePort
EOF

# Check deployment
kubectl get pods,pvc,svc
```

## 🔧 Configuration Details

### **Kubeadm Configuration**
- **Pod Network CIDR**: 10.244.0.0/16 (Flannel)
- **Service Subnet**: 10.96.0.0/12
- **CRI Socket**: unix:///run/containerd/containerd.sock
- **Cgroup Driver**: systemd

### **Storage Configuration**
- **Provisioner**: rancher.io/local-path
- **Default Path**: /opt/local-path-provisioner
- **Access Modes**: ReadWriteOnce
- **Reclaim Policy**: Delete

### **Network Configuration**
- **CNI**: Flannel
- **Backend**: VXLAN
- **MTU**: Auto-detected
- **IPAM**: host-local

## 🚨 Troubleshooting

### Common Issues

**1. Pods stuck in Pending state**
```bash
# Check node status
kubectl describe nodes

# Check events
kubectl get events --sort-by='.lastTimestamp'

# Check storage
kubectl get storageclass
kubectl get pv
```

**2. Storage provisioner not working**
```bash
# Check provisioner pods
kubectl get pods -n local-path-storage

# Check logs
kubectl logs -n local-path-storage -l app=local-path-provisioner

# Verify storage class
kubectl describe storageclass local-path
```

**3. CNI issues**
```bash
# Check Flannel pods
kubectl get pods -n kube-flannel

# Check Flannel logs
kubectl logs -n kube-flannel -l app=flannel

# Restart Flannel
kubectl delete pods -n kube-flannel -l app=flannel
```

**4. Node not ready**
```bash
# Check kubelet status
sudo systemctl status kubelet

# Check kubelet logs
sudo journalctl -u kubelet -f

# Check containerd
sudo systemctl status containerd
```

### Reset Cluster
```bash
# Reset cluster (if needed)
sudo kubeadm reset
sudo rm -rf /etc/kubernetes/
sudo rm -rf ~/.kube/
sudo rm -rf /var/lib/etcd/

# Clean iptables
sudo iptables -F && sudo iptables -t nat -F && sudo iptables -t mangle -F && sudo iptables -X

# Restart containerd
sudo systemctl restart containerd
```

## 🔗 Multi-Node Setup

### Adding Worker Nodes

1. **Install Kubernetes on worker nodes:**
   ```bash
   sudo bash scripts/install-k8s-ubuntu.sh
   sudo reboot
   ```

2. **Join worker to cluster:**
   ```bash
   # Get join command from master node
   sudo kubeadm token create --print-join-command
   
   # Run on worker node
   sudo kubeadm join <master-ip>:6443 --token <token> --discovery-token-ca-cert-hash sha256:<hash>
   ```

3. **Verify from master:**
   ```bash
   kubectl get nodes
   ```

## 📊 Resource Requirements

### **Minimum Requirements**
- **CPU**: 2 cores
- **Memory**: 2GB RAM
- **Disk**: 20GB free space
- **Network**: Internet connectivity

### **Recommended for Production**
- **CPU**: 4+ cores
- **Memory**: 4GB+ RAM
- **Disk**: 50GB+ SSD
- **Network**: Dedicated network interface

## 🔒 Security Considerations

### **Default Security**
- ✅ Non-root container execution
- ✅ Network policies supported
- ✅ RBAC enabled by default
- ✅ TLS encryption for all components

### **Additional Security (Optional)**
```bash
# Enable audit logging
sudo mkdir -p /etc/kubernetes/audit
sudo cat > /etc/kubernetes/audit/policy.yaml << EOF
apiVersion: audit.k8s.io/v1
kind: Policy
rules:
- level: Metadata
EOF

# Add to kubeadm config
# --audit-log-path=/var/log/audit.log
# --audit-policy-file=/etc/kubernetes/audit/policy.yaml
```

## 📚 Useful Commands

### **Cluster Management**
```bash
# Check cluster info
kubectl cluster-info

# Get cluster events
kubectl get events --sort-by='.lastTimestamp'

# Check resource usage
kubectl top nodes
kubectl top pods --all-namespaces
```

### **Storage Management**
```bash
# List storage classes
kubectl get storageclass

# List persistent volumes
kubectl get pv

# List persistent volume claims
kubectl get pvc --all-namespaces
```

### **Debugging**
```bash
# Describe node
kubectl describe node <node-name>

# Check pod logs
kubectl logs <pod-name> -n <namespace>

# Execute into pod
kubectl exec -it <pod-name> -- /bin/bash

# Port forward
kubectl port-forward <pod-name> 8080:80
```

## 🎯 Next Steps

After successful installation:

1. **Deploy IPFS Service Validator:**
   ```bash
   # Setup namespace and deploy
   bash scripts/setup-namespace.sh
   bash k8s/deploy.sh
   ```

2. **Monitor cluster:**
   ```bash
   # Install metrics server (optional)
   kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
   ```

3. **Setup ingress (optional):**
   ```bash
   # Install nginx ingress
   kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.2/deploy/static/provider/cloud/deploy.yaml
   ```

---

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Kubernetes logs: `sudo journalctl -u kubelet -f`
3. Check system resources: `kubectl top nodes`
4. Verify storage: `kubectl get storageclass`

The installation provides a production-ready Kubernetes cluster with persistent storage support, perfect for deploying the IPFS Service Validator and other containerized applications. 