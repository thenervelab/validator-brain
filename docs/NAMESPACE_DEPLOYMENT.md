# IPFS Service Validator - Namespace Deployment Guide

This guide covers deploying the IPFS Service Validator to a dedicated Kubernetes namespace for better organization, isolation, and management.

## 🏗️ Namespace Benefits

### **Isolation & Organization**
- **Resource Isolation**: All components are contained within the `ipfs-validator` namespace
- **Clean Separation**: No conflicts with other applications in the cluster
- **Easy Management**: Single namespace for all validator components

### **Security & Access Control**
- **RBAC Support**: Fine-grained access control per namespace
- **Network Policies**: Isolate network traffic within namespace
- **Resource Quotas**: Limit resource usage per namespace

### **Operational Benefits**
- **Easy Cleanup**: Delete entire namespace to remove all resources
- **Monitoring**: Namespace-scoped metrics and logging
- **Scaling**: Independent scaling without affecting other workloads

## 🚀 Quick Start

### **1. Setup Namespace Support**
```bash
# Run the namespace setup script
bash scripts/setup-namespace.sh
```

This script will:
- ✅ Create namespace configuration
- ✅ Add namespace to all YAML files
- ✅ Update deployment scripts
- ✅ Update monitoring scripts
- ✅ Create PostgreSQL PVC with namespace

### **2. Deploy to Namespace**
```bash
# Deploy all components to the ipfs-validator namespace
bash k8s/deploy.sh
```

### **3. Verify Deployment**
```bash
# Check all resources in the namespace
kubectl get all -n ipfs-validator

# Check specific components
kubectl get pods -n ipfs-validator
kubectl get services -n ipfs-validator
kubectl get pvc -n ipfs-validator
```

## 📋 Namespace Configuration

### **Namespace Definition**
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: ipfs-validator
  labels:
    name: ipfs-validator
    app.kubernetes.io/name: ipfs-service-validator
    app.kubernetes.io/component: infrastructure
    app.kubernetes.io/part-of: substrate-network
    app.kubernetes.io/managed-by: kubectl
  annotations:
    description: "IPFS Service Validator - Blockchain storage validation and health monitoring"
    contact: "admin@hippius.network"
    environment: "production"
```

### **Components in Namespace**
- **PostgreSQL**: High-scale database (1000 connections)
- **RabbitMQ**: Message queue for task processing
- **IPFS**: Distributed storage (StatefulSet with auto-scaling)
- **Consumers**: 9 different consumer deployments
- **Epoch Orchestrator**: Main validation orchestrator

## 🔧 Management Commands

### **Namespace Operations**
```bash
# Switch kubectl context to namespace
kubectl config set-context --current --namespace=ipfs-validator

# View all resources in namespace
kubectl get all -n ipfs-validator

# Describe namespace
kubectl describe namespace ipfs-validator

# Get resource usage
kubectl top pods -n ipfs-validator
kubectl top nodes
```

### **Application Management**
```bash
# Check logs
kubectl logs -l app=epoch-orchestrator -n ipfs-validator --tail=50
kubectl logs -l app=postgres -n ipfs-validator --tail=20

# Scale components
kubectl scale statefulset ipfs --replicas=3 -n ipfs-validator
kubectl scale deployment user-profile-consumer --replicas=2 -n ipfs-validator

# Restart deployments
kubectl rollout restart deployment/epoch-orchestrator -n ipfs-validator
kubectl rollout restart deployment/postgres -n ipfs-validator
```

### **Database Management**
```bash
# Monitor PostgreSQL connections (namespace-aware)
bash scripts/monitor-postgres.sh

# Check database connection status
bash scripts/fix-db-connections.sh check

# Apply high-scale database configuration
bash scripts/fix-db-connections.sh
```

## 📊 Monitoring & Debugging

### **Health Checks**
```bash
# Check pod status
kubectl get pods -n ipfs-validator -o wide

# Check service endpoints
kubectl get endpoints -n ipfs-validator

# Check persistent volumes
kubectl get pvc -n ipfs-validator
kubectl get pv
```

### **Resource Usage**
```bash
# Check resource requests/limits
kubectl describe pods -n ipfs-validator | grep -A 5 "Requests\|Limits"

# Monitor HPA status
kubectl get hpa -n ipfs-validator
kubectl describe hpa ipfs-hpa -n ipfs-validator
```

### **Troubleshooting**
```bash
# Check events in namespace
kubectl get events -n ipfs-validator --sort-by='.lastTimestamp'

# Debug failing pods
kubectl describe pod <pod-name> -n ipfs-validator
kubectl logs <pod-name> -n ipfs-validator --previous

# Check network connectivity
kubectl exec -it <pod-name> -n ipfs-validator -- ping postgres-service
kubectl exec -it <pod-name> -n ipfs-validator -- nslookup ipfs-service
```

## 🔒 Security & Access Control

### **RBAC Setup (Optional)**
```yaml
# Create service account for the namespace
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ipfs-validator-sa
  namespace: ipfs-validator
---
# Create role with necessary permissions
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: ipfs-validator
  name: ipfs-validator-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps", "secrets"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["apps"]
  resources: ["deployments", "statefulsets"]
  verbs: ["get", "list", "watch", "update", "patch"]
---
# Bind role to service account
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ipfs-validator-binding
  namespace: ipfs-validator
subjects:
- kind: ServiceAccount
  name: ipfs-validator-sa
  namespace: ipfs-validator
roleRef:
  kind: Role
  name: ipfs-validator-role
  apiGroup: rbac.authorization.k8s.io
```

### **Network Policies (Optional)**
```yaml
# Restrict network access to namespace
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ipfs-validator-netpol
  namespace: ipfs-validator
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: ipfs-validator
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: ipfs-validator
  - to: []  # Allow external egress for blockchain/IPFS
    ports:
    - protocol: TCP
      port: 443
    - protocol: TCP
      port: 80
```

## 🧹 Cleanup & Maintenance

### **Complete Cleanup**
```bash
# Delete entire namespace (removes all resources)
kubectl delete namespace ipfs-validator

# This will delete:
# - All pods, services, deployments
# - ConfigMaps and secrets
# - PersistentVolumeClaims
# - StatefulSets and HPAs
```

### **Selective Cleanup**
```bash
# Delete specific components
kubectl delete deployment epoch-orchestrator -n ipfs-validator
kubectl delete statefulset ipfs -n ipfs-validator
kubectl delete pvc postgres-pvc -n ipfs-validator
```

### **Backup & Migration**
```bash
# Export namespace configuration
kubectl get all -n ipfs-validator -o yaml > ipfs-validator-backup.yaml

# Export specific resources
kubectl get configmap ipfs-validator-config -n ipfs-validator -o yaml > config-backup.yaml
kubectl get pvc postgres-pvc -n ipfs-validator -o yaml > pvc-backup.yaml
```

## 📈 Scaling & Performance

### **Horizontal Scaling**
```bash
# Scale IPFS nodes
kubectl scale statefulset ipfs --replicas=5 -n ipfs-validator

# Scale consumer deployments
kubectl scale deployment user-profile-consumer --replicas=3 -n ipfs-validator
kubectl scale deployment miner-health-consumer --replicas=2 -n ipfs-validator
```

### **Resource Optimization**
```bash
# Check resource usage
kubectl top pods -n ipfs-validator
kubectl describe nodes

# Update resource limits
kubectl patch deployment postgres -n ipfs-validator -p '{"spec":{"template":{"spec":{"containers":[{"name":"postgres","resources":{"limits":{"memory":"8Gi","cpu":"4000m"}}}]}}}}'
```

## 🔄 Migration from Default Namespace

If you have existing deployments in the default namespace:

### **1. Backup Current State**
```bash
kubectl get all -o yaml > current-deployment-backup.yaml
```

### **2. Setup Namespace**
```bash
bash scripts/setup-namespace.sh
```

### **3. Deploy to New Namespace**
```bash
bash k8s/deploy.sh
```

### **4. Migrate Data (if needed)**
```bash
# Export PostgreSQL data
kubectl exec -it deployment/postgres -- pg_dump -U user substrate_fetcher > db-backup.sql

# Import to new namespace
kubectl exec -i deployment/postgres -n ipfs-validator -- psql -U user substrate_fetcher < db-backup.sql
```

### **5. Cleanup Old Deployment**
```bash
# Delete old resources from default namespace
kubectl delete deployment postgres rabbitmq ipfs epoch-orchestrator
kubectl delete service postgres-service rabbitmq-service ipfs-service
kubectl delete configmap ipfs-validator-config
```

## 💡 Best Practices

### **Resource Management**
- Set appropriate resource requests and limits
- Use HPA for auto-scaling IPFS nodes
- Monitor resource usage regularly
- Set up resource quotas for the namespace

### **Security**
- Use RBAC for access control
- Implement network policies if needed
- Regularly update container images
- Use secrets for sensitive configuration

### **Monitoring**
- Set up namespace-scoped monitoring
- Use the provided monitoring scripts
- Monitor PostgreSQL connection usage
- Track IPFS performance metrics

### **Maintenance**
- Regular backups of PostgreSQL data
- Monitor disk usage for PVCs
- Keep deployment scripts updated
- Document any custom configurations

---

## 🆘 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review logs: `kubectl logs -n ipfs-validator`
3. Check events: `kubectl get events -n ipfs-validator`
4. Use monitoring scripts: `bash scripts/monitor-postgres.sh`

The namespace-based deployment provides better isolation, easier management, and improved security for your IPFS Service Validator deployment. 