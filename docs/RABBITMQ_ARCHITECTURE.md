# RabbitMQ Architecture Documentation

## Overview

The IPFS Service Validator uses a comprehensive RabbitMQ-based producer/consumer architecture for processing blockchain data, health monitoring, and file management. This document provides a complete reference for all RabbitMQ components in the system.

## Architecture Pattern

All RabbitMQ components follow a consistent **Producer/Consumer Pattern**:

```
Blockchain/Data Source → Producer → RabbitMQ Queue → Consumer → Database
```

### Key Principles

1. **Producers**: One-time execution, fetch data, clear tables, queue messages
2. **Consumers**: Continuous execution, process messages, update databases
3. **Queues**: Durable, persistent messages, reliable delivery
4. **Separation**: Different queues for different data types and purposes

---

## 🔄 Core Producer/Consumer Pairs

### 1. Registration System ⭐

**Purpose**: Manage node registration data from substrate chain

#### Producer: `registration_processor.py`
- **Queue**: `registration_latest`
- **Trigger**: On-demand (start of epoch)
- **Process**:
  1. Clears `registration` table (`DELETE FROM registration`)
  2. Fetches `registration.coldkeyNodeRegistration` storage
  3. Fetches `registration.nodeRegistration` storage  
  4. Sends individual registration records to queue
- **Message Format**:
  ```json
  {
    "node_id": "12D3KooW...",
    "ipfs_peer_id": "12D3KooW...",
    "node_type": "StorageMiner",
    "owner_account": "5ComtN...",
    "registered_at": 657495,
    "status": "active",
    "source": "coldkey|node"
  }
  ```

#### Consumer: `registration_consumer.py`
- **Deployment**: `registration-consumer` (Kubernetes)
- **Process**: Upsert registration data with conflict resolution
- **Table**: `registration`

---

### 2. Node Metrics System

**Purpose**: Process node performance metrics

#### Producer: `node_metrics_processor.py`
- **Queue**: `node_metrics_latest`
- **Trigger**: On-demand
- **Process**:
  1. Clears `node_metrics` table
  2. Fetches node metrics from substrate
  3. Queues individual metric records

#### Consumer: `node_metrics_consumer.py`
- **Deployment**: `node-metrics-consumer` (Kubernetes)
- **Table**: `node_metrics`

---

### 3. User Profile System

**Purpose**: Manage user file storage profiles

#### Producer: `user_profile_processor.py`
- **Queue**: `user_profile_latest`
- **Process**: Fetches and queues user profile data

#### Consumer: `user_profile_consumer.py`
- **Deployment**: `user-profile-consumer` (Kubernetes)
- **Table**: `user_profile`

---

### 4. Miner Profile System

**Purpose**: Manage miner file storage profiles

#### Producer: `miner_profile_processor.py`
- **Queue**: `miner_profile_latest`
- **Process**: Fetches and queues miner profile data

#### Consumer: `miner_profile_consumer.py`
- **Deployment**: `miner-profile-consumer` (Kubernetes)
- **Table**: `miner_profile`

---

### 5. Pinning Request System

**Purpose**: Handle file pinning requests

#### Producer: `pinning_request_processor.py`
- **Queue**: `pinning_request_latest`
- **Process**: Fetches and queues pinning requests

#### Consumer: `pinning_request_consumer.py`
- **Deployment**: `pinning-request-consumer` (Kubernetes)
- **Table**: `storage_requests`

---

### 6. Pinning File System

**Purpose**: Process individual file pinning operations

#### Producer: `pinning_file_processor.py`
- **Queue**: `pinning_file_latest`
- **Process**: Queues file pinning tasks

#### Consumer: `pinning_file_consumer.py`
- **Deployment**: `pinning-file-consumer` (Kubernetes)
- **Process**: Performs IPFS pinning operations

---

### 7. Profile Reconstruction Systems

**Purpose**: Rebuild profile data from blockchain state

#### Miner Profile Reconstruction
- **Producer**: `miner_profile_reconstruction_processor.py`
- **Consumer**: `miner_profile_reconstruction_consumer.py`
- **Deployment**: `miner-profile-reconstruction-consumer`
- **Queue**: `miner_profile_reconstruction`

#### User Profile Reconstruction  
- **Producer**: `user_profile_reconstruction_processor.py`
- **Consumer**: `user_profile_reconstruction_consumer.py`
- **Deployment**: `user-profile-reconstruction-consumer`
- **Queue**: `user_profile_reconstruction`

---

## 🏥 Health Monitoring Systems

### 1. Continuous Health Monitoring (Real-time)

#### Consumer: `miner_health_consumer_with_reassignment.py`
- **Deployment**: `miner-health-consumer-with-reassignment` (Kubernetes)
- **Queue**: `miner_health_check`
- **Purpose**: Continuous health monitoring with automatic reassignment
- **Features**:
  - Checks up to 10 files per miner
  - Records failures in `file_failures` table
  - Updates `miner_availability` scores
  - **Automatic replica reassignment**
  - Real-time failure recovery

**Configuration**:
```yaml
MAX_FILES_PER_MINER: "10"
ENABLE_AUTO_REASSIGNMENT: "true"
MIN_REPLICAS: "3"
MAX_REPLICAS: "5"
MIN_AVAILABILITY_SCORE: "0.7"
MAX_CONSECUTIVE_FAILURES: "3"
FAILURE_WINDOW_HOURS: "24"
REASSIGNMENT_COOLDOWN_HOURS: "6"
```

### 2. Epoch Health Assessment (Comprehensive)

#### Producer: `epoch_health_processor.py`
- **Queue**: `epoch_health_check`
- **Trigger**: Start of validator epoch
- **Purpose**: Comprehensive epoch-based health assessment
- **Process**:
  1. **Clears `miner_epoch_health` table** for current epoch
  2. Fetches ALL active miners from `registration`
  3. Gets ALL files assigned to each miner
  4. Queues comprehensive health check messages

#### Consumer: `epoch_health_consumer.py`
- **Deployment**: `epoch-health-consumer` (Kubernetes)
- **Purpose**: Process comprehensive epoch health checks
- **Features**:
  - Checks up to 50 files per miner
  - Records results in `miner_epoch_health` table only
  - **No automatic reassignment** (assessment only)
  - Comprehensive epoch evaluation

**Configuration**:
```yaml
EPOCH_MAX_FILES_PER_MINER: "50"
```

**Message Format**:
```json
{
  "node_id": "12D3KooW...",
  "ipfs_peer_id": "12D3KooW...",
  "epoch": 12345,
  "files": ["QmABC...", "QmDEF...", ...],
  "total_files": 150,
  "check_type": "epoch_comprehensive",
  "timestamp": "2024-01-01T00:00:00Z",
  "block_number": 1234567
}
```

---

## 📊 Database Tables Updated

### Core Data Tables
- **`registration`**: Node registration data
- **`node_metrics`**: Node performance metrics  
- **`user_profile`**: User file storage profiles
- **`miner_profile`**: Miner file storage profiles
- **`storage_requests`**: File pinning requests

### Health Monitoring Tables
- **`miner_epoch_health`**: Epoch-based health statistics
- **`file_failures`**: Individual file failure tracking
- **`miner_availability`**: Miner availability scores and status

---

## 🚀 Deployment Status

### Active Kubernetes Deployments

```bash
kubectl get deployments | grep consumer
```

| Consumer | Status | Purpose |
|----------|--------|---------|
| `user-profile-consumer` | ✅ Running | User profiles |
| `pinning-request-consumer` | ✅ Running | Pinning requests |
| `node-metrics-consumer` | ✅ Running | Node metrics |
| `registration-consumer` | ✅ Running | Registration data |
| `miner-profile-reconstruction-consumer` | ✅ Running | Miner profile rebuild |
| `user-profile-reconstruction-consumer` | ✅ Running | User profile rebuild |
| `pinning-file-consumer` | ✅ Running | File pinning ops |
| `miner-health-consumer-with-reassignment` | ✅ Running | Real-time health + reassignment |
| `epoch-health-consumer` | ✅ Running | Epoch health assessment |

---

## 🔧 Running Producers

### On-Demand Execution

Producers are run as one-time jobs when needed:

```bash
# Registration data (start of epoch)
kubectl run registration-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/registration_processor.py

# Epoch health assessment (start of validator epoch)  
kubectl run epoch-health-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/epoch_health_processor.py

# Node metrics
kubectl run node-metrics-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/node_metrics_processor.py

# User profiles
kubectl run user-profile-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/user_profile_processor.py

# Miner profiles  
kubectl run miner-profile-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/miner_profile_processor.py
```

### Job-based Execution

```bash
# Create persistent jobs
kubectl create job registration-sync --image=ipfs-service-validator:latest -- python rabbitmq/registration_processor.py
kubectl create job epoch-health-check --image=ipfs-service-validator:latest -- python rabbitmq/epoch_health_processor.py
```

---

## 📋 Queue Management

### Queue Names

| Queue | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `registration_latest` | registration_processor | registration_consumer | Node registration |
| `node_metrics_latest` | node_metrics_processor | node_metrics_consumer | Performance metrics |
| `user_profile_latest` | user_profile_processor | user_profile_consumer | User profiles |
| `miner_profile_latest` | miner_profile_processor | miner_profile_consumer | Miner profiles |
| `pinning_request_latest` | pinning_request_processor | pinning_request_consumer | Pinning requests |
| `pinning_file_latest` | pinning_file_processor | pinning_file_consumer | File operations |
| `miner_profile_reconstruction` | miner_profile_reconstruction_processor | miner_profile_reconstruction_consumer | Profile rebuild |
| `user_profile_reconstruction` | user_profile_reconstruction_processor | user_profile_reconstruction_consumer | Profile rebuild |
| `miner_health_check` | - | miner_health_consumer_with_reassignment | Real-time health |
| `epoch_health_check` | epoch_health_processor | epoch_health_consumer | Epoch assessment |

### Queue Inspection

```bash
# Access RabbitMQ Management UI
kubectl port-forward service/rabbitmq-management-nodeport 15672:15672
# Visit: http://localhost:15672 (admin/admin)

# Check queue status
kubectl exec -it rabbitmq-xxx -- rabbitmqctl list_queues
```

---

## 🔍 Monitoring & Debugging

### Check Consumer Status

```bash
# All consumers
kubectl get pods | grep consumer

# Specific consumer logs
kubectl logs -f epoch-health-consumer-xxx
kubectl logs -f registration-consumer-xxx
kubectl logs -f miner-health-consumer-with-reassignment-xxx
```

### Database Queries

```bash
# Check registration data
kubectl exec -it postgres-client -- psql "postgres://user:password@postgres-service:5432/substrate_fetcher" -c "SELECT COUNT(*) FROM registration;"

# Check epoch health data
kubectl exec -it postgres-client -- psql "postgres://user:password@postgres-service:5432/substrate_fetcher" -c "SELECT epoch, COUNT(*) FROM miner_epoch_health GROUP BY epoch ORDER BY epoch DESC LIMIT 5;"

# Check health monitoring
python scripts/query_miner_health.py --epoch 12345
python scripts/query_availability_report.py --report
```

---

## 🎯 Usage Patterns

### Validator Epoch Workflow

1. **Start of Epoch**:
   ```bash
   # Clear and refresh registration data
   kubectl run registration-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/registration_processor.py
   
   # Start comprehensive health assessment
   kubectl run epoch-health-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/epoch_health_processor.py
   ```

2. **During Epoch**:
   - Continuous health monitoring runs automatically
   - Real-time reassignment handles failures
   - Profile reconstruction as needed

3. **End of Epoch**:
   - Query epoch health results
   - Generate reports
   - Prepare for next epoch

### Data Refresh Workflow

```bash
# Full data refresh
kubectl run registration-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/registration_processor.py
kubectl run node-metrics-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/node_metrics_processor.py
kubectl run user-profile-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/user_profile_processor.py
kubectl run miner-profile-processor --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/miner_profile_processor.py
```

---

## 🔧 Configuration

### Environment Variables

#### Global Configuration (ConfigMap: `ipfs-validator-config`)
- `RABBITMQ_URL`: RabbitMQ connection string
- `DATABASE_URL`: PostgreSQL connection string
- `NODE_URL`: Substrate node endpoint
- `IPFS_URL`: IPFS node endpoint

#### Health Monitoring Configuration
- `MAX_FILES_PER_MINER`: Files checked per miner (real-time)
- `EPOCH_MAX_FILES_PER_MINER`: Files checked per miner (epoch)
- `ENABLE_AUTO_REASSIGNMENT`: Enable automatic reassignment
- `MIN_REPLICAS`: Minimum replicas per file
- `MAX_REPLICAS`: Maximum replicas per file
- `MIN_AVAILABILITY_SCORE`: Minimum availability threshold
- `MAX_CONSECUTIVE_FAILURES`: Max failures before marking inactive

---

## 📈 Performance Considerations

### Message Processing
- **Prefetch Count**: 1 (process one message at a time)
- **Message Persistence**: All messages are persistent
- **Queue Durability**: All queues are durable
- **Connection Robustness**: Auto-reconnection enabled

### Resource Limits
- **File Limits**: Configurable per consumer type
- **Batch Processing**: One message per miner/entity
- **Backpressure**: Controlled via prefetch limits
- **Timeout Handling**: Graceful shutdown on stop events

### Scaling
- **Horizontal**: Increase consumer replicas
- **Vertical**: Adjust resource limits
- **Queue Partitioning**: Separate queues for different data types
- **Load Balancing**: Round-robin message distribution

---

## 🚨 Troubleshooting

### Common Issues

1. **Consumer Not Processing Messages**:
   ```bash
   kubectl logs -f consumer-pod-name
   kubectl describe pod consumer-pod-name
   ```

2. **Queue Backlog**:
   - Check RabbitMQ management UI
   - Verify consumer is running
   - Check database connectivity

3. **Database Connection Issues**:
   ```bash
   kubectl exec -it postgres-client -- psql $DATABASE_URL -c "SELECT 1;"
   ```

4. **IPFS Connectivity**:
   ```bash
   kubectl exec -it consumer-pod -- curl http://ipfs-service:5001/api/v0/id
   ```

### Recovery Procedures

1. **Restart Consumer**:
   ```bash
   kubectl rollout restart deployment/consumer-name
   ```

2. **Clear Queue** (if needed):
   ```bash
   kubectl exec -it rabbitmq-pod -- rabbitmqctl purge_queue queue_name
   ```

3. **Reprocess Data**:
   ```bash
   kubectl run processor-name --image=ipfs-service-validator:latest --rm -it --restart=Never -- python rabbitmq/processor_name.py
   ```

---

## 📝 Development Guidelines

### Adding New Producer/Consumer Pairs

1. **Create Producer** (`new_data_processor.py`):
   - Connect to data source
   - Clear target table
   - Fetch and queue data
   - Graceful shutdown

2. **Create Consumer** (`new_data_consumer.py`):
   - Process queue messages
   - Update database
   - Error handling
   - Continuous operation

3. **Add to Kubernetes** (`consumers.yaml`):
   - Deployment configuration
   - Environment variables
   - Resource limits
   - Health checks

4. **Update Documentation**:
   - Add to this file
   - Update README if needed
   - Document configuration options

### Code Standards

- **Error Handling**: Comprehensive try/catch blocks
- **Logging**: Structured logging with appropriate levels
- **Configuration**: Environment variable driven
- **Testing**: Unit tests for core logic
- **Documentation**: Inline comments and docstrings

---

This documentation provides a complete reference for the RabbitMQ architecture in the IPFS Service Validator system. All components follow consistent patterns and are designed for reliability, scalability, and maintainability. 