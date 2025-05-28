# Epoch Orchestrator Documentation

The **Epoch Orchestrator** is the central controller that manages the entire IPFS Service Validator application lifecycle. It continuously monitors the blockchain and executes different workflows based on whether the validator is the current epoch validator or not.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Workflows](#workflows)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Overview

### Key Features

- **🔄 Continuous Monitoring**: Checks blockchain every 6 seconds (every block)
- **🎭 Role-Based Execution**: Different workflows for validators vs non-validators
- **📊 Phase-Based Operations**: Structured execution within 100-block epochs
- **🔐 Transaction Signing**: Optional support for submitting transactions to chain
- **⚡ Queue Coordination**: Waits for processors to complete before proceeding
- **🛡️ Error Handling**: Robust error handling with automatic recovery
- **📈 State Management**: Tracks completion to avoid duplicate work

### Epoch Structure

Each epoch consists of **100 blocks** (~10 minutes), divided into phases:

```
Block 0-10:   🚀 Initialization Phase
Block 11-50:  📌 Pinning Phase (Validator Only)
Block 51-80:  📋 Assignment Phase
Block 81-95:  🔧 Reconstruction Phase
Block 96-99:  🏁 Finalization Phase
```

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Epoch Orchestrator                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Blockchain      │  │ Queue           │  │ State        │ │
│  │ Monitor         │  │ Coordinator     │  │ Manager      │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Validator       │  │ Non-Validator   │  │ Transaction  │ │
│  │ Workflow        │  │ Workflow        │  │ Signer       │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Processor Execution                     │
├─────────────────────────────────────────────────────────────┤
│ Registration │ Node Metrics │ User Profiles │ Health Checks │
│ Pinning Req  │ Pinning Files│ File Assign   │ Reconstruction│
└─────────────────────────────────────────────────────────────┘
```

### Integration Points

- **Blockchain**: Queries `IpfsPallet.CurrentEpochValidator` for role determination
- **Database**: PostgreSQL for persistent state and data storage
- **Message Queue**: RabbitMQ for processor coordination
- **IPFS**: For file storage and retrieval operations

## Configuration

### Environment Variables

#### Required Configuration

```env
# Validator Identity (REQUIRED)
VALIDATOR_ACCOUNT_ID=5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g

# Blockchain Connection
NODE_URL=wss://rpc.hippius.network

# Database Connection
DATABASE_URL=postgresql://user:password@postgres-service:5432/substrate_fetcher

# Message Queue
RABBITMQ_URL=amqp://admin:admin@rabbitmq-service:5672/
```

#### Optional Configuration

```env
# Monitoring Configuration
BLOCK_CHECK_INTERVAL=6           # Check every 6 seconds (every block)
QUEUE_CHECK_TIMEOUT=300          # 5 minute timeout for queue processing

# Transaction Signing (OPTIONAL but recommended for validators)
VALIDATOR_SEED="your twelve word seed phrase here for signing transactions"

# Processor Timeouts
PROCESSOR_TIMEOUT=600            # 10 minute timeout for individual processors
```

### Security Considerations

#### Validator Seed Management

The validator seed is **optional** but recommended for full functionality:

- **With Seed**: Can submit health check results and other transactions to chain
- **Without Seed**: Read-only mode, cannot submit transactions

**🔒 Security Best Practices:**

1. **Never commit seeds to version control**
2. **Use Kubernetes secrets for production**
3. **Rotate seeds periodically**
4. **Monitor for unauthorized access**

#### Kubernetes Secrets (Recommended)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: validator-secrets
type: Opaque
stringData:
  VALIDATOR_SEED: "your twelve word seed phrase here"
  VALIDATOR_ACCOUNT_ID: "5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g"
```

Then reference in deployment:

```yaml
env:
- name: VALIDATOR_SEED
  valueFrom:
    secretKeyRef:
      name: validator-secrets
      key: VALIDATOR_SEED
- name: VALIDATOR_ACCOUNT_ID
  valueFrom:
    secretKeyRef:
      name: validator-secrets
      key: VALIDATOR_ACCOUNT_ID
```

## Deployment

### Local Development

```bash
# Set environment variables
export VALIDATOR_ACCOUNT_ID="5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g"
export VALIDATOR_SEED="your twelve word seed phrase here"
export NODE_URL="wss://rpc.hippius.network"
export DATABASE_URL="postgresql://user:password@localhost:5432/substrate_fetcher"
export RABBITMQ_URL="amqp://admin:admin@localhost:5672/"

# Run orchestrator
python epoch_orchestrator.py
```

### Kubernetes Deployment

```bash
# Update configuration in k8s/epoch-orchestrator.yaml
# Replace VALIDATOR_ACCOUNT_ID and VALIDATOR_SEED with your values

# Deploy
kubectl apply -f k8s/epoch-orchestrator.yaml

# Check status
kubectl get pods -l app=epoch-orchestrator
kubectl logs -f deployment/epoch-orchestrator
```

### Docker Compose

```yaml
version: '3.8'
services:
  epoch-orchestrator:
    build: .
    command: python epoch_orchestrator.py
    environment:
      - VALIDATOR_ACCOUNT_ID=5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g
      - VALIDATOR_SEED=your twelve word seed phrase here
      - NODE_URL=wss://rpc.hippius.network
      - DATABASE_URL=postgresql://user:password@postgres:5432/substrate_fetcher
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672/
    depends_on:
      - postgres
      - rabbitmq
```

## Workflows

### Non-Validator Workflow

When **not** the current epoch validator:

```mermaid
graph TD
    A[Epoch Start] --> B[Initialize Data]
    B --> C[Registration Refresh]
    B --> D[Node Metrics Refresh]
    B --> E[User Profiles Refresh]
    C --> F[Wait for Queues]
    D --> F
    E --> F
    F --> G[Health Checks]
    G --> H[Submit Results to Chain]
    H --> I[Wait for Next Epoch]
    I --> A
```

**Timeline:**
- **Block 0-10**: Data initialization
- **Block 11-99**: Health checks and waiting

### Validator Workflow

When **is** the current epoch validator:

```mermaid
graph TD
    A[Epoch Start] --> B[Initialization Phase<br/>Blocks 0-10]
    B --> C[Pinning Phase<br/>Blocks 11-50]
    C --> D[Assignment Phase<br/>Blocks 51-80]
    D --> E[Reconstruction Phase<br/>Blocks 81-95]
    E --> F[Finalization Phase<br/>Blocks 96-99]
    F --> A
    
    B --> B1[Registration Refresh]
    B --> B2[Node Metrics Refresh]
    B --> B3[User Profiles Refresh]
    
    C --> C1[Process Pinning Requests]
    C --> C2[Process Pinning Files]
    
    D --> D1[Assign Files to Miners]
    D --> D2[Health Checks]
    
    E --> E1[User Profile Reconstruction]
    E --> E2[Miner Profile Reconstruction]
```

**Critical Timing:**
- **Profile reconstruction MUST complete by block 95**
- **Pinning requests processed periodically during blocks 11-50**
- **File assignment happens once during blocks 51-80**

## Monitoring

### Real-Time Status

```bash
# Check orchestrator logs
kubectl logs -f deployment/epoch-orchestrator

# Check current epoch and role
python -c "
from app.utils.epoch_validator import *
substrate = connect_substrate()
epoch, block = get_current_epoch_info(substrate)
validator_account = get_validator_account_from_env()
is_val, current_val, epoch_start = is_epoch_validator(substrate, validator_account)
print(f'Epoch: {epoch}, Block: {block}, Position: {block % 100}/99')
print(f'We are validator: {is_val}')
print(f'Current validator: {current_val}')
"
```

### Queue Status

```bash
# Check all queue status
python scripts/check_queue_status.py registration node_metrics_latest user_profile miner_health_check

# Wait for specific queues to be empty
python scripts/check_queue_status.py --wait --timeout 300 pinning_request pinning_file_processing
```

### Health Metrics

```bash
# Database connections
kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "SELECT COUNT(*) FROM registration;"

# RabbitMQ management
kubectl port-forward service/rabbitmq-service 15672:15672
# Open http://localhost:15672 (admin/admin)

# IPFS status
kubectl port-forward service/ipfs-service 5001:5001
curl http://localhost:5001/api/v0/id
```

### Log Analysis

Key log patterns to monitor:

```bash
# Successful epoch transitions
grep "New epoch detected" logs.txt

# Validator role changes
grep "Role:" logs.txt

# Phase completions
grep "completed successfully" logs.txt

# Errors and failures
grep "ERROR\|FAILED" logs.txt

# Transaction signing status
grep "Transaction signing" logs.txt
```

## Troubleshooting

### Common Issues

#### 1. Validator Account Not Set

**Error:** `ValueError: VALIDATOR_ACCOUNT_ID environment variable is required`

**Solution:**
```bash
export VALIDATOR_ACCOUNT_ID="5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g"
```

#### 2. Blockchain Connection Failed

**Error:** `Error connecting to substrate`

**Solutions:**
- Check NODE_URL is correct
- Verify network connectivity
- Ensure blockchain node is running

#### 3. Database Connection Failed

**Error:** `Failed to initialize epoch orchestrator`

**Solutions:**
- Verify DATABASE_URL is correct
- Check PostgreSQL is running
- Ensure database exists and migrations are applied

#### 4. Queue Processing Timeout

**Error:** `Timeout waiting for queues to be empty`

**Solutions:**
- Check RabbitMQ is running
- Verify consumers are processing messages
- Increase QUEUE_CHECK_TIMEOUT if needed

#### 5. Transaction Signing Failed

**Error:** `No validator seed available for transaction signing`

**Solutions:**
- Set VALIDATOR_SEED environment variable
- Verify seed phrase is correct
- Check keypair generation

### Performance Tuning

#### Resource Requirements

**Minimum:**
- CPU: 250m
- Memory: 512Mi

**Recommended:**
- CPU: 500m
- Memory: 1Gi

#### Scaling Considerations

- **Single Instance**: Orchestrator should run as single replica
- **High Availability**: Use pod disruption budgets and node affinity
- **Resource Monitoring**: Monitor CPU and memory usage

### Emergency Procedures

#### Manual Intervention

If orchestrator fails during critical phases:

```bash
# Run individual processors manually
python rabbitmq/registration_processor.py
python rabbitmq/node_metrics_processor.py
python rabbitmq/user_profile_processor.py

# Check queue status
python scripts/check_queue_status.py registration node_metrics_latest user_profile

# Restart orchestrator
kubectl rollout restart deployment/epoch-orchestrator
```

#### Recovery from Failed Epoch

1. **Check current block position**
2. **Identify failed phase**
3. **Run missing processors manually**
4. **Restart orchestrator**
5. **Monitor next epoch for normal operation**

### Support and Debugging

#### Enable Debug Logging

```yaml
env:
- name: LOG_LEVEL
  value: "DEBUG"
```

#### Collect Diagnostics

```bash
# Orchestrator logs
kubectl logs deployment/epoch-orchestrator > orchestrator.log

# Queue status
python scripts/check_queue_status.py registration node_metrics_latest user_profile > queues.json

# Database status
kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "\dt" > tables.txt

# System status
kubectl get pods -o wide > pods.txt
kubectl describe deployment epoch-orchestrator > deployment.txt
```

## Best Practices

### Security

1. **Use Kubernetes secrets for sensitive data**
2. **Rotate validator seeds regularly**
3. **Monitor for unauthorized access**
4. **Use network policies to restrict access**

### Operations

1. **Monitor orchestrator logs continuously**
2. **Set up alerts for failed epochs**
3. **Test disaster recovery procedures**
4. **Keep backups of configuration**

### Development

1. **Test with testnet before mainnet**
2. **Use staging environment for validation**
3. **Monitor resource usage patterns**
4. **Document configuration changes**

---

For additional support, check the main README.md or create an issue in the repository. 