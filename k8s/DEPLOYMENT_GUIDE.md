# IPFS Service Validator - Deployment Guide

This guide explains how to deploy the IPFS Service Validator orchestrator and all required components using the comprehensive deployment script.

## Prerequisites

- Kubernetes cluster (v1.20+)
- kubectl configured and authenticated
- Docker (if building images)
- Sufficient cluster resources:
  - 4+ CPU cores
  - 8+ GB RAM
  - 50+ GB storage

## Quick Start

### 1. Basic Deployment (Default Namespace)

```bash
# Deploy to default namespace 'ipfs-validator'
./deploy-full.sh
```

### 2. Custom Namespace Deployment

```bash
# Deploy to a custom namespace
./deploy-full.sh my-production-env
```

### 3. Production Deployment (No Build)

```bash
# Deploy without building image (use existing)
./deploy-full.sh production --no-build
```

## Script Options

| Option | Description | Default |
|--------|-------------|---------|
| `NAMESPACE` | Target Kubernetes namespace | `ipfs-validator` |
| `--no-build` | Skip Docker image build | Build enabled |
| `--timeout N` | Wait timeout in seconds | 300 |
| `--verbose` | Enable verbose logging | Disabled |
| `--dry-run` | Show deployment plan without applying | Disabled |
| `--help` | Show help message | - |

## Deployment Examples

### Development Environment
```bash
# Quick development deployment with verbose output
./deploy-full.sh dev-env --verbose
```

### Staging Environment
```bash
# Staging with longer timeout for slower clusters
./deploy-full.sh staging --timeout 600 --no-build
```

### Production Environment
```bash
# Production deployment with existing images
./deploy-full.sh production --no-build --timeout 900
```

### Dry Run (Preview)
```bash
# See what would be deployed without applying
./deploy-full.sh test-namespace --dry-run
```

## Deployment Phases

The script deploys components in the following order:

### Phase 1: Core Infrastructure
- **Persistent Volumes**: Storage configuration
- **ConfigMaps**: Application and migration configs

### Phase 2: Database Infrastructure  
- **PostgreSQL**: High-scale database (1000 connections)
- **Database Migrations**: Schema setup via dbmate

### Phase 3: Message Queue & Storage
- **RabbitMQ**: Distributed message processing
- **IPFS**: Distributed file storage with auto-scaling

### Phase 4: Processing Infrastructure
- **Consumers**: Background message processors
  - User Profile Consumer
  - Miner Profile Consumer  
  - File Assignment Consumer

### Phase 5: Job Processors
- **Health Processors**: Miner and epoch health checks
- **Assignment Processors**: File assignment management
- **Profile Processors**: User and miner profile reconstruction

### Phase 6: Orchestrator
- **Epoch Orchestrator**: Main coordination service

### Phase 7: Network Services
- **NodePorts**: External access (if configured)

## Useful Commands

After deployment, use these commands to monitor and manage:

```bash
# Check all resources in namespace
kubectl get all -n your-namespace

# Check orchestrator logs
kubectl logs -l app=epoch-orchestrator -n your-namespace --tail=50

# Monitor all pods
kubectl get pods -n your-namespace -w

# Scale IPFS if needed
kubectl scale statefulset ipfs --replicas=3 -n your-namespace
``` 