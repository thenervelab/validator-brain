# Kubernetes Deployment for IPFS Service Validator

This directory contains Kubernetes manifests for deploying the IPFS Service Validator to Minikube.

## Prerequisites

- Minikube installed and running
- kubectl configured to use Minikube
- Docker installed (for building the image)

## Setup Instructions

### 1. Start Minikube

```bash
minikube start --memory=4096 --cpus=2
```

### 2. Build the Docker Image

First, configure Docker to use Minikube's Docker daemon:

```bash
eval $(minikube docker-env)
```

Then build the image:

```bash
# From the project root directory
docker build -t ipfs-service-validator:latest .
```

### 3. Create the Migrations ConfigMap

```bash
cd k8s
chmod +x create-migrations-configmap.sh
./create-migrations-configmap.sh
```

### 4. Deploy to Kubernetes

Apply all the manifests in order:

```bash
# Create ConfigMap for environment variables
kubectl apply -f configmap.yaml

# Create PersistentVolumeClaims
kubectl apply -f persistent-volumes.yaml

# Deploy PostgreSQL
kubectl apply -f postgres.yaml

# Deploy RabbitMQ
kubectl apply -f rabbitmq.yaml

# Deploy IPFS
kubectl apply -f ipfs.yaml

# Apply migrations ConfigMap
kubectl apply -f migrations-configmap.yaml

# Run database migrations
kubectl apply -f dbmate-job.yaml

# Wait for migrations to complete
kubectl wait --for=condition=complete job/dbmate-migrations --timeout=60s

# Deploy consumers
kubectl apply -f consumers.yaml

# Create NodePort services for external access
kubectl apply -f nodeports.yaml
```

### 5. Mount Application Code (for development)

For development, you need to mount your local code into Minikube:

```bash
# From the project root directory
minikube mount $(pwd):/app
```

Keep this terminal open while developing.

### 6. Access Services

Get the Minikube IP:

```bash
minikube ip
```

Then access services at:
- PostgreSQL: `<minikube-ip>:30432`
- RabbitMQ Management UI: `http://<minikube-ip>:30672` (username: admin, password: admin)
- IPFS Gateway: `http://<minikube-ip>:30080`
- IPFS API: `http://<minikube-ip>:30501`

Or use minikube service command:

```bash
# Open RabbitMQ Management UI in browser
minikube service rabbitmq-management-nodeport

# Get service URLs
minikube service list
```

## Running Processors

The processors need to be run manually to populate the queues:

```bash
# Create a pod with the application
kubectl run processor --image=ipfs-service-validator:latest --rm -it -- bash

# Inside the pod, run processors as needed:
python rabbitmq/user_profile_processor.py
python rabbitmq/miner_profile_processor.py
python rabbitmq/pinning_request_processor.py
python rabbitmq/node_metrics_processor.py
python rabbitmq/miner_health_processor.py  # Queue health checks for miners
python rabbitmq/registration_processor.py  # Update node registrations
```

## Registration System

The deployment includes an automated registration system that:

1. **Fetches registration data** from both `registration.coldkeyNodeRegistration` and `registration.nodeRegistration` storage items
2. **Clears the registration table** before processing to ensure fresh data
3. **Combines data** from both storage sources into a unified registration table
4. **Handles duplicates** using upsert logic when the same node appears in both sources

### Registration Data Flow

1. **Producer** (`registration_processor.py`):
   - Connects to substrate chain
   - Clears the registration table
   - Fetches from `registration.coldkeyNodeRegistration`
   - Fetches from `registration.nodeRegistration`
   - Queues individual registration records

2. **Consumer** (`registration_consumer.py`):
   - Processes registration messages from queue
   - Stores data in registration table using upsert logic
   - Handles data from both storage sources

### Running Registration Updates

```bash
# Update registration data
kubectl exec -it <processor-pod> -- python rabbitmq/registration_processor.py

# Check registration data
kubectl exec -it <processor-pod> -- psql $DATABASE_URL -c "SELECT node_id, node_type, status, owner_account FROM registration LIMIT 10;"
```

## Health Monitoring System

The deployment includes an automated health monitoring system that:

1. **Monitors all files** assigned to each miner via IPFS ping and pin checks
2. **Tracks failures** in the `file_failures` and `miner_availability` tables
3. **Automatically reassigns replicas** when miners fail availability checks
4. **Maintains minimum replica counts** (default: 3 replicas per file)

### Health Check Configuration

The `miner-health-consumer-with-reassignment` deployment includes these configurable parameters:

- `MAX_FILES_PER_MINER`: Maximum files to check per miner (default: 10)
- `ENABLE_AUTO_REASSIGNMENT`: Enable automatic replica reassignment (default: true)
- `MIN_REPLICAS`: Minimum replicas per file (default: 3)
- `MIN_AVAILABILITY_SCORE`: Minimum score for reliable miners (default: 0.7)
- `MAX_CONSECUTIVE_FAILURES`: Max failures before marking miner inactive (default: 3)
- `FAILURE_WINDOW_HOURS`: Time window for failure tracking (default: 24)

### Monitoring Health Status

```bash
# Check availability report
kubectl exec -it <processor-pod> -- python scripts/query_availability_report.py --report

# View files needing reassignment
kubectl exec -it <processor-pod> -- python scripts/query_availability_report.py --reassignments

# Process pending reassignments manually
kubectl exec -it <processor-pod> -- python scripts/query_availability_report.py --process

# View specific miner details
kubectl exec -it <processor-pod> -- python scripts/query_availability_report.py --miner <miner-id>
```

## Monitoring

Check pod status:

```bash
kubectl get pods
kubectl logs <pod-name>
```

Check jobs:

```bash
kubectl get jobs
kubectl logs job/dbmate-migrations
```

## Cleanup

To remove all resources:

```bash
kubectl delete -f .
```

To stop Minikube:

```bash
minikube stop
```

To delete Minikube cluster:

```bash
minikube delete
```

## Troubleshooting

1. **Pods stuck in Pending**: Check PVC status with `kubectl get pvc`
2. **Image pull errors**: Make sure you're using Minikube's Docker daemon
3. **Services not accessible**: Check NodePort services with `kubectl get svc`
4. **Database connection errors**: Check if migrations completed successfully

## Notes

- The consumer deployments use `hostPath` volumes to mount code from the host
- Make sure to run `minikube mount` to make your code available to the pods
- The `imagePullPolicy: Never` ensures Kubernetes uses the locally built image
- Adjust resource requests/limits in the deployments as needed for your environment 