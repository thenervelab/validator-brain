# IPFS Service Validator

This project validates IPFS services and tracks their performance metrics on the Hippius network.

## Overview

The IPFS Service Validator consists of several components:
- **Processors**: Fetch data from the blockchain and queue it in RabbitMQ
- **Consumers**: Process queued messages and store data in PostgreSQL
- **Database**: PostgreSQL database for persistent storage
- **Message Queue**: RabbitMQ for reliable message processing

## Setup

### Using Docker Compose (Recommended)

1. Clone the repository
2. Create a `.env` file with required configuration (see Configuration section)
3. Start all services:
```bash
docker compose up -d
```

### Manual Setup

1. Install dependencies:
```bash
pip install -e .
```

2. Set up the database:
```bash
dbmate up
```

3. Start RabbitMQ:
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## Configuration

Create a `.env` file with the following variables:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/substrate_fetcher
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=substrate_fetcher

# Blockchain
NODE_URL=wss://rpc.hippius.network

# RabbitMQ
RABBITMQ_URL=amqp://admin:admin@localhost:5672/
RABBITMQ_USER=admin
RABBITMQ_PASSWORD=admin

# IPFS
IPFS_NODE_URL=http://localhost:5001
IPFS_GATEWAY_URL=https://ipfs.io
REMOTE_IPFS_URL=https://store.hippius.network

# Validator
VALIDATOR_ACCOUNT_ID=5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g

# Processing Configuration
MINER_PROFILE_BATCH_SIZE=0  # 0 = process all miners, >0 = batch size limit
USER_PROFILE_BATCH_SIZE=0   # 0 = process all users, >0 = batch size limit
NODE_METRICS_HISTORY_BLOCKS=10  # Number of blocks to keep per miner
```

## Components

### 1. User Profiles

Processes user profile data from IPFS:

```bash
# Fill queue with user profile data
python rabbitmq/user_profile_processor.py

# Start consumer (or multiple instances)
python rabbitmq/user_profile_consumer.py
```

### 2. Miner Profiles

Processes miner profile data:

```bash
# Fill queue with miner profile data
python rabbitmq/miner_profile_processor.py 

# Start consumer
python rabbitmq/miner_profile_consumer.py
```

### 3. Miner Profile Reconstruction

Reconstructs and publishes miner profiles to remote IPFS:

```bash
# Queue miners for profile reconstruction (configurable batch size)
python rabbitmq/miner_profile_reconstruction_processor.py

# Start consumer to reconstruct and publish profiles
python rabbitmq/miner_profile_reconstruction_consumer.py
```

**Batch Size Configuration:**
- Set `MINER_PROFILE_BATCH_SIZE=0` to process ALL eligible miners (default in Kubernetes)
- Set `MINER_PROFILE_BATCH_SIZE=100` to process 100 miners at a time
- Set `MINER_PROFILE_BATCH_SIZE=500` to process 500 miners at a time

**Important Security Notes:**
- Files without owners in `file_assignments` table are **skipped** to prevent incorrect billing
- `VALIDATOR_ACCOUNT_ID` environment variable is **required** - no default fallback to prevent incorrect validator assignments

The processor will skip miners that:
- Have no files assigned in the `file_assignments` table
- Already have published profiles in the `pending_miner_profile` table

### 4. User Profile Reconstruction

Reconstructs and publishes user profiles to remote IPFS:

```bash
# Queue users for profile reconstruction (configurable batch size)
python rabbitmq/user_profile_reconstruction_processor.py

# Start consumer to reconstruct and publish profiles
python rabbitmq/user_profile_reconstruction_consumer.py
```

**Batch Size Configuration:**
- Set `USER_PROFILE_BATCH_SIZE=0` to process ALL eligible users (default in Kubernetes)
- Set `USER_PROFILE_BATCH_SIZE=100` to process 100 users at a time
- Set `USER_PROFILE_BATCH_SIZE=500` to process 500 users at a time

**Important Security Notes:**
- `VALIDATOR_ACCOUNT_ID` environment variable is **required** - no default fallback to prevent incorrect validator assignments

The processor will skip users that:
- Have no files assigned in the `file_assignments` table
- Already have published profiles in the `pending_user_profile` table

### 5. Pinning Requests

Handles storage pinning requests:

```bash
# Process pinning requests
python rabbitmq/pinning_request_processor.py

# Start consumer
python rabbitmq/pinning_request_consumer.py
```

### 6. Pinning File Processing

Processes individual files from pinning requests:

```bash
# Parse pinning request files and queue individual files
python rabbitmq/pinning_file_processor.py

# Start consumer to get file sizes and store in database
python rabbitmq/pinning_file_consumer.py
```

This workflow:
1. **Processor**: Fetches pinning request files from IPFS, parses JSON content, and queues individual files
2. **Consumer**: Gets file sizes from IPFS and stores file information in `pending_assignment_file` table

**File Format Expected:**
```json
[
  {
    "filename": "image (7).jpg",
    "cid": "bafkreigqtkuunz3cqxs44jutnp7djz4bcf74goc3kstex7gjhbagufjonm"
  }
]
```

**Kubernetes Deployment:**
- Consumer runs automatically as `pinning-file-consumer`
- Processor runs on-demand using `kubectl apply -f k8s/pinning-file-processor-job.yaml`
- Uses IPFS service at `http://ipfs-service:5001` for scalability

### 7. Node Metrics

Fetches and stores IPFS node metrics from the blockchain:

```bash
# Fetch metrics from the latest block (one-time execution, run manually)
python rabbitmq/node_metrics_processor.py

# The consumer runs automatically via Docker Compose
# To run manually or use the history retention version:
python rabbitmq/node_metrics_consumer.py
NODE_METRICS_HISTORY_BLOCKS=10 python rabbitmq/node_metrics_consumer_with_history.py

# Query metrics
python scripts/query_node_metrics.py [options]

# Clear the queue if needed
python rabbitmq/clear_queue.py node_metrics_latest
```

**Note**: 
- The processor (`node_metrics_processor.py`) should be run manually when you need to fetch the latest metrics
- The consumer runs automatically in Docker Compose using `node_metrics_consumer.py` (latest metrics only)
- For historical data retention, manually run `node_metrics_consumer_with_history.py` instead

### 8. Miner Health Checks

Performs IPFS ping and pin tests on miners to validate their connectivity and file availability:

```bash
# Queue miners for health checks (run manually when needed)
python rabbitmq/miner_health_processor.py

# Start health check consumer (processes ping and pin tests)
python rabbitmq/miner_health_consumer.py

# Inspect the health check queue
python rabbitmq/inspect_health_queue.py

# Query health check results
python scripts/query_miner_health.py [options]
```

**Health Check Process:**
1. **Processor**: Fetches miners from database (file_assignments + node_metrics), gets current epoch, queues health check tasks
2. **Consumer**: Performs IPFS ping tests and pin tests on random files assigned to each miner
3. **Database**: Results stored in `miner_epoch_health` table with success/failure counts per epoch

**Health Check Types:**
- **Ping Test**: Tests IPFS connectivity to miner's peer ID using `/api/v0/ping`
- **Pin Test**: Verifies miner is a provider for a random file using DHT `/api/v0/dht/findprovs`

**Query Examples:**
```bash
# Show latest health summary for all miners
python scripts/query_miner_health.py

# Show health for specific epoch
python scripts/query_miner_health.py --epoch 7104

# Show detailed history for specific miner
python scripts/query_miner_health.py --miner 12D3KooWKnhGPbTtCgEPWRxGJhtFFcbMTEerfSKMpVnbpLQzBy

# Show epoch statistics
python scripts/query_miner_health.py --epoch 7104 --stats
```

**Configuration:**
- `IPFS_TIMEOUT_SECONDS`: Timeout for ping operations (default: 10s)
- `IPFS_DHT_TIMEOUT_SECONDS`: Timeout for DHT provider lookups (default: 60s)
- `IPFS_REFS_TIMEOUT_SECONDS`: Timeout for fetching file references (default: 30s)

## Docker Services

## Kubernetes Deployment

The Kubernetes deployment includes all services and consumers:

**Services:**
- `postgres`: PostgreSQL database
- `rabbitmq`: Message queue with management UI
- `ipfs`: IPFS node
- `dbmate`: Database migration tool

**Consumers (all running automatically):**
- `user-profile-consumer`: Processes user profiles from IPFS
- `pinning-request-consumer`: Processes pinning requests
- `pinning-file-consumer`: Processes individual files from pinning requests
- `node-metrics-consumer`: Processes node metrics from the queue
- `miner-profile-reconstruction-consumer`: Reconstructs and publishes miner profiles
- `user-profile-reconstruction-consumer`: Reconstructs and publishes user profiles

**Processors (run as jobs when needed):**
- `miner-profile-reconstruction-processor`: Queues miners for profile reconstruction
- `user-profile-reconstruction-processor`: Queues users for profile reconstruction
- `pinning-file-processor`: Parses pinning request files and queues individual files
- `miner-health-processor`: Queues miners for health checks

**Running Processor Jobs:**
```bash
# Run miner profile reconstruction processor
kubectl apply -f k8s/miner-profile-reconstruction-job.yaml

# Run user profile reconstruction processor  
kubectl apply -f k8s/user-profile-reconstruction-job.yaml

# Run pinning file processor
kubectl apply -f k8s/pinning-file-processor-job.yaml
```

## Docker Compose

The `docker-compose.yml` includes basic services but not all consumers. For full functionality, use Kubernetes deployment.

## Database Schema

### Tables

1. **files**: IPFS file information
   - `cid`: Content identifier (unique)
   - `name`: File name
   - `size`: File size in bytes
   - `created_date`: Creation timestamp

2. **file_assignments**: Maps files to owners and miners
   - `cid`: File CID (foreign key)
   - `owner`: File owner
   - `miner1-5`: Assigned miners
   - `updated_at`: Last update timestamp

3. **parsed_cids**: Tracks processed CIDs
   - `cid`: Profile CID
   - `profile_type`: 'user' or 'miner'
   - `parsed_at`: Processing timestamp
   - `file_count`: Number of files in profile
   - `account`: Associated account

4. **pinning_requests**: Storage pinning requests
   - Request details including file hash, name, replicas, etc.

5. **node_metrics**: IPFS node storage metrics
   - `miner_id`: Miner's peer ID
   - `ipfs_repo_size`: Repository size in bytes
   - `ipfs_storage_max`: Maximum storage capacity
   - `block_number`: Blockchain block number
   - `updated_at`: Last update timestamp

6. **pending_miner_profile**: Tracks miner profile reconstruction
   - `cid`: Published IPFS CID
   - `node_id`: Miner's peer ID
   - `files_count`: Number of files in profile
   - `files_size`: Total size of files
   - `block_number`: Block number when created
   - `status`: Processing status ('pending', 'published', 'failed')
   - `created_at`, `published_at`: Timestamps

7. **pending_user_profile**: Tracks user profile reconstruction
   - `cid`: Published IPFS CID
   - `owner`: User's account ID
   - `files_count`: Number of files in profile
   - `files_size`: Total size of files
   - `block_number`: Block number when created
   - `status`: Processing status ('pending', 'published', 'failed')
   - `created_at`, `published_at`: Timestamps

8. **pending_assignment_file**: Individual files from pinning requests
   - `cid`: File CID
   - `owner`: File owner
   - `filename`: Original filename
   - `file_size_bytes`: File size in bytes
   - `status`: Processing status ('pending', 'processed', 'failed')
   - `created_at`, `processed_at`: Timestamps

## Monitoring

### RabbitMQ Management

Access the RabbitMQ management UI at http://localhost:15672
- Username: admin
- Password: admin

### Queue Inspection

```bash
# Inspect any queue
python rabbitmq/inspect_queue.py <queue_name>

# Available queues:
# - user_profile
# - miner_profile
# - miner_profile_reconstruction
# - user_profile_reconstruction
# - pinning_request
# - pinning_file_processing
# - node_metrics_latest

# Example: Check pinning file processing queue
python rabbitmq/inspect_queue.py pinning_file_processing
```

### Database Monitoring

```bash
# Check processed files in pending_assignment_file table
psql $DATABASE_URL -c "SELECT cid, owner, filename, file_size_bytes, status FROM pending_assignment_file ORDER BY created_at DESC LIMIT 10;"

# Check processing statistics
psql $DATABASE_URL -c "SELECT status, COUNT(*) FROM pending_assignment_file GROUP BY status;"
```

### Node Metrics Queries

```bash
# Get latest metrics for all miners
python scripts/query_node_metrics.py

# Get aggregate statistics
python scripts/query_node_metrics.py --stats

# Get history for specific miner
python scripts/query_node_metrics.py --miner <MINER_ID> --limit 20
```

## Development

### Running Migrations

```bash
# Run migrations
dbmate up

# Create new migration
dbmate new <migration_name>

# Rollback
dbmate down
```

### Testing Connections

```bash
# Test substrate and RabbitMQ connections
python rabbitmq/test_connection.py
```

## Troubleshooting

1. **Container startup issues**: Check logs with `docker compose logs <service-name>`
2. **Database connection errors**: Ensure PostgreSQL is running and credentials are correct
3. **RabbitMQ connection errors**: Check if RabbitMQ is accessible on port 5672
4. **IPFS errors**: Ensure IPFS node is running and API is accessible

## License

MIT License - see LICENSE file for details




DATABASE_URL=postgresql://user:password@localhost:5432/substrate_fetcher IPFS_NODE_URL=http://localhost:5001 python rabbitmq/node_metrics_processor.py 