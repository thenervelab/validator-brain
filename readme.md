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

### Quick Setup with Launch Script

For easy setup and testing, use the provided launch script:

```bash
# 1. Copy the environment template
cp environment.example my_environment.sh

# 2. Edit the file with your actual values
nano my_environment.sh

# 3. Source the environment
source my_environment.sh

# 4. Test connectivity
./launch_epoch_orchestrator.sh --check-only

# 5. Show current blockchain status
./launch_epoch_orchestrator.sh --status

# 6. Launch the orchestrator
./launch_epoch_orchestrator.sh
```

The launch script provides:
- ✅ **Pre-flight checks**: Validates environment variables and dependencies
- ✅ **Connectivity tests**: Tests blockchain and database connections
- ✅ **Status display**: Shows current epoch and validator information
- ✅ **Graceful shutdown**: Handles Ctrl+C properly
- ✅ **Colored output**: Easy-to-read status messages

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

### 7. File Assignment System

Assigns files from the `pending_assignment_file` table to miners with proper capacity checking and network balancing:

```bash
# Run file assignment processor (near end of epoch)
kubectl apply -f k8s/file-assignment-processor-job.yaml

# Start consumer to process assignments (runs automatically)
python rabbitmq/file_assignment_consumer.py

# Monitor assignment system
python scripts/query_file_assignments.py [options]
```

**Assignment Process:**
1. **Processor**: Fetches unassigned files from `pending_assignment_file`, selects optimal miners using advanced scoring algorithm, queues assignment tasks
2. **Consumer**: Processes assignment tasks, updates `file_assignments` table, marks files as assigned

**Reassignment Process:**
The system also handles **reassigning empty miner slots** in existing file assignments:
1. **Detection**: Identifies files with NULL miner columns (created when health checks remove offline miners)
2. **Smart Filling**: Selects new miners to fill empty slots while avoiding already-assigned miners
3. **Race Condition Protection**: Uses database locking and timestamp checking to prevent conflicts
4. **Seamless Integration**: Processes both new assignments and reassignments in the same workflow

**Scoring Algorithm:**
The system uses a sophisticated scoring algorithm to balance file assignments:
- **Storage Availability (50%)**: Prefers miners with more available storage
- **File Count Balance (25%)**: Distributes load evenly across miners
- **Health Score (15%)**: Considers miner reliability and performance
- **Registration Recency (10%)**: Slight preference for stability, but with new miner boost

**New Miner Support:**
- **New Miner Boost**: Miners registered within 30 days get 1.5x scoring boost
- **Capacity Checking**: Ensures miners have sufficient storage for assigned files
- **Balanced Distribution**: Prevents overloading any single miner

**Configuration:**
```env
REPLICAS_PER_FILE=5              # Number of replicas per file
MAX_FILES_PER_BATCH=100          # Files processed per batch
MAX_REASSIGNMENTS_PER_BATCH=50   # Reassignments processed per batch
MIN_MINER_HEALTH_SCORE=70.0      # Minimum health score for assignment
NEW_MINER_BOOST_DAYS=30          # Days for new miner boost
NEW_MINER_BOOST_FACTOR=1.5       # Boost factor for new miners
```

**Monitoring:**
```bash
# Show all assignment statistics
python scripts/query_file_assignments.py

# Show only pending files
python scripts/query_file_assignments.py --pending

# Show files needing reassignment
python scripts/query_file_assignments.py --reassignments

# Show miner distribution
python scripts/query_file_assignments.py --distribution

# Show new miner analysis
python scripts/query_file_assignments.py --new-miners

# Show capacity analysis
python scripts/query_file_assignments.py --capacity
```

**Integration with Health Checks:**
- Health check system removes offline miners by setting their columns to NULL
- File assignment system automatically detects and fills these empty slots
- No manual intervention required - the system self-heals
- Race condition protection ensures data consistency

**Kubernetes Deployment:**
- Consumer runs automatically as `file-assignment-consumer`
- Processor runs on-demand using the job manifest
- Integrates with existing health monitoring and capacity tracking

### 8. Node Metrics

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

### 9. Miner Health Checks

Performs IPFS ping and pin tests on miners to validate their connectivity and file availability. **Failed miners are automatically removed from file assignments**, and the file assignment system handles reassignment:

```bash
# Queue miners for health checks (run manually when needed)
python rabbitmq/miner_health_processor.py

# Start health check consumer (processes ping and pin tests)
python rabbitmq/miner_health_consumer.py

# Run health processor as Kubernetes job
kubectl apply -f k8s/miner-health-processor-job.yaml

# Query health check results
python scripts/query_miner_health.py [options]
```

**Simplified Health Check Process:**
1. **Processor**: Fetches active miners from registration table, gets a few assigned files per miner, queues health check messages
2. **Consumer**: Performs IPFS ping tests and pin tests on assigned files
3. **Failure Handling**: Removes failed miners from ALL file assignments (sets miner columns to NULL)
4. **Reassignment**: File assignment system automatically detects empty slots and reassigns files
5. **Database**: Results stored in `miner_epoch_health` table with success/failure counts per epoch

**Health Check Types:**
- **Ping Test**: Tests IPFS connectivity to miner's peer ID using `/api/v0/ping`
- **Pin Test**: Verifies miner is a provider for assigned files using DHT `/api/v0/dht/findprovs`

**Failure Thresholds:**
- **Ping Failure**: Remove miner immediately after 1 ping failure (configurable via `PING_FAILURE_THRESHOLD`)
- **Pin Failure**: Remove miner after 2 pin test failures (configurable via `PIN_FAILURE_THRESHOLD`)

**Integration with File Assignment System:**
- When miners fail health checks, they are removed from `file_assignments` table (columns set to NULL)
- File assignment processor automatically detects files with empty miner slots
- New healthy miners are selected and assigned to maintain replica count
- No manual intervention required - the system self-heals

**Configuration:**
```env
HEALTH_CHECK_FILES_PER_MINER=3   # Number of files to test per miner
PING_FAILURE_THRESHOLD=1         # Remove after 1 ping failure
PIN_FAILURE_THRESHOLD=2          # Remove after 2 pin failures
IPFS_TIMEOUT_SECONDS=10          # Timeout for ping operations
IPFS_DHT_TIMEOUT_SECONDS=60      # Timeout for DHT provider lookups
```

**Kubernetes Deployment:**
- Consumer runs automatically as `miner-health-consumer`
- Processor runs on-demand using `kubectl apply -f k8s/miner-health-processor-job.yaml`
- Integrates seamlessly with file assignment system for automatic reassignment

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

### 10. Epoch Orchestrator

The **Epoch Orchestrator** is the main controller that manages the entire IPFS Service Validator application lifecycle based on whether we are the current epoch validator or not. It runs continuously and coordinates all processors and consumers.

**🆕 Automatic Table Cleanup**: At the start of each epoch, the orchestrator automatically cleans up the following tables to ensure fresh data:
- `pinning_requests`
- `miner_epoch_health` 
- `node_metrics`
- `parsed_cids`
- `pending_assignment_file`
- `pending_miner_profile`
- `pending_submissions`
- `pending_user_profile`
- `processed_pinning_requests`

```bash
# Run the epoch orchestrator (main application controller)
python epoch_orchestrator.py

# OR use the convenient launch script with pre-flight checks
./launch_epoch_orchestrator.sh

# Deploy in Kubernetes
kubectl apply -f k8s/epoch-orchestrator.yaml
```

**Launch Script Options:**
```bash
# Show help and usage
./launch_epoch_orchestrator.sh --help

# Check connectivity without launching
./launch_epoch_orchestrator.sh --check-only

# Show current blockchain status
./launch_epoch_orchestrator.sh --status

# Normal launch (with full pre-flight checks)
./launch_epoch_orchestrator.sh
```

**Epoch Structure (100 blocks per epoch):**
- **Block 0-10**: Initialization (registration, node metrics, user profiles)
- **Block 11-50**: Pinning requests processing (validator only)
- **Block 51-80**: File assignment and health checks
- **Block 81-95**: Profile reconstruction (must complete before block 95)
- **Block 96-99**: Finalization and preparation for next epoch

**Workflow Modes:**

**🔸 Non-Validator Mode:**
1. Refresh data at epoch start (registration, node metrics, user profiles)
2. Perform health checks and submit results to chain
3. Wait for end of epoch

**🔸 Validator Mode:**
1. **Initialization Phase (0-10)**: Refresh all base data
2. **Pinning Phase (11-50)**: Process pinning requests periodically
3. **Assignment Phase (51-80)**: Assign files and perform health checks
4. **Reconstruction Phase (81-95)**: Reconstruct user and miner profiles
5. **Finalization Phase (96-99)**: Prepare for next epoch

**Key Features:**
- **Automatic Role Detection**: Queries blockchain to determine if we're the current epoch validator
- **Phase-Based Execution**: Different tasks run at appropriate times within the epoch
- **Queue Monitoring**: Waits for processors to complete before moving to next phase
- **State Management**: Tracks completion of each phase to avoid duplicate work
- **Error Handling**: Robust error handling with fallback mechanisms
- **Continuous Operation**: Runs indefinitely, monitoring blockchain for epoch changes

**Configuration:**
```env
VALIDATOR_ACCOUNT_ID=5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g  # Your validator account (REQUIRED)
VALIDATOR_SEED="your twelve word seed phrase here"                        # Your validator seed for transaction signing (OPTIONAL)
BLOCK_CHECK_INTERVAL=6           # How often to check blockchain (seconds) - every block
QUEUE_CHECK_TIMEOUT=300          # Timeout for queue processing (seconds)
```

**Blockchain Integration:**
- Queries `IpfsPallet.CurrentEpochValidator` to determine current validator
- Monitors block progression every 6 seconds (every block) to trigger phase transitions
- Automatically detects epoch changes and resets state
- Optional transaction signing with validator seed for submitting results to chain

**Queue Management:**
- Monitors RabbitMQ queues to ensure processing completion
- Uses actual queue message counts (not time-based assumptions)
- Waits for queues to be empty before proceeding to next phase

**Kubernetes Deployment:**
- Runs as single replica deployment (`epoch-orchestrator`)
- Requires `VALIDATOR_ACCOUNT_ID` environment variable
- Optional `VALIDATOR_SEED` for transaction signing capabilities
- Manages all other processors automatically
- All consumers run continuously in parallel

**Monitoring:**
```bash
# Check orchestrator logs
kubectl logs -f deployment/epoch-orchestrator

# Check current epoch and validator status
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

# Check queue status
python scripts/check_queue_status.py registration node_metrics_latest user_profile
```

**📚 Detailed Documentation:**
For comprehensive setup, configuration, and troubleshooting information, see [docs/EPOCH_ORCHESTRATOR.md](docs/EPOCH_ORCHESTRATOR.md)

## Docker Services

## Kubernetes Deployment

The Kubernetes deployment includes all services and consumers with **centralized configuration management** via ConfigMap.

### Configuration Management

All environment variables are managed through the `ipfs-validator-config` ConfigMap for consistency across all deployments.

**Quick Setup:**
```bash
# 1. Update ConfigMap with your validator credentials and deploy
./launch_epoch_orchestrator.sh --update-configmap \
                               --validator-account "YOUR_VALIDATOR_ACCOUNT_ID" \
                               --validator-seed "your twelve word seed phrase here" \
                               --apply-k8s

# 2. Deploy remaining services (if not already deployed)
kubectl apply -f k8s/

# 3. Monitor the epoch orchestrator
kubectl logs -f deployment/epoch-orchestrator
```

**Manual Configuration:**
```bash
# Edit the ConfigMap directly
nano k8s/configmap.yaml

# Update these required values:
# VALIDATOR_ACCOUNT_ID: "your_actual_validator_account_id"
# VALIDATOR_SEED: "your_actual_seed_phrase"  # Uncomment this line

# Apply the changes
kubectl apply -f k8s/configmap.yaml
kubectl rollout restart deployment/epoch-orchestrator
```

**ConfigMap Management via Launch Script:**
```bash
# Update ConfigMap only (no deployment)
./launch_epoch_orchestrator.sh --update-configmap \
                               --validator-account "YOUR_VALIDATOR_ACCOUNT_ID" \
                               --validator-seed "your seed phrase"

# Update and apply to Kubernetes
./launch_epoch_orchestrator.sh --update-configmap \
                               --validator-account "YOUR_VALIDATOR_ACCOUNT_ID" \
                               --apply-k8s

# Check current status
./launch_epoch_orchestrator.sh --status
```

**Services:**
- `postgres`: PostgreSQL database
- `rabbitmq`: Message queue with management UI
- `ipfs`: IPFS node
- `dbmate`: Database migration tool

**Consumers (all running automatically):**
- `user-profile-consumer`: Processes user profiles from IPFS
- `pinning-request-consumer`: Processes pinning requests
- `pinning-file-consumer`: Processes individual files from pinning requests
- `file-assignment-consumer`: Processes file assignments to miners
- `node-metrics-consumer`: Processes node metrics from the queue
- `miner-profile-reconstruction-consumer`: Reconstructs and publishes miner profiles
- `user-profile-reconstruction-consumer`: Reconstructs and publishes user profiles
- `miner-health-consumer`: Processes miner health checks and removes failed miners

**Processors (run as jobs when needed):**
- `miner-profile-reconstruction-processor`: Queues miners for profile reconstruction
- `user-profile-reconstruction-processor`: Queues users for profile reconstruction
- `pinning-file-processor`: Parses pinning request files and queues individual files
- `file-assignment-processor`: Assigns files to miners with capacity checking
- `miner-health-processor`: Queues miners for health checks

**Running Processor Jobs:**
```bash
# Run miner profile reconstruction processor
kubectl apply -f k8s/miner-profile-reconstruction-job.yaml

# Run user profile reconstruction processor  
kubectl apply -f k8s/user-profile-reconstruction-job.yaml

# Run pinning file processor
kubectl apply -f k8s/pinning-file-processor-job.yaml

# Run file assignment processor
kubectl apply -f k8s/file-assignment-processor-job.yaml

# Run miner health processor
kubectl apply -f k8s/miner-health-processor-job.yaml
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
   - `status`: Processing status ('pending', 'processed', 'assigned', 'failed')
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
# - file_assignment_processing
# - node_metrics_latest

# Example: Check file assignment processing queue
python rabbitmq/inspect_queue.py file_assignment_processing
```

### Database Monitoring

```bash
# Check processed files in pending_assignment_file table
psql $DATABASE_URL -c "SELECT cid, owner, filename, file_size_bytes, status FROM pending_assignment_file ORDER BY created_at DESC LIMIT 10;"

# Check processing statistics
psql $DATABASE_URL -c "SELECT status, COUNT(*) FROM pending_assignment_file GROUP BY status;"

# Check file assignments
psql $DATABASE_URL -c "SELECT COUNT(*) as total_assignments, COUNT(DISTINCT owner) as unique_owners FROM file_assignments;"
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

### File Assignment Monitoring

```bash
# Monitor file assignment system
python scripts/query_file_assignments.py

# Check pending files only
python scripts/query_file_assignments.py --pending

# Analyze miner distribution
python scripts/query_file_assignments.py --distribution

# Check new miner assignments
python scripts/query_file_assignments.py --new-miners

# Analyze capacity usage
python scripts/query_file_assignments.py --capacity
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
5. **File assignment issues**: Check miner capacity and health scores using the monitoring scripts

## License

MIT License - see LICENSE file for details

**Environment Variables in ConfigMap:**

The ConfigMap includes all necessary environment variables for the entire system:

- **Database**: `DATABASE_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- **RabbitMQ**: `RABBITMQ_URL`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`
- **IPFS**: `IPFS_NODE_URL`, `IPFS_GATEWAY_URL`, `REMOTE_IPFS_URL`
- **Blockchain**: `NODE_URL`
- **Validator**: `VALIDATOR_ACCOUNT_ID`, `VALIDATOR_SEED` (optional)
- **Orchestrator**: `BLOCK_CHECK_INTERVAL`, `QUEUE_CHECK_TIMEOUT`
- **Health Checks**: `HEALTH_CHECK_FILES_PER_MINER`, `PING_FAILURE_THRESHOLD`, etc.
- **File Assignment**: `REPLICAS_PER_FILE`, `MIN_MINER_HEALTH_SCORE`, etc.
- **Processing**: `MINER_PROFILE_BATCH_SIZE`, `USER_PROFILE_BATCH_SIZE`, etc.

**Services:**

1) get all the node metrics
DATABASE_URL=postgresql://user:password@localhost:5432/substrate_fetcher IPFS_NODE_URL=http://localhost:5001 python rabbitmq/node_metrics_processor.py 

2) get all the registred miners
DATABASE_URL=postgresql://user:password@localhost:54180/substrate_fetcher IPFS_NODE_URL=http://localhost:5001 python rabbitmq/registration_processor.py

3) profiles
DATABASE_URL=postgresql://user:password@localhost:54180/substrate_fetcher IPFS_NODE_URL=http://localhost:5001 python rabbitmq/registration_processor.py

4) pin / pinning requests
DATABASE_URL=postgresql://user:password@localhost:54180/substrate_fetcher IPFS_NODE_URL=http://localhost:5001 python rabbitmq/pinning_request_processor.py

5) pinning file processing
DATABASE_URL=postgresql://user:password@localhost:54180/substrate_fetcher IPFS_NODE_URL=http://localhost:5001 python rabbitmq/pinning_file_processor.py

6) miner health checks
DATABASE_URL=postgresql://user:password@localhost:54180/substrate_fetcher IPFS_NODE_URL=http://localhost:5001 python rabbitmq/miner_health_processor.py