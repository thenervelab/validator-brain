# IPFS Service Validator

An IPFS Service Validator for Substrate-based blockchain networks (specifically the
Bittensor/Hippius Network). The validator monitors IPFS storage providers, performs health checks,
processes storage requests, and submits findings back to the blockchain using an epoch-based system.

## Overview

This service operates as a distributed validator system with the following core components:

- **Epoch Orchestrator**: Main coordination system that manages validator activities based on
  blockchain epochs
- **RabbitMQ Consumer/Processor Pairs**: Event-driven processing for different data types (file
  assignments, health monitoring, profile management, etc.)
- **Substrate Fetcher**: Blockchain interaction layer for fetching data and submitting transactions
- **Database Layer**: PostgreSQL-based persistence with automated migrations
- **IPFS Integration**: Content verification, health checking, and file management

## Quick Start with Docker

The system is designed to run with Docker Compose using a modular approach with different service
profiles.

### Prerequisites

1. Docker and Docker Compose installed
2. Git repository cloned
3. Environment variables configured (see [Environment Configuration](#environment-configuration))

### Running the Full System

```bash
# Start infrastructure services (PostgreSQL, RabbitMQ, IPFS)
docker compose up -d

# Start all consumer services
docker compose --profile consumers up -d --build

# Run epoch orchestrator locally (recommended for development)
python epoch_orchestrator.py
```

### Running Individual Components

```bash
# Start only infrastructure
docker compose up postgres rabbitmq ipfs -d

# Start specific consumers
docker compose --profile consumers up pinning-request-consumer -d --build
docker compose --profile consumers up miner-health-consumer -d --build

# View logs for specific services
docker compose logs -f pinning-request-consumer
```

### Available Consumer Services

The system includes the following consumer/processor pairs:

- `pinning-request-consumer` - Processes storage requests and assigns files to miners
- `miner-health-consumer` - Monitors IPFS node health and availability
- `file-assignment-consumer` - Handles file assignment operations
- `registration-consumer` - Processes miner registration events
- `unpin-request-consumer` - Handles file unpinning requests
- `user-profile-consumer` - Manages user profile updates
- `miner-profile-reconstruction-consumer` - Rebuilds miner profiles
- `user-profile-reconstruction-consumer` - Rebuilds user profiles
- `network-self-healing-consumer` - Handles automatic file reassignments
- `node-metrics-consumer` - Collects and processes node metrics
- `epoch-health-consumer` - Performs epoch-based health checks

## Environment Configuration

Copy the example environment file and configure it for your setup:

```bash
cp environment.example .env
```

### Required Variables

Edit your `.env` file with the following required settings:

```bash
# Validator account ID (REQUIRED)
export VALIDATOR_ACCOUNT_ID="your_validator_account_id_here"

# Blockchain RPC endpoint (REQUIRED)
export NODE_URL="wss://your_url"

# Database connection (REQUIRED)
export DATABASE_URL="postgresql://user:password@localhost:5432/substrate_fetcher"

# RabbitMQ connection (REQUIRED)
export RABBITMQ_URL="amqp://admin:admin@localhost:5672/"

# IPFS configuration
export IPFS_NODE_URL="http://localhost:5001"
```

### Optional Configuration

The system includes many optional configuration parameters for fine-tuning:

- **Health Check Settings**: Timeouts, failure thresholds, files to test per miner
- **File Assignment**: Replica counts, batch sizes, minimum health scores
- **Processing Limits**: Batch sizes for profile processing
- **Timing Configuration**: Block check intervals and queue timeouts

See `environment.example` for complete configuration options with detailed comments.

## Running the Epoch Orchestrator

The epoch orchestrator is the main coordination component and is typically run locally for easier
debugging:

```bash
# Source your environment
source .env

# Run the orchestrator
python epoch_orchestrator.py

# Or run with specific options
python epoch_orchestrator.py --check-only    # Check connectivity only
python epoch_orchestrator.py --status        # Show current status
```

## Development Setup

### Local Development

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install in development mode
pip install -e .

# Run database migrations
docker compose up postgres -d
dbmate up

# Start RabbitMQ and IPFS
docker compose up rabbitmq ipfs -d

# Run individual components
python rabbitmq/pinning_request_consumer.py
python rabbitmq/miner_health_processor.py
```

### Code Quality

```bash
# Run linting and formatting
ruff check .
ruff format .
```

### Database Management

```bash
# Run migrations
dbmate up

# Create new migration
dbmate new migration_name

# View migration status
dbmate status
```

## Architecture Details

### System Architecture

```
┌───────────────────────────────────────────────────────────────────────────────── ┐
│                           IPFS Service Validator System                          │
├───────────────────────────────────────────────────────────────────────────────── ┤
│                                                                                  │
│  ┌─────────────────────┐       ┌──────────────────────────────────────────────┐  │
│  │   Epoch             │◄──────┤              Infrastructure                  │  │
│  │   Orchestrator      │       │                                              │  │
│  │   (Python Process)  │       │  ┌─────────────┐ ┌─────────────┐ ┌─────────┐ │  │
│  └─────────┬───────────┘       │  │ PostgreSQL  │ │  RabbitMQ   │ │  IPFS   │ │  │
│            │                   │  │ Database    │ │  Message    │ │  Node   │ │  │
│            │ Calls Processors  │  │ Container   │ │  Broker     │ │ Container │  │
│            │                   │  └─────────────┘ └─────────────┘ └─────────┘ │  │
│            ▼                   └──────────────────────────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                        RabbitMQ Consumer Services                           │ │
│  │                                                                             │ │
│  │ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────┐ │ │
│  │ │ pinning-request │ │ miner-health    │ │ file-assignment │ │ registration│ │ │
│  │ │ consumer        │ │ consumer        │ │ consumer        │ │ consumer    │ │ │
│  │ └─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────┘ │ │
│  │                                                                             │ │
│  │ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────┐ │ │
│  │ │ unpin-request   │ │ user-profile    │ │ miner-profile   │ │ node-metrics│ │ │
│  │ │ consumer        │ │ consumer        │ │ reconstruction  │ │ consumer    │ │ │
│  │ └─────────────────┘ └─────────────────┘ └─────────────────┘ └─────────────┘ │ │
│  │                                                                             │ │
│  │ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐                 │ │
│  │ │ user-profile    │ │ network-self    │ │ epoch-health    │                 │ │
│  │ │ reconstruction  │ │ healing         │ │ consumer        │                 │ │
│  │ └─────────────────┘ └─────────────────┘ └─────────────────┘                 │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
│  Data Flow:                                                                      │
│  Blockchain Events ──► RabbitMQ Queues ──► Consumer Services ──► PostgreSQL      │
│                                                                                  │
│  Epoch Orchestrator ──► Processor Functions ──► Prepare Queues ──► Wait Empty    
│                                                                                  │
└───────────────────────────────────────────────────────────────────────────────── ┘
```

### Validator Workflow (Epoch Orchestrator)

The epoch orchestrator executes a structured workflow based on blockchain epochs (100 blocks each).
Here's the complete validator workflow:

#### Phase 1: Initialization (Blocks 0-10)

1. **Connect to Blockchain**: Establish WebSocket connection to Substrate node
2. **Database Connection**: Initialize PostgreSQL connection pool
3. **Validator Status Check**: Determine if this node is the current epoch validator
4. **Queue Status Assessment**: Check RabbitMQ queue lengths and health
5. **State Recovery**: Load previous epoch state if recovering from connection failure

#### Phase 2: Data Collection & Health Monitoring (Blocks 11-35)

6. **Fetch Blockchain Data**: Call `validator_workflow.fetch_and_store_blockchain_data()`
7. **Process Registration Events**:
   Execute `registration_processor.process_all_registration_messages()`
8. **Health Check Execution**: Run `health_checker.check_miner_health()` for all miners
9. **Node Metrics Collection**: Execute `node_metrics_processor.process_all_node_metrics()`
10. **Network Self-Healing**: Run `network_self_healing_processor.process_all_messages()`
11. **Health Score Processing**: Execute `health_score_processor.process_health_scores()`

#### Phase 3: File Assignment (Blocks 36-60)

12. **Process Pinning Requests**: Execute `pinning_request_processor.process_all_pinning_requests()`
13. **File Assignment Logic**: Run `file_assignment_processor.process_all_file_assignments()`
14. **Miner Selection**: Use health scores to select optimal miners for new files
15. **Assignment Verification**: Verify file assignments were created successfully
16. **Queue Monitoring**: Wait for assignment queues to empty before proceeding

#### Phase 4: Profile Reconstruction (Blocks 61-75)

17. **Miner Profile Updates**:
    Execute `miner_profile_reconstruction_processor.process_all_messages()`
18. **User Profile Updates**: Execute `user_profile_reconstruction_processor.process_all_messages()`
19. **Profile Data Collection**: Call `collect_miner_profiles_for_submission()`
20. **Profile Verification**: Ensure profiles contain valid data before submission
21. **IPFS Profile Upload**: Upload profile JSON data to IPFS for blockchain submission

#### Phase 5: Blockchain Submission (Blocks 76-90)

22. **Collect Storage Requests**: Execute `collect_storage_requests_for_submission()`
23. **Submit Health Metrics**: Call `submit_health_metrics_to_blockchain()`
24. **Submit Unpin Requests**: Execute `submit_unpin_requests_to_blockchain()`
25. **Update Pin Requests**: Call `call_update_pin_and_storage_requests()`
26. **Mark Submissions Complete**: Execute `mark_submissions_as_completed()`
27. **Transaction Verification**: Verify blockchain transactions were successful

#### Phase 6: Cleanup & Monitoring (Blocks 91-99)

28. **Process Unpin Requests**: Execute `unpin_request_processor.process_all_unpin_requests()`
29. **Availability Management**: Run `availability_manager_processor.process_availability_updates()`
30. **Epoch Summary**: Log epoch completion statistics and performance metrics
31. **State Reset**: Reset phase completion flags for next epoch
32. **Queue Cleanup**: Clear any remaining messages from epoch-specific queues

### Consumer Services Detail

Each consumer service operates independently and processes specific blockchain events:

- **pinning-request-consumer**: Listens for new storage requests from blockchain
- **miner-health-consumer**: Monitors IPFS node health and connectivity
- **file-assignment-consumer**: Processes file-to-miner assignment operations
- **registration-consumer**: Handles new miner registration events
- **unpin-request-consumer**: Processes file unpinning requests
- **user-profile-consumer**: Updates user profile information
- **miner-profile-reconstruction-consumer**: Rebuilds miner profiles from assignments
- **user-profile-reconstruction-consumer**: Rebuilds user profiles from storage data
- **network-self-healing-consumer**: Handles automatic file reassignments
- **node-metrics-consumer**: Collects and processes node performance metrics
- **epoch-health-consumer**: Performs periodic health assessments

### Epoch-Based Operations

The validator operates on blockchain epochs with different phases:

### RabbitMQ Event Processing

The system uses RabbitMQ queues for asynchronous processing:

- Each consumer reads from specific blockchain events
- Processors handle the business logic and database updates
- Automatic retry and error handling for failed operations
- Configurable batch processing for performance optimization

### Database Schema

Key database tables:

- `files` and `file_assignments`: Core file storage tracking
- `storage_requests`: Incoming storage requests from blockchain
- `miner_profile` and `user_profile`: Network participant profiles
- `processed_unpin_requests`: Unpin operation tracking
- `file_failures` and `miner_availability`: Health monitoring data
- `pending_*` tables: Staging areas for blockchain submissions

## Monitoring and Troubleshooting

### Viewing Logs

```bash
# Docker Compose logs
docker compose logs -f [service-name]

# Kubernetes logs
kubectl logs -f deployment/[deployment-name] -n [namespace]

# View specific consumer logs
docker compose logs -f pinning-request-consumer
```

### Health Checks

The system includes built-in health checks for all infrastructure components:

- PostgreSQL: Connection and query testing
- RabbitMQ: Queue availability and message processing
- IPFS: Node connectivity and content retrieval
- Blockchain: WebSocket connection and epoch tracking

### Common Issues

1. **Database Connection Issues**: Check PostgreSQL is running and DATABASE_URL is correct
2. **RabbitMQ Queue Backlogs**: Monitor queue sizes in RabbitMQ management
   UI (http://localhost:15672)
3. **IPFS Connectivity**: Verify IPFS node is accessible at configured URL
4. **Blockchain Sync Issues**: Check NODE_URL WebSocket connection and validator account

## License

See [LICENSE](./LICENSE) for license information.