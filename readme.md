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

# Node Metrics (optional)
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

### 3. Pinning Requests

Handles storage pinning requests:

```bash
# Process pinning requests
python rabbitmq/pinning_request_processor.py

# Start consumer
python rabbitmq/pinning_request_consumer.py
```

### 4. Node Metrics

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

## Docker Services

The `docker-compose.yml` includes:

- `postgres`: PostgreSQL database
- `rabbitmq`: Message queue with management UI (http://localhost:15672)
- `ipfs`: IPFS node
- `dbmate`: Database migration tool
- `user-profile-consumer`: Processes user profiles
- `pinning-request-consumer`: Processes pinning requests
- `node-metrics-consumer`: Processes node metrics from the queue

**Note**: The miner-profile-consumer is not included in docker-compose.yml by default. Run it manually with:
```bash
python rabbitmq/miner_profile_consumer.py
```

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
# - pinning_request
# - node_metrics_latest
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
