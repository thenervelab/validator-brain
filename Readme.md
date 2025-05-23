# IPFS Service

IPFS Service is a Rust application that connects to a Substrate blockchain and manages IPFS storage operations for the Hippius Subnet.

## Overview

This service handles:
- File storage requests and pinning to IPFS nodes
- Node metrics collection and reporting
- Miner state management
- User and miner profile management
- Epoch-based validation and verification

## Configuration

The application is configured via environment variables:

### Substrate/Chain Settings

| Environment Variable | Description | Default Value |
|----------------------|-------------|---------------|
| HIPPIUS_RPC_URL | Substrate node WebSocket URL | ws://127.0.0.1:9944 |
| HIPPIUS_KEYSTORE_PATH | Path to the keystore containing validator keys | /opt/hippius/data/chains/hippius-mainnet/keystore/ |
| HIPPIUS_EPOCH_LENGTH | Length of an epoch in blocks | 100 |
| HIPPIUS_EPOCH_CLEANUP_BLOCK | Block within epoch to clean up profiles | 98 |
| HIPPIUS_VALIDATOR_ADDRESS | Optional validator address override (SS58 format) | None |

### API Settings

| Environment Variable | Description | Default Value |
|----------------------|-------------|---------------|
| HIPPIUS_API_PORT | HTTP API port | 3000 |

### PostgreSQL Settings

| Environment Variable | Description | Default Value |
|----------------------|-------------|---------------|
| POSTGRES_HOST | PostgreSQL host | localhost |
| POSTGRES_PORT | PostgreSQL port | 5432 |
| POSTGRES_USER | PostgreSQL username | postgres |
| POSTGRES_PASSWORD | PostgreSQL password | password |
| POSTGRES_DB | PostgreSQL database name | hippius |
| POSTGRES_MAX_CONNECTIONS | Maximum database connections | 5 |

### IPFS Settings

| Environment Variable | Description | Default Value |
|----------------------|-------------|---------------|
| IPFS_NODE_URL | IPFS node URL for storage operations | https://store.hippius.network |
| IPFS_NETWORK_URL | IPFS network URL | get.hippius.network |

## Running the Application

### Prerequisites

- Rust toolchain (install via [rustup](https://rustup.rs/))
- PostgreSQL database
- IPFS node
- Access to a Substrate node

### Quick Start

1. Clone the repository:
   ```
   git clone https://github.com/dubs-network/ipfs-service.git
   cd ipfs-service
   ```

2. Build the application:
   ```
   cargo build --release
   ```

3. Set up environment variables:
   ```bash
   export HIPPIUS_RPC_URL="ws://127.0.0.1:9944"
   export POSTGRES_HOST="localhost"
   export POSTGRES_PORT="5432"
   export POSTGRES_USER="postgres"
   export POSTGRES_PASSWORD="password"
   export POSTGRES_DB="hippius"
   export POSTGRES_MAX_CONNECTIONS="5"
   export IPFS_NODE_URL="http://127.0.0.1:5001"
   export IPFS_NETWORK_URL="get.hippius.network"
   export HIPPIUS_EPOCH_LENGTH="100"
   export HIPPIUS_EPOCH_CLEANUP_BLOCK="98"
   export HIPPIUS_API_PORT="3001"
   # Optional: Set validator address for testing
   export HIPPIUS_VALIDATOR_ADDRESS="SS58 Vali ADdress so we don't get from keystore"
   ```

4. Run the application:
   ```
   cargo run --release
   ```

## Database Schema

The application automatically creates the following tables in the PostgreSQL database:

- `blockchain.ipfs_nodemetrics`: Stores node metrics data
- `blockchain.ipfs_registration`: Stores node registration data
- `blockchain.registration`: Stores general registration data
- `blockchain.ipfs_minerprofile`: Stores miner profiles
- `blockchain.ipfs_userprofile`: Stores user profiles
- `blockchain.epoch_miners`: Tracks miners in the current epoch
- Various other tables for handling storage requests and state

## Validator Operation

The service operates in a validator role when:

1. The local validator matches the current epoch validator from the chain
2. The current block is within the current validator's epoch

During each epoch:
- At the start of the epoch: All profile tables are refreshed (user profiles, miner profiles, and epoch miners)
- At the cleanup block (default: block 98 of the epoch): All profile tables are cleared to prepare for the next epoch

## API Endpoints

The service exposes the following HTTP endpoints:

- GET `/api/ipfs/:table`: Get all entries from a specific table
- GET `/api/ipfs/:table/:storage_key`: Get entry by key from a specific table
- GET `/api/ipfs/pin-profiles`: Pin all user and miner profiles to IPFS
- GET `/api/ipfs/file-size/:file_hash`: Get the size of a file by hash
- GET `/api/ipfs/cid-info/:file_hash`: Get information about a specific CID
- GET `/api/ipfs/cid-info`: Get information about all CIDs

## Development and Testing

For development and testing purposes, you can set `HIPPIUS_VALIDATOR_ADDRESS` to a specific account address to force the service to act as if that validator is the current epoch validator.



docker run -d --name postgres-substrate \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=hippius \
  -p 5432:5432 \
  postgres:17
