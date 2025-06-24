# IPFS Service Validator

An IPFS Service Validator for Substrate-based blockchain networks (specifically the Bittensor/Hippius Network).

## Overview

This service acts as a validator that monitors IPFS storage providers, operates on an epoch-based system, performs health checks and content verification of IPFS nodes, processes and assigns storage requests to miners, and submits validator findings back to the blockchain.

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd ipfs-service-validator

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install in development mode
pip install -e .
```

## Running

```bash
# Start the validator
python main.py
```

## Configuration

Configure through environment variables:

- `IPFS_NODE_URL`: URL for the IPFS node (default: http://localhost:5001)
- `POSTGRES_HOST`: PostgreSQL host (default: localhost)
- `POSTGRES_PORT`: PostgreSQL port (default: 5432)
- `POSTGRES_USER`: PostgreSQL username (default: user)
- `POSTGRES_PASSWORD`: PostgreSQL password (default: password)
- `POSTGRES_DB`: PostgreSQL database name (default: substrate_fetcher)
- `KEYSTORE_PATH`: Path to validator keypair for blockchain operations
- `VALIDATOR_ACCOUNT_ID`: Blockchain account ID for the validator
- `EPOCH_BLOCK_INTERVAL`: Number of blocks per epoch (default: 100)

## Development

See [CLAUDE.md](./CLAUDE.md) for detailed development guidelines and architecture information.