# Substrate Storage Fetcher

A Python-based service that fetches and indexes storage items from a Substrate-based blockchain (e.g., Hippius Network) and stores them in a PostgreSQL database. It includes features for handling IPFS content and maintaining state across finalized blocks.

## Features

- Real-time subscription to finalized block headers
- Configurable storage item and map fetching
- PostgreSQL integration for persistent storage
- IPFS content fetching support
- Automatic reconnection handling
- Error resilience and graceful shutdown

## Prerequisites

- Python 3.8 or newer
- Docker (for PostgreSQL)
- IPFS Node (optional, for CID content fetching)
- Access to a Substrate node (e.g., `wss://rpc.hippius.network`)

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/substrate-storage-fetcher.git
cd substrate-storage-fetcher
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. PostgreSQL Setup with Docker

```bash
docker run -d --name postgres-substrate --restart always \
  -e POSTGRES_USER=user \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=substrate_fetcher \
  -p 5432:5432 \
  postgres:17
```

Verify the container is running:
```bash
docker ps
```

### 5. Configuration

Edit `substrate_fetcher/config.py` to set:

- **Node Connection:**
  ```python
  NODE_URL = "wss://rpc.hippius.network"  # Your Substrate node WebSocket URL
  ```

- **Storage Items to Fetch:**
  ```python
  STORAGE_ITEMS_TO_FETCH = [
      ("Timestamp", "Now"),  # Simple storage item
      ("System", "Account", ["5GrwvaEF..."]),  # Storage map with key
  ]
  ```

- **Storage Maps to Fetch:**
  ```python
  STORAGE_MAPS_TO_FETCH_ALL = [
      ("Registration", "NodeRegistration"),
      ("Registration", "ColdkeyNodeRegistration"),
  ]
  ```

- **Database Settings:**
  ```python
  POSTGRES_USER = "user"
  POSTGRES_PASSWORD = "password"
  POSTGRES_DB = "substrate_fetcher"
  POSTGRES_HOST = "localhost"
  POSTGRES_PORT = "5432"
  ```

## Running the Application

1. **Ensure PostgreSQL Container is Running:**
   ```bash
   docker start postgres-substrate  # If not already running
   ```

2. **Start the Fetcher:**
   ```bash
   python run_fetcher.py
   ```

The application will:
- Connect to the Substrate node
- Initialize the PostgreSQL database
- Subscribe to finalized blocks
- Begin fetching and storing data

## Database Schema

### Miners Table
```sql
CREATE TABLE miners (
    node_id VARCHAR PRIMARY KEY,
    ipfs_storage_max BIGINT,
    ipfs_zfs_pool_size BIGINT,
    last_online_block INTEGER,
    miner_profile_cid VARCHAR,
    updated_at TIMESTAMP
);
```

### Registration Table
```sql
CREATE TABLE registration (
    node_id VARCHAR PRIMARY KEY,
    ipfs_node_id VARCHAR,
    node_type VARCHAR,
    owner VARCHAR,
    registered_at INTEGER,
    status VARCHAR,
    updated_at TIMESTAMP
);
```

## Project Structure

- `run_fetcher.py` - Main entry point
- `substrate_fetcher/`
  - `config.py` - Configuration settings
  - `storage_fetcher.py` - Core fetching logic
  - `main.py` - Application initialization
  - `utils.py` - Helper functions

## Error Handling

The service includes:
- Automatic reconnection to the Substrate node
- Transaction rollback on database errors
- Graceful shutdown on CTRL+C
- Detailed error logging

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.