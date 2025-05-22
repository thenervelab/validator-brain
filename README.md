# IPFS Service Validator

A validator service for monitoring IPFS storage providers on a Substrate-based blockchain network (specifically the Hippius Network). 

## Overview

This service:

1. Acts as a validator that monitors IPFS storage providers
2. Operates on an epoch-based system (100 blocks per epoch)
3. Performs health checks and content verification of IPFS nodes
4. Processes and assigns storage requests to miners
5. Submits validator findings back to the blockchain

## Architecture

### Core Components

1. **Storage Fetcher**: Connects to Substrate node via WebSocket, monitors finalized blocks
2. **IPFS Health Service**: Runs health checks on IPFS nodes at epoch transitions
3. **CID Checker**: Verifies content availability through multi-stage checks
4. **Epoch Validator**: Monitors blocks to detect when node becomes validator
5. **IPFS Utilities**: Handles IPFS content operations
6. **Substrate Utilities**: Loads validator keypair for blockchain operations

## Setup

### Prerequisites

- Python 3.9+
- PostgreSQL database
- IPFS node
- Substrate node with WebSocket access

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ipfs-service-validator.git
   cd ipfs-service-validator
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Copy the example environment file and configure it:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. Run database migrations:
   ```bash
   # Install dbmate (if not using Docker)
   go install github.com/amacneil/dbmate/v2@v2.6.0
   
   # Run migrations
   ./scripts/migrate.sh up
   ```

6. Install pre-commit hooks (optional for development):
   ```bash
   pip install pre-commit
   pre-commit install
   ```

### Running with Docker

The easiest way to run the validator is with Docker:

```bash
# Production mode
docker-compose up -d

# Development mode with hot-reloading
docker-compose --profile dev up
```

This will start:
- PostgreSQL database
- IPFS node
- Validator service

In development mode, the app will automatically reload when you make changes to the code.

### Running Locally for Development

For development without Docker, you can run the service with hot-reloading:

```bash
# Make sure PostgreSQL and IPFS are running
./run.py
```

Or using uvicorn directly:

```bash
uvicorn app.main:app --reload
```

## API Endpoints

- `GET /`: Basic service information
- `GET /health`: Health check endpoint
- `GET /metrics`: Basic metrics for monitoring
- `GET /docs`: FastAPI Swagger documentation (development)
- `GET /redoc`: FastAPI ReDoc documentation (development)

## Project Structure

```
app/
├── api/             # API endpoints
├── core/            # Core validator functionality
│   └── validator.py # Main validator loop
├── db/              # Database operations
│   └── queries/     # SQL query files
├── models/          # Data models
├── services/        # Service layer
│   ├── db_manager.py
│   ├── health_checker.py
│   ├── ipfs_api.py
│   ├── profile_manager.py
│   └── storage_processor.py
└── utils/           # Utility functions
    ├── config.py
    ├── http.py
    └── logging.py
db/
├── migrations/      # Database migrations
└── schema.sql       # Database schema file
```

## Development

### Code Style and Quality

This project uses:
- **Ruff**: For linting and formatting (replaces Black, isort, flake8, pylint)
- **MyPy**: For static type checking
- **SQLFluff**: For SQL linting and formatting
- **Pre-commit**: Runs linters and formatters automatically on commit

To run linters manually:

```bash
# Format and lint Python code
ruff check --fix .
ruff format .

# Type checking
mypy .

# SQL formatting
sqlfluff fix --dialect postgres app/db/migrations/
```

### Adding Database Migrations

To create a new migration:

```bash
# Create a new migration file
dbmate new migration_name

# This will create a file like: db/migrations/20250522120000_migration_name.sql
# with the migrate:up and migrate:down sections

# Apply the migration
./scripts/migrate.sh up
```

Migration files follow dbmate's format:

```sql
-- migrate:up
CREATE TABLE example (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL
);

-- migrate:down
DROP TABLE example;
```

### Docker Development Workflow

The development Docker setup includes:

1. Hot-reloading for code changes
2. Automatic database migrations
3. Development tools installed (pytest, ruff, mypy, pre-commit)
4. A separate container to avoid conflicts with production

To use it:

```bash
# Start development environment
docker-compose --profile dev up

# Run tests inside the container
docker-compose exec validator-dev pytest

# Format code inside the container
docker-compose exec validator-dev ruff format .
docker-compose exec validator-dev ruff check --fix .

# Run pre-commit checks manually
docker-compose exec validator-dev pre-commit run --all-files
```

## License

This project is licensed under [MIT License](LICENSE).