import os
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file into environment variables

# Substrate Node Configuration
SUBSTRATE_NODE_URL = os.getenv("SUBSTRATE_NODE_URL", "ws://127.0.0.1:9944")

# PostgreSQL Database Configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ipfs_substrate_indexer")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Construct Database URL from individual components
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# IPFS Gateway Configuration
IPFS_GATEWAY_URL = os.getenv("IPFS_GATEWAY_URL", "https://ipfs.io") # Default to a public gateway

# Other configurations
ECHO_SQL = os.getenv("ECHO_SQL", "False").lower() == "true"
POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL_SECONDS", "12")) 