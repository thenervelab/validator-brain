# substrate_fetcher/config.py
import os

# --- Connection Configuration ---
NODE_URL = "wss://rpc.hippius.network"  # CHANGE THIS TO YOUR NODE

# --- Storage Items Configuration ---
# For single storage items or specific keys in a map
# Format: (PalletName, StorageItemName, [optional_params_if_map_key])
STORAGE_ITEMS_TO_FETCH = [
    # ("Timestamp", "Now"), # Example: a simple non-map item
]

# For fetching all entries of a StorageMap
# Format: (PalletName, StorageItemName)
STORAGE_MAPS_TO_FETCH_ALL = [
    ("Registration", "NodeRegistration"),
    ("Registration", "ColdkeyNodeRegistration"),
    ("ExecutionUnit", "NodeMetrics"),
    ("ExecutionUnit", "BlockNumbers"),
    ("IpfsPallet", "MinerProfile"),
    ("IpfsPallet", "UserProfile"),
    # Add other maps you want to fetch entirely
]

# --- Application Settings ---
SUBSCRIPTION_RETRY_DELAY = 10  # seconds
MAIN_LOOP_SLEEP_INTERVAL = 5  # seconds for main.py's loop
OUTPUT_JSON_FILE = "latest_storage_state.json"  # File to save the JSON output (unused now)

# --- IPFS Configuration ---
IPFS_NODE_URL = "http://localhost:5001"  # IPFS node URL for fetching CID content
IPFS_GATEWAY_URL = "http://localhost:8080"  # Adjust as needed
IPFS_TIMEOUT_SECONDS = 10

# --- PostgreSQL Database Configuration ---
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "substrate_fetcher")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Construct Database URL from individual components
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Global database pool (to be initialized in main.py)
db_pool = None