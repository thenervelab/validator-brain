# substrate_fetcher/config.py
import os

# --- Connection Configuration ---
NODE_URL = "wss://rpc.dubs.rs"  # CHANGE THIS TO YOUR NODE

TYPE_REGISTRY = {
    "types": {
        "AccountId": "AccountId32",
        "BlockNumber": "u32",
        "<T::AccountId, BlockNumberFor<T>>": "(AccountId32, u32)",
        "Option<(T::AccountId, BlockNumberFor<T>)>": "Option<(AccountId32, u32)>",
        "FileHash": "BoundedVec<u8, 350>",
        "FileName": "BoundedVec<u8, 350>",
        "BoundedVec<u8, 350>": "Vec<u8>",
        "BoundedVec<u8, 64>": "Vec<u8>",
        "StorageRequest<AccountId, BlockNumber>": {
            "type": "struct",
            "type_mapping": [
                ["total_replicas", "u32"],
                ["owner", "AccountId32"],
                ["file_hash", "BoundedVec<u8, 350>"],
                ["file_name", "BoundedVec<u8, 350>"],
                ["last_charged_at", "u32"],
                ["created_at", "u32"],
                ["miner_ids", "Option<BoundedVec<BoundedVec<u8, 64>, 5>>"],
                ["selected_validator", "AccountId32"],
                ["is_assigned", "bool"]
            ]
        },
        "Option<StorageRequest<AccountId, BlockNumber>>": "Option<StorageRequest<AccountId32, u32>>"
    }
}

STORAGE_ITEMS_TO_FETCH = [
    # ("IpfsPallet", "CurrentEpochValidator"),
]

STORAGE_MAPS_TO_FETCH_ALL = [
    ("ExecutionUnit", "NodeMetrics"),
    ("ExecutionUnit", "BlockNumbers"),
    ("IpfsPallet", "MinerProfile"),
    ("IpfsPallet", "UserProfile"),
    ("Registration", "NodeRegistration"),
    ("Registration", "ColdkeyNodeRegistration"),
    ("IpfsPallet", "UserStorageRequests"),
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
POSTGRES_URI = f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
# Global database pool (to be initialized in main.py)
db_pool = None
IPFS_FETCH_TIMEOUT = 60  # Timeout for IPFS fetch in seconds