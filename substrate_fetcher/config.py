# substrate_fetcher/config.py
import os

# --- Connection Configuration ---
NODE_URL = "ws://127.0.0.1:9944"

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
        "BoundedVec<u8, ConstU32<64>>": "Vec<u8>",
        "BlockNumbers": "Vec<u32>",
        "MinerProfile": "BoundedVec<u8, ConstU32<64>>",
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
        "Option<StorageRequest<AccountId, BlockNumber>>": "Option<StorageRequest<AccountId32, u32>>",
        "NodeMetricsData": {
            "type": "struct",
            "type_mapping": [
                ["miner_id", "Vec<u8>"],
                ["bandwidth_mbps", "u32"],
                ["current_storage_bytes", "u64"],
                ["total_storage_bytes", "u64"],
                ["geolocation", "Vec<u8>"],
                ["successful_pin_checks", "u32"],
                ["total_pin_checks", "u32"],
                ["storage_proof_time_ms", "u32"],
                ["storage_growth_rate", "u32"],
                ["latency_ms", "u32"],
                ["total_latency_ms", "u32"],
                ["total_times_latency_checked", "u32"],
                ["avg_response_time_ms", "u32"],
                ["peer_count", "u32"],
                ["failed_challenges_count", "u32"],
                ["successful_challenges", "u32"],
                ["total_challenges", "u32"],
                ["uptime_minutes", "u32"],
                ["total_minutes", "u32"],
                ["consecutive_reliable_days", "u32"],
                ["recent_downtime_hours", "u32"],
                ["is_sev_enabled", "bool"],
                ["zfs_info", "Vec<Vec<u8>>"],
                ["ipfs_zfs_pool_size", "u128"],
                ["ipfs_zfs_pool_alloc", "u128"],
                ["ipfs_zfs_pool_free", "u128"],
                ["raid_info", "Vec<Vec<u8>>"],
                ["vm_count", "u32"],
                ["primary_network_interface", "Option<NetworkInterfaceInfo>"],
                ["disks", "Vec<DiskInfo>"],
                ["ipfs_repo_size", "u64"],
                ["ipfs_storage_max", "u64"],
                ["cpu_model", "Vec<u8>"],
                ["cpu_cores", "u32"],
                ["memory_mb", "u64"],
                ["free_memory_mb", "u64"],
                ["gpu_name", "Option<Vec<u8>>"],
                ["gpu_memory_mb", "Option<u32>"],
                ["hypervisor_disk_type", "Option<Vec<u8>>"],
                ["vm_pool_disk_type", "Option<Vec<u8>>"],
                ["disk_info", "Vec<DiskDetails>"]
            ]
        }
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
    ("IpfsPallet", "UserStorageRequests"),("IpfsPallet", "MinerTotalFilesSize"),
    ("IpfsPallet", "MinerTotalFilesPinned"),

]
# --- Application Settings ---
SUBSCRIPTION_RETRY_DELAY = 10  # seconds
MAIN_LOOP_SLEEP_INTERVAL = 5  # seconds for main.py's loop
OUTPUT_JSON_FILE = "latest_storage_state.json"  # File to save the JSON output (unused now)

# --- IPFS Configuration ---
IPFS_NODE_URL = "http://localhost:5001"  # IPFS node URL for fetching CID content
IPFS_GATEWAY_URL = "http://localhost:8080"  # Adjust as needed
IPFS_TIMEOUT_SECONDS = 10
IPFS_REFS_TIMEOUT_SECONDS = 30 # Timeout for 'ipfs refs' command
IPFS_DHT_TIMEOUT_SECONDS = 60  # Timeout for 'ipfs dht findprovs' command
IPFS_FETCH_TIMEOUT = 60  # Timeout for IPFS fetch in seconds (used by ipfs_fetch_worker)

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