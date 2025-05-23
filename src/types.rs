use crate::substrate_fetcher::SubstrateFetcher;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use governor::RateLimiter;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use std::sync::Arc;

// Response struct for file size endpoint
#[derive(Serialize)]
pub struct FileSizeResponse {
    pub size: u32,
}

#[derive(Clone)]
pub struct AppState {
    pub session: Arc<PgPool>,
    pub ipfs_entries: Vec<String>,
    pub ipfs_client: Client,
    pub ipfs_node_url: String,
    pub substrate_fetcher: Arc<SubstrateFetcher>,
    pub rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

#[derive(Serialize)]
pub struct Entry {
    pub storage_key: String,
    pub storage_value: String,
}

#[derive(Serialize)]
pub struct PinResponse {
    pub pinned: Vec<String>,
    pub failed: Vec<String>,
}

// #[derive(Deserialize)]
// pub struct MinerCheckRequest {
//     pub miners: Vec<MinerProfile>,
// }

// #[derive(Deserialize)]
// pub struct MinerProfile {
//     #[serde(rename = "MinerProfile")]
//     pub miner_profile: String, // CID of the MinerProfile
//     #[serde(rename = "IpfsId")]
//     pub ipfs_id: String, // Miner node ID
// }

#[derive(Serialize)]
pub struct MinerCheckResponse {
    pub results: Vec<MinerResult>,
}

#[derive(Serialize)]
pub struct MinerResult {
    #[serde(rename = "MinerId")]
    pub miner_id: String, // IpfsId from request
    pub total_count: usize,          // Total CIDs checked
    pub passed_count: usize,         // CIDs pinned by this miner
    pub failed_count: usize,         // CIDs not pinned by this miner
    pub cids_not_found: Vec<String>, // CIDs not pinned by this miner
}

// Response struct for CID info endpoint
#[derive(Serialize)]
pub struct CidInfoResponse {
    pub item_value: String,
}

// Response struct for CID info entry
#[derive(Serialize)]
pub struct CidInfoEntry {
    pub hash_key: String,
    pub item_value: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct UpdatedStorageRequest {
    pub total_replicas: u32,
    pub owner: String,
    pub file_hash: String,
    pub file_name: String,
    pub main_req_hash: String,
    pub last_charged_at: u64,
    pub created_at: u64,
    pub miner_ids: Option<Vec<String>>,
    pub selected_validator: String,
    pub is_assigned: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StorageRequest {
    pub total_replicas: u32,
    pub owner: String,
    pub file_hash: String,
    pub file_name: String,
    pub last_charged_at: u64,
    pub created_at: u64,
    pub miner_ids: Option<Vec<String>>,
    pub selected_validator: String,
    pub is_assigned: bool,
}

// MinerProfileItem for response
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MinerProfileItem {
    pub miner_node_id: String,
    pub cid: String,
    pub files_count: u32,
    pub files_size: u32,
}

// Response struct for the process storage endpoint
#[derive(Serialize)]
pub struct ProcessRequestResponse {
    pub status: String,
    pub message: String,
}

// Response struct for the new endpoint
#[derive(Serialize, Debug)]
pub struct ProcessStorageResponse {
    pub storage_request_owner: String,
    pub storage_request_file_hash: String,
    pub file_size: u32,
    pub user_profile_cid: String,
}

// Add to existing response structs
#[derive(Serialize, Debug)]
pub struct UnpinResponse {
    pub updated_miners: Vec<MinerProfileItem>,
    pub failed_miners: Vec<String>,
}

// // Updated request struct
// #[derive(Deserialize, Clone)]
// pub struct UnpinItem {
//     pub cid: String,
//     pub owner: String,
// }

#[derive(serde::Serialize)]
pub struct VerificationResult {
    pub miner_id: String,
    pub total_checks: usize,
    pub passed_checks: usize,
}

// Define struct for decoding UserUnpinRequest
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserUnpinRequest {
    pub owner: String,
    pub file_hash: String,
    pub selected_validator: String,
}

// Define wrapper structs to deserialize NodeMetricsData JSON
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeMetricsDataWrapper {
    pub miner_id: String,
    pub bandwidth_mbps: u32,
    pub current_storage_bytes: u64,
    pub total_storage_bytes: u64,
    pub geolocation: String,
    pub successful_pin_checks: u32,
    pub total_pin_checks: u32,
    pub storage_proof_time_ms: u32,
    pub storage_growth_rate: u32,
    pub latency_ms: u32,
    pub total_latency_ms: u32,
    pub total_times_latency_checked: u32,
    pub avg_response_time_ms: u32,
    pub peer_count: u32,
    pub failed_challenges_count: u32,
    pub successful_challenges: u32,
    pub total_challenges: u32,
    pub uptime_minutes: u32,
    pub total_minutes: u32,
    pub consecutive_reliable_days: u32,
    pub recent_downtime_hours: u32,
    pub is_sev_enabled: bool,
    pub zfs_info: Vec<String>,
    pub ipfs_zfs_pool_size: String,
    pub ipfs_zfs_pool_alloc: String,
    pub ipfs_zfs_pool_free: String,
    pub raid_info: Vec<String>,
    pub vm_count: u32,
    pub primary_network_interface: Option<NetworkInterfaceWrapper>,
    pub disks: Vec<DiskWrapper>,
    pub ipfs_repo_size: u64,
    pub ipfs_storage_max: u64,
    pub cpu_model: String,
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub free_memory_mb: u64,
    pub gpu_name: Option<String>,
    pub gpu_memory_mb: Option<u32>,
    pub hypervisor_disk_type: Option<String>,
    pub vm_pool_disk_type: Option<String>,
    pub disk_info: Vec<DiskDetailsWrapper>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInterfaceWrapper {
    pub name: String,
    pub mac_address: Option<String>,
    pub uplink_mb: u64,
    pub downlink_mb: u64,
    pub network_details: Option<NetworkDetailsWrapper>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkDetailsWrapper {
    pub network_type: String,
    pub city: Option<String>,
    pub region: Option<String>,
    pub country: Option<String>,
    pub loc: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiskWrapper {
    pub name: String,
    pub disk_type: String,
    pub total_space_mb: u64,
    pub free_space_mb: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiskDetailsWrapper {
    pub name: String,
    pub serial: String,
    pub model: String,
    pub size: String,
    pub is_rotational: bool,
    pub disk_type: String,
}

// // Helper struct for input parameters
// #[derive(Clone, Debug)]
// pub struct StorageUnpinUpdateRequestInput {
//     pub miner_pin_requests: Vec<MinerProfileItem>,
//     pub storage_request_owner: String,
//     pub storage_request_file_hash: String,
//     pub file_size: u128,
//     pub user_profile_cid: String,
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RebalanceItem {
    pub old_miner_id: String,
    pub miner_profile_cid: String,
}

// #[derive(Debug)]
// pub struct UpdatedMinerProfileItem {
//     pub miner_node_id: Vec<u8>,
//     pub cid: Vec<u8>,
//     pub added_files_count: u32,
//     pub added_file_size: u128,
// }

// #[derive(Debug)]
// pub struct UpdatedUserProfileItem {
//     pub user: String,
//     pub cid: Vec<u8>,
// }

#[derive(Serialize, Debug)]
pub struct MinerPinMetrics {
    pub node_id: String,
    pub total_pin_checks: u32,
    pub successful_pin_checks: u32,
    pub total_challenges: u32,
    pub successful_challenges: u32,
}

// Create a simple struct for the minimal data you need
#[derive(sqlx::FromRow)]
pub struct MinerStorageInfo {
    pub miner_id: String,
    pub ipfs_repo_size: i64,
    pub ipfs_storage_max: i64,
}
