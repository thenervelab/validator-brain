use hex;
use parity_scale_codec::{Decode, DecodeAll};
use serde_json::json;
use std::collections::BTreeSet;
use subxt::metadata::types::{StorageEntryMetadata, StorageEntryType};
use subxt::utils::AccountId32;
use std::fs;
use subxt::ext::sp_core::crypto::Pair as SubxtPair;
use subxt::ext::sp_core::sr25519;
use crate::substrate_fetcher::custom_runtime::runtime_types::pallet_execution_unit::types::{NetworkType, NodeMetricsData};
use crate::substrate_fetcher::custom_runtime::runtime_types::bounded_collections::bounded_vec::BoundedVec;
use crate::substrate_fetcher::custom_runtime::runtime_types::ipfs_pallet::types::RebalanceRequestItem;
use crate::config::Config;

pub fn decode_value(
        value: &[u8],
        entry: &StorageEntryMetadata,
        _metadata: &subxt::metadata::Metadata,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut input = &value[..]; // Immutable input
        let entry_name = entry.name();
        if entry_name == "UserStorageRequests" {
            // Decode Option<StorageRequest<AccountId32, u32>> manually
            let option_byte = input.first().ok_or("Empty input for Option")?;
            if *option_byte != 1 {
                return if *option_byte == 0 {
                    Ok(json!(null))
                } else {
                    Err(format!("Invalid Option byte: {}", option_byte).into())
                };
            }
            input = &input[1..]; // Skip Option::Some byte

            // Decode fields
            let total_replicas = u32::decode(&mut input)
                .map_err(|e| format!("Failed to decode total_replicas: {}", e))?;
            let owner = AccountId32::decode(&mut input)
                .map_err(|e| format!("Failed to decode owner: {}", e))?;
            let file_hash = BoundedVec::<u8>::decode(&mut input)
                .map_err(|e| format!("Failed to decode file_hash: {}", e))?;
            let file_name = BoundedVec::<u8>::decode(&mut input)
                .map_err(|e| format!("Failed to decode file_name: {}", e))?;
            let last_charged_at = u32::decode(&mut input)
                .map_err(|e| format!("Failed to decode last_charged_at: {}", e))?;
            let skip_bytes = count_leading_zeros(input);
            // Skip 4 extra bytes (00000000) before created_at
            if input.len() < skip_bytes {
                return Err("Input too short to skip extra bytes before created_at".into());
            }
            input = &input[skip_bytes..];

            let created_at = u32::decode(&mut input)
                .map_err(|e| format!("Failed to decode created_at: {}", e))?;

            input = &input[input.len().saturating_sub(33)..];
            // Decode selected_validator
            if input.len() < 32 {
                return Err(format!(
                    "Insufficient input length for selected_validator: {}",
                    input.len()
                )
                .into());
            }
            let selected_validator = AccountId32::decode(&mut input)
                .map_err(|e| format!("Failed to decode selected_validator: {}", e))?;

            let _is_assigned = bool::decode(&mut input)
                .map_err(|e| format!("Failed to decode is_assigned: {}", e))?;

            return Ok(json!({
                "total_replicas": total_replicas,
                "owner": owner.to_string(),
                "file_hash": String::from_utf8_lossy(&file_hash.0).to_string(),
                "file_name": String::from_utf8_lossy(&file_name.0).to_string(),
                "last_charged_at": last_charged_at,
                "created_at": created_at,
                "miner_ids": null,
                "selected_validator": selected_validator.to_string(),
                "is_assigned": false,
            }));
        } else if entry_name == "UserUnpinRequests" {
            let unpin_requests =
                Vec::<(AccountId32, BoundedVec<u8>, AccountId32)>::decode(&mut input)
                    .map_err(|e| format!("Failed to decode UserUnpinRequests: {}", e))?;

            let requests_json: Vec<serde_json::Value> = unpin_requests
                .into_iter()
                .map(|(owner, file_hash, selected_validator)| {
                    json!({
                        "owner": owner.to_string(),
                        "file_hash": String::from_utf8_lossy(&file_hash.0).to_string(),
                        "selected_validator": selected_validator.to_string(),
                    })
                })
                .collect();

            return Ok(json!(requests_json));
        } else if entry_name == "NodeMetrics" {
            // Decode the NodeMetricsData directly instead of wrapping it in Option
            if let Ok(metrics) = NodeMetricsData::decode_all(&mut input) {
                let node_id = String::from_utf8(metrics.miner_id.clone())
                    .unwrap_or_else(|_| hex::encode(&metrics.miner_id));

                return Ok(json!({
                    "miner_id": node_id,
                    "bandwidth_mbps": metrics.bandwidth_mbps,
                    "current_storage_bytes": metrics.current_storage_bytes,
                    "total_storage_bytes": metrics.total_storage_bytes,
                    "geolocation": String::from_utf8_lossy(&metrics.geolocation).to_string(),
                    "successful_pin_checks": metrics.successful_pin_checks,
                    "total_pin_checks": metrics.total_pin_checks,
                    "storage_proof_time_ms": metrics.storage_proof_time_ms,
                    "storage_growth_rate": metrics.storage_growth_rate,
                    "latency_ms": metrics.latency_ms,
                    "total_latency_ms": metrics.total_latency_ms,
                    "total_times_latency_checked": metrics.total_times_latency_checked,
                    "avg_response_time_ms": metrics.avg_response_time_ms,
                    "peer_count": metrics.peer_count,
                    "failed_challenges_count": metrics.failed_challenges_count,
                    "successful_challenges": metrics.successful_challenges,
                    "total_challenges": metrics.total_challenges,
                    "uptime_minutes": metrics.uptime_minutes,
                    "total_minutes": metrics.total_minutes,
                    "consecutive_reliable_days": metrics.consecutive_reliable_days,
                    "recent_downtime_hours": metrics.recent_downtime_hours,
                    "is_sev_enabled": metrics.is_sev_enabled,
                    "zfs_info": metrics.zfs_info.iter().map(|info| String::from_utf8_lossy(info).to_string()).collect::<Vec<String>>(),
                    "ipfs_zfs_pool_size": metrics.ipfs_zfs_pool_size.to_string(),
                    "ipfs_zfs_pool_alloc": metrics.ipfs_zfs_pool_alloc.to_string(),
                    "ipfs_zfs_pool_free": metrics.ipfs_zfs_pool_free.to_string(),
                    "raid_info": metrics.raid_info.iter().map(|info| String::from_utf8_lossy(info).to_string()).collect::<Vec<String>>(),
                    "vm_count": metrics.vm_count,
                    "primary_network_interface": metrics.primary_network_interface.map(|net| json!({
                        "name": String::from_utf8_lossy(&net.name).to_string(),
                        "mac_address": net.mac_address.map(|mac| hex::encode(mac)),
                        "uplink_mb": net.uplink_mb,
                        "downlink_mb": net.downlink_mb,
                        "network_details": net.network_details.map(|details| json!({
                            "network_type": match details.network_type {
                                NetworkType::Private => "Private",
                                NetworkType::Public => "Public",
                            },
                            "city": details.city.map(|c| String::from_utf8_lossy(&c).to_string()),
                            "region": details.region.map(|r| String::from_utf8_lossy(&r).to_string()),
                            "country": details.country.map(|c| String::from_utf8_lossy(&c).to_string()),
                            "loc": details.loc.map(|l| String::from_utf8_lossy(&l).to_string()),
                        })),
                    })),
                    "disks": metrics.disks.iter().map(|disk| json!({
                        "name": String::from_utf8_lossy(&disk.name).to_string(),
                        "disk_type": String::from_utf8_lossy(&disk.disk_type).to_string(),
                        "total_space_mb": disk.total_space_mb,
                        "free_space_mb": disk.free_space_mb,
                    })).collect::<Vec<_>>(),
                    "ipfs_repo_size": metrics.ipfs_repo_size,
                    "ipfs_storage_max": metrics.ipfs_storage_max,
                    "cpu_model": String::from_utf8_lossy(&metrics.cpu_model).to_string(),
                    "cpu_cores": metrics.cpu_cores,
                    "memory_mb": metrics.memory_mb,
                    "free_memory_mb": metrics.free_memory_mb,
                    "gpu_name": metrics.gpu_name.map(|name| String::from_utf8_lossy(&name).to_string()),
                    "gpu_memory_mb": metrics.gpu_memory_mb,
                    "hypervisor_disk_type": metrics.hypervisor_disk_type.map(|dt| String::from_utf8_lossy(&dt).to_string()),
                    "vm_pool_disk_type": metrics.vm_pool_disk_type.map(|dt| String::from_utf8_lossy(&dt).to_string()).unwrap_or_default(),
                    "disk_info": metrics.disk_info.iter().map(|details| json!({
                        "name": String::from_utf8_lossy(&details.name).to_string(),
                        "serial": String::from_utf8_lossy(&details.serial).to_string(),
                        "model": String::from_utf8_lossy(&details.model).to_string(),
                        "size": String::from_utf8_lossy(&details.size).to_string(),
                        "is_rotational": details.is_rotational,
                        "disk_type": String::from_utf8_lossy(&details.disk_type).to_string(),
                    })).collect::<Vec<_>>(),
                }));
            } else {
                return Ok(json!(null)); // Handle the case where decoding fails
            }
        } else if entry_name == "UserProfile" {
            if let Ok(decoded) = BoundedVec::<u8>::decode(&mut input) {
                return Ok(serde_json::Value::String(
                    String::from_utf8_lossy(&decoded.0).to_string(),
                ));
            }
            return Err(format!("Failed to decode UserProfile: {}", hex::encode(value)).into());
        } else if entry_name == "MinerProfile" {
            if let Ok(decoded) = BoundedVec::<u8>::decode(&mut input) {
                return Ok(serde_json::Value::String(
                    String::from_utf8_lossy(&decoded.0).to_string(),
                ));
            }
            return Err(format!("Failed to decode MinerProfile: {}", hex::encode(value)).into());
        } else if entry_name == "RebalanceRequest" {
            // First try decoding as BoundedVec (direct storage value)
            if let Ok(decoded_vec) = <BoundedVec<RebalanceRequestItem>>::decode(&mut input) {
                let requests_json: Vec<serde_json::Value> = decoded_vec
                    .0
                    .into_iter()
                    .map(|item| {
                        json!({
                            "node_id": String::from_utf8_lossy(&item.node_id.0),
                            "miner_profile_id": String::from_utf8_lossy(&item.miner_profile_id.0),
                        })
                    })
                    .collect();
                return Ok(json!(requests_json));
            }

            // If that fails, reset input and try decoding as Option
            let mut input = &value[..];
            if let Ok(Some(decoded_vec)) =
                Option::<BoundedVec<RebalanceRequestItem>>::decode(&mut input)
            {
                let requests_json: Vec<serde_json::Value> = decoded_vec
                    .0
                    .into_iter()
                    .map(|item| {
                        json!({
                            "node_id": String::from_utf8_lossy(&item.node_id.0),
                            "miner_profile_id": String::from_utf8_lossy(&item.miner_profile_id.0),
                        })
                    })
                    .collect();
                return Ok(json!(requests_json));
            }

            Err(format!(
                "Failed to decode RebalanceRequest: input={}",
                hex::encode(value)
            )
            .into())
        } else {
            // Existing decoding logic for other types
            if let Ok(decoded) = u32::decode_all(&mut input) {
                return Ok(json!(decoded));
            }
            if let Ok(decoded) = u64::decode_all(&mut input) {
                return Ok(json!(decoded));
            }
            if let Ok(decoded) = u128::decode_all(&mut input) {
                return Ok(json!(decoded.to_string()));
            }
            if let Ok(decoded) = i32::decode_all(&mut input) {
                return Ok(json!(decoded.to_string()));
            }
            if let Ok(decoded) = AccountId32::decode_all(&mut input) {
                return Ok(json!(decoded.to_string()));
            }
            if let Ok(decoded) = Vec::<u8>::decode_all(&mut input) {
                return Ok(json!(String::from_utf8_lossy(&decoded)));
            }
            if let Ok(decoded) = Vec::<u16>::decode_all(&mut input) {
                return Ok(json!(decoded));
            }
            if let Ok(decoded) = Vec::<u64>::decode_all(&mut input) {
                return Ok(json!(decoded));
            }
            if let Ok(decoded) = bool::decode_all(&mut input) {
                return Ok(json!(decoded));
            }
            if let Ok(decoded_ids) = Vec::<AccountId32>::decode_all(&mut input) {
                return Ok(json!(
                    decoded_ids
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                ));
            }
            if let Ok(decoded_bytes) = Vec::<Vec<u8>>::decode_all(&mut input) {
                return Ok(json!(
                    decoded_bytes
                        .iter()
                        .map(|bytes| String::from_utf8_lossy(bytes))
                        .collect::<Vec<_>>()
                ));
            }
            if let Ok((account_id, number, account_set)) =
                <(AccountId32, u128, BTreeSet<AccountId32>)>::decode(&mut input)
            {
                return Ok(json!({
                    "account_id": account_id.to_string(),
                    "number": number.to_string(),
                    "account_set": account_set.iter().map(|id| id.to_string()).collect::<Vec<_>>()
                }));
            }
            if let Ok(option_account_id) = Option::<AccountId32>::decode(&mut input) {
                return Ok(json!(option_account_id.map(|id| id.to_string())));
            }
            if let Ok(decoded) = BoundedVec::<u8>::decode(&mut input) {
                return Ok(serde_json::Value::String(
                    String::from_utf8_lossy(&decoded.0).to_string(),
                ));
            }
            if let Ok(decoded_vec) = BoundedVec::<BoundedVec<u8>>::decode(&mut input) {
                return Ok(json!(
                    decoded_vec
                        .0
                        .iter()
                        .map(|hash| String::from_utf8_lossy(&hash.0).to_string())
                        .collect::<Vec<_>>()
                ));
            }
            if let Ok((user, amount, bittensor_coldkey, confirmations, bittensor_data)) =
                <(
                    AccountId32,
                    u128,
                    AccountId32,
                    BTreeSet<AccountId32>,
                    Option<(Vec<u8>, Vec<u8>)>,
                )>::decode(&mut input)
            {
                return Ok(json!({
                    "user": user.to_string(),
                    "amount": amount.to_string(),
                    "bittensor_coldkey": bittensor_coldkey.to_string(),
                    "confirmations": confirmations.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
                    "bittensor_data": bittensor_data.map(|(block_hash, extrinsic_id)| {
                        let block_hash = String::from_utf8(block_hash.clone())
                            .unwrap_or_else(|_| hex::encode(&block_hash));
                        let extrinsic_id = String::from_utf8(extrinsic_id.clone())
                            .unwrap_or_else(|_| hex::encode(&extrinsic_id));
                        json!({
                            "bittensor_block_hash": block_hash,
                            "extrinsic_id": extrinsic_id
                        })
                    }).unwrap_or(json!(null))
                }));
            }
            if let Ok(decoded) = Vec::<u128>::decode_all(&mut input) {
                return Ok(json!(
                    decoded.iter().map(|n| n.to_string()).collect::<Vec<_>>()
                ));
            }

            Ok(json!(hex::encode(value)))
        }
    }

pub fn decode_key(
        key_bytes: &[u8],
        entry: &StorageEntryMetadata,
        _metadata: &subxt::metadata::Metadata,
    ) -> Result<String, Box<dyn std::error::Error>> {
        const PREFIX_LEN: usize = 32;
        const BLAKE2_128_LEN: usize = 16;

        if key_bytes.len() <= PREFIX_LEN {
            return Ok(hex::encode(key_bytes));
        }

        let key_data = &key_bytes[PREFIX_LEN..];

        if let StorageEntryType::Map { hashers, .. } = entry.entry_type() {
            if hashers
                .iter()
                .any(|h| matches!(h, subxt::metadata::types::StorageHasher::Blake2_128Concat))
            {
                if key_data.len() <= BLAKE2_128_LEN {
                    return Ok(hex::encode(key_bytes));
                }

                let mut raw_key = &key_data[BLAKE2_128_LEN..];

                // Prioritize AccountId32 for UserProfile
                if entry.name() == "UserProfile" {
                    if let Ok(decoded) = AccountId32::decode_all(&mut raw_key) {
                        return Ok(decoded.to_string());
                    }
                }

                if let Ok(decoded) = BoundedVec::<u8>::decode(&mut raw_key) {
                    return Ok(String::from_utf8_lossy(&decoded.0).to_string());
                }
                if let Ok(decoded) = u32::decode_all(&mut raw_key) {
                    return Ok(decoded.to_string());
                }
                if let Ok(decoded) = u64::decode_all(&mut raw_key) {
                    return Ok(decoded.to_string());
                }
                if let Ok(decoded) = u128::decode_all(&mut raw_key) {
                    return Ok(decoded.to_string());
                }
                if let Ok(decoded) = AccountId32::decode_all(&mut raw_key) {
                    return Ok(decoded.to_string());
                }
                if let Ok(decoded) = Vec::<u8>::decode_all(&mut raw_key) {
                    return Ok(String::from_utf8_lossy(&decoded).to_string());
                }
                if let Ok(decoded) = bool::decode_all(&mut raw_key) {
                    return Ok(decoded.to_string());
                }
            }
        }

        Ok(hex::encode(key_bytes))
    }

pub fn decode_double_key(
        key_bytes: &[u8],
        entry: &StorageEntryMetadata,
        _metadata: &subxt::metadata::Metadata,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        const PREFIX_LEN: usize = 32;
        const BLAKE2_128_LEN: usize = 16;
        const ACCOUNT_ID_LEN: usize = 32;

        if key_bytes.len() <= PREFIX_LEN {
            return Err(format!("Key too short for double map: {}", hex::encode(key_bytes)).into());
        }

        let key_data = &key_bytes[PREFIX_LEN..];
        let entry_name = entry.name();

        if let StorageEntryType::Map { hashers, .. } = entry.entry_type() {
            if hashers.len() == 2
                && hashers
                    .iter()
                    .all(|h| matches!(h, subxt::metadata::types::StorageHasher::Blake2_128Concat))
            {
                if key_data.len() < 2 * BLAKE2_128_LEN + ACCOUNT_ID_LEN {
                    return Err(format!(
                        "Key data too short for double map: {}",
                        hex::encode(key_data)
                    )
                    .into());
                }
                let k1_data = &key_data[BLAKE2_128_LEN..BLAKE2_128_LEN + ACCOUNT_ID_LEN];
                // Second key: Blake2_128 hash (16 bytes) + FileHash (remaining bytes)
                let k2_raw = &key_data[BLAKE2_128_LEN + ACCOUNT_ID_LEN + BLAKE2_128_LEN..];

                if entry_name == "UserStorageRequests" {
                    // First key: AccountId32
                    let decoded_key1 =
                        if let Ok(decoded) = AccountId32::decode_all(&mut &k1_data[..]) {
                            decoded.to_string()
                        } else {
                            return Err(format!(
                                "Failed to decode AccountId32 for key1: {}",
                                hex::encode(k1_data)
                            )
                            .into());
                        };

                    // Second key: FileHash (BoundedVec<u8>)
                    let decoded_key2 =
                        if let Ok(decoded) = BoundedVec::<u8>::decode(&mut &k2_raw[..]) {
                            String::from_utf8_lossy(&decoded.0).to_string()
                        } else if let Ok(decoded) = Vec::<u8>::decode(&mut &k2_raw[..]) {
                            hex::encode(decoded)
                        } else {
                            return Err(format!(
                                "Failed to decode FileHash for key2: {}",
                                hex::encode(k2_raw)
                            )
                            .into());
                        };

                    return Ok((decoded_key1, decoded_key2));
                }

                // Generic decoding for other double maps
                let mut k1_input = k1_data;
                let mut k2_input = k2_raw;

                let decoded_key1 = if let Ok(decoded) = AccountId32::decode_all(&mut k1_input) {
                    decoded.to_string()
                } else if let Ok(decoded) = u128::decode_all(&mut k1_input) {
                    decoded.to_string()
                } else if let Ok(decoded) = Vec::<u8>::decode_all(&mut k1_input) {
                    String::from_utf8_lossy(&decoded).to_string()
                } else {
                    hex::encode(k1_data)
                };

                let decoded_key2 = if let Ok(decoded) = AccountId32::decode_all(&mut k2_input) {
                    decoded.to_string()
                } else if let Ok(decoded) = u128::decode_all(&mut k2_input) {
                    decoded.to_string()
                } else if let Ok(decoded) = Vec::<u8>::decode_all(&mut k2_input) {
                    String::from_utf8_lossy(&decoded).to_string()
                } else {
                    hex::encode(k2_raw)
                };

                return Ok((decoded_key1, decoded_key2));
            }
        }

        Err(format!(
            "Unsupported storage type for double key decoding: {}",
            entry.name()
        )
        .into())
    }

pub fn load_hips_account_id() -> Result<AccountId32, Box<dyn std::error::Error>> {
    let key_file = find_hips_key(&Config::get().keystore_path)?
        .ok_or("HIPS key not found in keystore")?;
    let key_path = std::path::Path::new(&Config::get().keystore_path).join(key_file);
    let raw = fs::read_to_string(&key_path)?;
    let phrase = raw.trim().trim_matches('"');
    let keypair = sr25519::Pair::from_string(phrase, None)?;
    let account_id = AccountId32::from(keypair.public());
    Ok(account_id)
}

pub fn find_hips_key(keystore_path: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let target_prefix = "68697073"; // "hips" in hex
    let dir_entries = fs::read_dir(keystore_path)?;
    for entry in dir_entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.starts_with(target_prefix) {
                    return Ok(Some(file_name.to_string()));
                }
            }
        }
    }
    Ok(None)
}

fn count_leading_zeros(bytes: &[u8]) -> usize {
    bytes.iter().take_while(|&&b| b == 0).count()
}
