use crate::AppState;
use crate::MinerStorageInfo;
use crate::ipfs_utils::{fetch_ipfs_content, fetch_ipfs_file_sizes};
use crate::types::StorageRequest;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde_json::Value;
use std::collections::BTreeMap;
use std::error::Error;
use subxt::utils::H256;
use crate::helpers::{read_json_array, write_json_array};

pub async fn assign_miners_to_storage_request(
    app_state: &AppState,
    block_hash: H256,
) -> Result<(), Box<dyn Error>> {
    // Start a database transaction
    let mut tx = app_state.session.begin().await?;

    // Step 1: Query in-progress storage requests within transaction
    let in_progress_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT file_hash, owner FROM blockchain.in_progress_storage_requests"
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("Failed to query in-progress storage requests: {}", e))?;

    if in_progress_rows.is_empty() {
        println!("No in-progress storage requests found.");
        return Ok(());
    }

    // Step 2: Fetch storage requests in batch
    let mut storage_requests = Vec::new();
    for (file_hash, owner) in in_progress_rows {
        let storage_key = format!("{}_{}", owner, file_hash);
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT storage_value FROM blockchain.ipfs_userstoragerequests WHERE storage_key = $1"
        )
        .bind(&storage_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| format!("Failed to query storage request for key {}: {}", storage_key, e))?;

        if let Some((storage_value,)) = row {
            match serde_json::from_str::<StorageRequest>(&storage_value) {
                Ok(storage_request) => storage_requests.push(storage_request),
                Err(e) => {
                    eprintln!("Failed to parse storage request for key {}: {}", storage_key, e);
                    continue;
                }
            }
        }
    }

    if storage_requests.is_empty() {
        println!("No valid storage requests found for in-progress entries.");
        return Ok(());
    }

    // Step 3: Fetch registered storage miners
    let miner_ids: Vec<String> = sqlx::query_as(
        "SELECT node_id FROM blockchain.ipfs_registration WHERE node_type = 'StorageMiner'"
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("Failed to query registered storage miners: {}", e))?
    .into_iter()
    .map(|(node_id,)| node_id)
    .collect();

    if miner_ids.is_empty() {
        println!("No registered storage miners found.");
        return Ok(());
    }

    // Step 4: Fetch node metrics in a single query
    let node_metrics_rows: Vec<MinerStorageInfo> = if !miner_ids.is_empty() {
        let query = format!(
            "SELECT miner_id, ipfs_repo_size, ipfs_storage_max 
             FROM blockchain.ipfs_nodemetrics 
             WHERE node_id IN ({})",
            miner_ids.iter().enumerate().map(|(i, _)| format!("${}", i + 1)).collect::<Vec<_>>().join(",")
        );

        let mut query = sqlx::query_as::<_, MinerStorageInfo>(&query);
        for miner_id in &miner_ids {
            query = query.bind(miner_id);
        }
        query.fetch_all(&mut *tx).await?
    } else {
        Vec::new()
    };

    let node_metrics_map: BTreeMap<Vec<u8>, MinerStorageInfo> = node_metrics_rows
        .into_iter()
        .map(|metrics| (metrics.miner_id.as_bytes().to_vec(), metrics))
        .collect();

    let original_free_miners: Vec<Vec<u8>> = node_metrics_map.keys().cloned().collect();
    println!("Available miners: {}", original_free_miners.len());

    // Prepare collections for results
    let mut miner_pin_requests: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut user_storage_requests: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut processed_requests = Vec::new();

    // Process storage requests in batches
    const BATCH_SIZE: usize = 20;
    for storage_request in storage_requests {
        // Convert file_hash to CID
        let cid_str = String::from_utf8(hex::decode(&storage_request.file_hash)?)?;
        
        // Fetch content with retry
        let content = fetch_ipfs_content(app_state, &cid_str).await?;
        let files: Vec<Value> = serde_json::from_slice(&content)?;
        
        // Track free miners per storage request
        let mut free_miners = original_free_miners.clone();

        // Process files in batches
        for chunk in files.chunks(BATCH_SIZE) {
            // Collect CIDs and fetch sizes in batch
            let batch_cids: Vec<String> = chunk.iter()
                .filter_map(|file| file["cid"].as_str().map(String::from))
                .collect();
            
            let size_map = fetch_ipfs_file_sizes(app_state, &batch_cids).await?;

            // Process each file in this batch
            for file in chunk {
                let filename = file["filename"].as_str().ok_or("Missing filename")?.to_string();
                let cid = file["cid"].as_str().ok_or("Missing CID")?;
                let file_size = *size_map.get(cid).unwrap_or(&0);

                // Filter available miners with sufficient storage
                let mut available_miners: Vec<Vec<u8>> = free_miners.iter()
                    .filter(|miner_id| {
                        node_metrics_map.get(*miner_id)
                            .map(|metrics| {
                                metrics.ipfs_storage_max.saturating_sub(metrics.ipfs_repo_size) >= file_size.into()
                            })
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect();

                // If fewer than 5 miners are available, reset free_miners and recalc available_miners
                if available_miners.len() < 5 {
                    free_miners = original_free_miners.clone();
                    available_miners = free_miners.iter()
                        .filter(|miner_id| {
                            node_metrics_map.get(*miner_id)
                                .map(|metrics| {
                                    metrics.ipfs_storage_max.saturating_sub(metrics.ipfs_repo_size) >= file_size.into()
                                })
                                .unwrap_or(false)
                        })
                        .cloned()
                        .collect();

                    if available_miners.len() < 5 {
                        eprintln!("Not enough available miners for file {} even after reset", cid);
                        continue;
                    }
                }

                // Create deterministic RNG seed
                let seed = {
                    let combined = [block_hash.0.as_ref(), cid.as_bytes()].concat();
                    let hash = blake3::hash(&combined);
                    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
                };
                let mut rng = StdRng::seed_from_u64(seed);

                // Select 5 random miners
                let mut selected_miners = available_miners;
                selected_miners.shuffle(&mut rng);
                let selected_miners = selected_miners.into_iter().take(5).collect::<Vec<_>>();

                // Prepare miner node IDs as strings
                let miner_node_ids: Vec<String> = selected_miners.iter()
                    .map(|id| String::from_utf8_lossy(id).into_owned())
                    .collect();

                // Create file hash key
                let file_hash_key = hex::encode(cid.as_bytes());
                let update_hash_vec: Vec<u8> = file_hash_key.clone().into();

                // Prepare pin requests for miners
                for miner_id in &miner_node_ids {
                    let pin_request = serde_json::json!({
                        "miner_node_id": miner_id,
                        "file_hash": update_hash_vec.clone(),
                        "created_at": storage_request.created_at,
                        "file_size_in_bytes": file_size,
                        "selected_validator": storage_request.selected_validator,
                        "owner": storage_request.owner
                    });

                    miner_pin_requests.entry(miner_id.clone())
                        .or_default()
                        .push(pin_request);
                }

                // Prepare storage request for user
                let storage_req = serde_json::json!({
                    "created_at": storage_request.created_at,
                    "file_hash": update_hash_vec,
                    "file_name": filename,
                    "file_size_in_bytes": file_size,
                    "is_assigned": true,
                    "last_charged_at": storage_request.last_charged_at,
                    "main_req_hash": storage_request.file_hash.clone(),
                    "miner_ids": miner_node_ids,
                    "total_replicas": storage_request.total_replicas,
                    "owner": storage_request.owner.clone(),
                    "selected_validator": storage_request.selected_validator.clone(),
                });

                user_storage_requests.entry(storage_request.owner.clone())
                    .or_default()
                    .push(storage_req);

                // Update free miners
                free_miners.retain(|miner_id| !selected_miners.contains(miner_id));
            }

            // Mark this request for processing
            processed_requests.push((storage_request.file_hash.clone(), storage_request.owner.clone()));
            
            // Yield to prevent blocking
            tokio::task::yield_now().await;
        }
    }

    // Step 6: Batch update database
    for (file_hash, owner) in processed_requests {
        // Delete from in-progress
        sqlx::query(
            "DELETE FROM blockchain.in_progress_storage_requests WHERE file_hash = $1 AND owner = $2"
        )
        .bind(&file_hash)
        .bind(&owner)
        .execute(&mut *tx)
        .await?;

        // Insert to processed if not exists
        let exists: Option<(i32,)> = sqlx::query_as(
            "SELECT 1 FROM blockchain.processed_storage_requests WHERE file_hash = $1 AND owner = $2"
        )
        .bind(&file_hash)
        .bind(&owner)
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_none() {
            sqlx::query(
                "INSERT INTO blockchain.processed_storage_requests (file_hash, owner) VALUES ($1, $2)"
            )
            .bind(&file_hash)
            .bind(&owner)
            .execute(&mut *tx)
            .await?;
        }
    }

    // Commit transaction
    tx.commit().await?;

    // Step 7: Write results to files
    for (miner_id, requests) in miner_pin_requests {
        let path = format!("profile/miners/{}.json", miner_id);
        let mut existing = read_json_array(&path).unwrap_or_default();
        existing.extend(requests);
        write_json_array(&path, &existing)?;
    }

    for (owner, requests) in user_storage_requests {
        let path = format!("profile/users/{}.json", owner);
        let mut existing = read_json_array(&path).unwrap_or_default();
        existing.extend(requests);
        write_json_array(&path, &existing)?;
    }

    Ok(())
}