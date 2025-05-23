use crate::assignment;
use crate::reconstruct_profile;
use crate::config::Config;
use crate::types::*;
use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::get,
};
use std::fs;
use std::sync::Arc;
use tokio::fs as tokio_fs;
use crate::ipfs_utils::{pin_profiles,fetch_ipfs_file_size, pin_content_to_ipfs};
use crate::transactions::{submit_storage_requests_transactions};

pub fn create_router(app_state: AppState) -> Router {
    Router::new()
        .route("/api/ipfs/:table", get(get_all_entries))
        .route("/api/ipfs/:table/:storage_key", get(get_entry_by_key))
        .route("/api/ipfs/pin-profiles", get(pin_profiles))
        .route("/api/ipfs/file-size/:file_hash", get(get_file_size))
        .route("/api/ipfs/cid-info/:file_hash", get(get_cid_info))
        .route("/api/ipfs/cid-info", get(get_all_cids_info))
        .with_state(app_state)
}

async fn get_all_entries(
    State(state): State<AppState>,
    Path(table): Path<String>,
) -> Result<Json<Vec<Entry>>, (StatusCode, String)> {
    let table_name = format!("ipfs_{}", table.to_lowercase());
    if !state.ipfs_entries.iter().any(|e| e.to_lowercase() == table) {
        return Err((StatusCode::NOT_FOUND, format!("Table {} not found", table)));
    }

    let query_string = format!("SELECT  storage_value FROM blockchain.{}", table_name);

    // Adapted for sqlx
    let entries: Vec<Entry> = sqlx::query_as(&query_string)
        .fetch_all(&*state.session) // state.session is Arc<PgPool>
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_iter()
        .map(|(storage_key, storage_value): (String, String)| Entry {
            storage_key,
            storage_value,
        })
        .collect();

    Ok(Json(entries))
}

async fn get_entry_by_key(
    State(state): State<AppState>,
    Path((table, storage_key_param)): Path<(String, String)>, // Renamed storage_key to avoid conflict
) -> Result<Json<Entry>, (StatusCode, String)> {
    let table_name = format!("ipfs_{}", table.to_lowercase());
    if !state.ipfs_entries.iter().any(|e| e.to_lowercase() == table) {
        return Err((StatusCode::NOT_FOUND, format!("Table {} not found", table)));
    }

    let query_string = format!(
        "SELECT storage_key, storage_value FROM blockchain.{} WHERE storage_key = $1",
        table_name
    );

    // Adapted for sqlx
    let row: Option<(String, String)> = sqlx::query_as(&query_string)
        .bind(storage_key_param.clone()) // Pass the owned String or a slice
        .fetch_optional(&*state.session) // state.session is Arc<PgPool>
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    match row {
        Some((storage_key, storage_value)) => Ok(Json(Entry {
            storage_key,
            storage_value,
        })),
        None => Err((StatusCode::NOT_FOUND, "Entry not found".to_string())),
    }
}


// New endpoint to get file size
async fn get_file_size(
    State(state): State<AppState>,
    Path(file_hash): Path<String>,
) -> Result<Json<FileSizeResponse>, (StatusCode, String)> {
    let size = fetch_ipfs_file_size(&state, &file_hash)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(FileSizeResponse { size }))
}

// New endpoint to get CID info
async fn get_cid_info(
    State(state): State<AppState>,
    Path(file_hash): Path<String>,
) -> Result<Json<CidInfoResponse>, (StatusCode, String)> {
    let query = "SELECT item_value FROM blockchain.cidsInfo WHERE hash_key = $1";

    // Use sqlx query pattern with bind()
    let row: (String,) = sqlx::query_as(query)
        .bind(&file_hash)
        .fetch_one(&*state.session)
        .await
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                format!("CID info not found for file hash: {}", e),
            )
        })?;

    let (item_value,) = row;
    Ok(Json(CidInfoResponse { item_value }))
}

// New endpoint to get all CIDs info
async fn get_all_cids_info(
    State(state): State<AppState>,
) -> Result<Json<Vec<CidInfoEntry>>, (StatusCode, String)> {
    let query = "SELECT hash_key, item_value FROM blockchain.cidsInfo";

    // Use sqlx query pattern
    let rows: Vec<(String, String)> = sqlx::query_as(query)
        .fetch_all(&*state.session)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let entries = rows
        .into_iter()
        .map(|(hash_key, item_value)| CidInfoEntry {
            hash_key,
            item_value,
        })
        .collect::<Vec<_>>();

    Ok(Json(entries))
}


// pub async fn cleanup_processed_requests(
//     state: &AppState,
//     retention_period: i64,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     // Remove old processed storage requests
//     let query = "DELETE FROM blockchain.processed_storage_requests WHERE processed_at < NOW() - INTERVAL '$1 seconds'";

//     sqlx::query(query)
//         .bind(retention_period)
//         .execute(&*state.session)
//         .await
//         .map_err(|e| {
//             let err = format!("Failed to clean up processed requests: {}", e);
//             eprintln!("{}", err);
//             Box::new(Error::new(ErrorKind::Other, err))
//         })?;

//     // Remove old processed unpin requests
//     let delete_query = "DELETE FROM blockchain.processed_unpin_requests WHERE processed_at < NOW() - INTERVAL '$1 seconds'";

//     sqlx::query(delete_query)
//         .bind(retention_period)
//         .execute(&*state.session)
//         .await
//         .map_err(|e| {
//             let err = format!("Failed to clean up processed unpin requests: {}", e);
//             eprintln!("{}", err);
//             Box::new(Error::new(ErrorKind::Other, err))
//         })?;

//     Ok(())
// }

// pub async fn update_blacklist_cid(
//     state: &AppState,
//     clean_cid: &str,
// ) -> Result<String, (StatusCode, String)> {
//     // Fetch current blacklist
//     let query =
//         "SELECT storage_value FROM blockchain.ipfs_blacklist WHERE storage_key = 'blacklist'";

//     // Use sqlx query pattern
//     let blacklist_json: Option<String> = sqlx::query_scalar(query)
//         .fetch_optional(&*state.session)
//         .await
//         .map_err(|e| {
//             eprintln!("Error querying blacklist: {}", e);
//             (
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 format!("Database error: {}", e),
//             )
//         })?;

//     let mut blacklist: Vec<String> = blacklist_json
//         .and_then(|json| serde_json::from_str(&json).ok())
//         .unwrap_or_default();

//     // Add CID to blacklist if not already there
//     if !blacklist.contains(&clean_cid.to_string()) {
//         blacklist.push(clean_cid.to_string());
//     }

//     // Update blacklist in database
//     let updated_blacklist_json = serde_json::to_string(&blacklist).map_err(|e| {
//         eprintln!("Error serializing blacklist: {}", e);
//         (
//             StatusCode::INTERNAL_SERVER_ERROR,
//             format!("Failed to serialize blacklist: {}", e),
//         )
//     })?;

//     let update_query = "INSERT INTO blockchain.ipfs_blacklist (storage_key, storage_value) VALUES ('blacklist', $1)";

//     // Use sqlx query pattern
//     sqlx::query(update_query)
//         .bind(&updated_blacklist_json)
//         .execute(&*state.session)
//         .await
//         .map_err(|e| {
//             eprintln!("Error updating blacklist: {}", e);
//             (
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 format!("Failed to update blacklist: {}", e),
//             )
//         })?;

//     Ok(format!("Added CID {} to blacklist", clean_cid))
// }

/// Initializes a new epoch and runs basic epoch-related operations.
/// Called at the start of each epoch (every 1200 blocks).
pub async fn log_miner_epoch_info(state: &Arc<AppState>) -> Result<(), String> {
    // Note: epoch_miners table is now refreshed in refresh_profile_tables
    // This function can be used for additional epoch initialization tasks
    println!("Running additional epoch initialization tasks");

    // Example: Check number of miners in the current epoch
    let count_query = "SELECT COUNT(*) FROM blockchain.epoch_miners";
    let count: i64 = sqlx::query_scalar(count_query)
        .fetch_one(&*state.session)
        .await
        .map_err(|e| format!("Failed to count epoch miners: {}", e))?;

    println!("Current epoch has {} registered miners", count);

    Ok(())
}

/// Refreshes all profile tables at the start of an epoch
pub async fn refresh_profile_tables(state: &Arc<AppState>) -> Result<(), String> {
    println!("======================================");
    println!("Refreshing profile tables at the start of a new epoch");
    println!("======================================");

    // Refresh epoch_miners table first
    println!("Refreshing epoch_miners table");
    let clear_query = "DELETE FROM blockchain.epoch_miners";
    sqlx::query(clear_query)
        .execute(&*state.session)
        .await
        .map_err(|e| format!("Failed to clear epoch miners: {}", e))?;

    // Fetch registration data
    let registration_query = "SELECT storage_key, storage_value FROM blockchain.registration";
    let rows: Vec<(String, String)> = sqlx::query_as(registration_query)
        .fetch_all(&*state.session)
        .await
        .map_err(|e| format!("Failed to fetch registration data: {}", e))?;
    println!("rows for reg are {}", rows.len());
    let mut miners_added = 0;

    // Process miners from registration data
    for (node_id, storage_value) in rows {
        // Parse storage value
        let json_value: serde_json::Value = serde_json::from_str(&storage_value)
            .map_err(|e| format!("Failed to parse registration data for {}: {}", node_id, e))?;

        // Check if this is a StorageMiner
        if let Some(node_type) = json_value.get("node_type").and_then(|v| v.as_str()) {
            if node_type == "StorageMiner" {
                if let Some(ipfs_node_id) = json_value.get("ipfs_node_id").and_then(|v| v.as_str())
                {
                    let insert_query = "INSERT INTO blockchain.epoch_miners (miner_id, ipfs_node_id) VALUES ($1, $2)";
                    sqlx::query(insert_query)
                        .bind(&node_id)
                        .bind(ipfs_node_id)
                        .execute(&*state.session)
                        .await
                        .map_err(|e| format!("Failed to insert epoch miner {}: {}", node_id, e))?;

                    miners_added += 1;

                    println!(
                        "Added miner {} with IPFS node ID {} to epoch_miners",
                        node_id, ipfs_node_id
                    );
                } else {
                    println!("Skipped miner {} - missing IPFS node ID", node_id);
                }
            } else {
                println!(
                    "Skipped non-StorageMiner node {} (type: {})",
                    node_id, node_type
                );
            }
        } else {
            println!("Skipped node {} - missing node_type field", node_id);
        }
    }

    println!("Added {} miners to epoch_miners table", miners_added);

    println!("======================================");
    println!("Profile refresh complete:");
    println!(" - {} miners added to epoch_miners", miners_added);
    println!("======================================");

    Ok(())
}

/// Clears profile and state tables at the end of an epoch
pub async fn clear_profile_tables(state: &Arc<AppState>) -> Result<(), String> {
    println!("Clearing profile and state tables at the end of the epoch");

    // Clear profile tables
    let clear_miners_query = "DELETE FROM blockchain.ipfs_minerprofile";
    sqlx::query(clear_miners_query)
        .execute(&*state.session)
        .await
        .map_err(|e| format!("Failed to clear miner profiles: {}", e))?;

    let clear_users_query = "DELETE FROM blockchain.ipfs_userprofile";
    sqlx::query(clear_users_query)
        .execute(&*state.session)
        .await
        .map_err(|e| format!("Failed to clear user profiles: {}", e))?;

    // Clear state tables
    let clear_tables = [
        "DELETE FROM blockchain.ipfs_blacklist",
        "DELETE FROM blockchain.cidsInfo",
        "DELETE FROM blockchain.ipfs_userstoragerequests",
        "DELETE FROM blockchain.ipfs_userunpinrequests",
        "DELETE FROM blockchain.ipfs_pinningenabled",
        "DELETE FROM blockchain.ipfs_assignmentenabled",
        "DELETE FROM blockchain.ipfs_rebalancerequest",
    ];

    for query in clear_tables.iter() {
        sqlx::query(query)
            .execute(&*state.session)
            .await
            .map_err(|e| format!("Failed to execute query {}: {}", query, e))?;
    }

    // Remove profile directories
    let miners_dir = "profile/miners";
    if tokio_fs::metadata(miners_dir).await.is_ok() {
        match tokio_fs::remove_dir_all(miners_dir).await {
            Ok(()) => println!("Successfully removed directory {}", miners_dir),
            Err(e) => eprintln!("Failed to remove directory {}: {}", miners_dir, e),
        }
    } else {
        println!("Directory {} does not exist, skipping", miners_dir);
    }

    let users_dir = "profile/users";
    if tokio_fs::metadata(users_dir).await.is_ok() {
        match tokio_fs::remove_dir_all(users_dir).await {
            Ok(()) => println!("Successfully removed directory {}", users_dir),
            Err(e) => eprintln!("Failed to remove directory {}: {}", users_dir, e),
        }
    } else {
        println!("Directory {} does not exist, skipping", users_dir);
    }

    println!("All profile and state tables cleared successfully");

    Ok(())
}

pub async fn run_epoch_verification(
    state: &Arc<AppState>,
    current_block: u64,
    block_hash: subxt::utils::H256,
    metadata: &subxt::metadata::Metadata,
) -> Result<(), String> {
    // First check if we are the current epoch validator
    match state.substrate_fetcher.is_current_epoch_validator().await {
        Ok(true) => {
            println!(
                "We are the current epoch validator, initializing epoch for block {}",
                current_block
            );
            // Check if we're in the current epoch
            match state
                .substrate_fetcher
                .is_in_current_validator_epoch(current_block)
                .await
            {
                Ok(true) => {
                    println!("Processing epoch verification as the current validator");

                    // Get epoch details
                    let epoch_info = {
                        let current_epoch_validator = state
                            .substrate_fetcher
                            .current_epoch_validator
                            .lock()
                            .map_err(|e| format!("Mutex lock failed: {}", e))?;
                        if let Some((validator, epoch_start_block)) = &*current_epoch_validator {
                            Some((epoch_start_block.clone(), validator.clone()))
                        } else {
                            None
                        }
                    };

                    if let Some((epoch_start_block, _validator)) = epoch_info {
                        let epoch_cleanup_block = Config::get().epoch_cleanup_block;
                        let block_in_epoch = current_block - epoch_start_block;
                        // Check if this is the start of the epoch (first blocks)
                        if block_in_epoch < 5 {
                            // // Pause block processing
                            {
                                let mut is_paused = state
                                    .substrate_fetcher
                                    .is_paused
                                    .lock()
                                    .map_err(|e| format!("Mutex lock failed: {}", e))?;
                                *is_paused = true;
                                println!(
                                    "Paused block processing for IPFS fetch at block {}",
                                    current_block
                                );
                            }
                            // This is the start of the epoch, refresh profile tables
                            println!(
                                "Start of epoch detected at block {} (block {} in epoch)",
                                current_block, block_in_epoch
                            );
                            refresh_profile_tables(state).await?;
                        }
                        // For next 20 blocks we should fetch the data for ipfs
                        else if block_in_epoch >= 5 && block_in_epoch < 40 {
                            // fetch ipfs related data 
                            let processed = state
                                .substrate_fetcher
                                .fetch_and_update_ipfs_data(block_hash, metadata, &state.clone())
                                .await
                                .map_err(|e| format!("Failed to fetch IPFS data: {}", e))?;

                            if processed {
                                println!(
                                    "Processed IPFS storage or unpin requests at block {}",
                                    current_block
                                );
                            }
                        } else if block_in_epoch > 5 && block_in_epoch != epoch_cleanup_block {
                            let mut should_process = false; // Introduce a new variable
                            {
                                let mut has_processed = state
                                    .substrate_fetcher
                                    .has_processed_assignments
                                    .lock()
                                    .map_err(|e| format!("Mutex lock failed: {}", e))?;
                                if !*has_processed {
                                    should_process = true; // Store the result of the check
                                    *has_processed = true; // Set the flag *before* dropping the guard
                                }
                            } // `has_processed` is dropped here, releasing the lock
                            if should_process {
                                println!("Started Reconstruction");
                                // next we should reconstruct
                                reconstruct_profile::reconstruct_profiles_to_files(state)
                                    .await
                                    .map_err(|e| {
                                        format!("Failed to sync profiles to files: {}", e)
                                    })?;
                                // next we should process
                                assignment::assign_miners_to_storage_request(state, block_hash)
                                    .await
                                    .map_err(|e| {
                                        format!(
                                            "Failed to assign miners to storage requests: {}",
                                            e
                                        )
                                    })?;
                                // next we should pin the profiles and submit tx submit tx at the end
                                println!("Submitting transaction .......");
                                process_storage_requests_and_profiles(state).await.map_err(
                                    |e| {
                                        format!(
                                            "Failed to assign miners to storage requests: {}",
                                            e
                                        )
                                    },
                                )?;
                                // should_process = false;
                                // *has_processed = false;
                            }
                        }
                        // Check if this is the cleanup block
                        else if block_in_epoch >= epoch_cleanup_block {
                            // This is the cleanup block, clear profile tables
                            println!(
                                "Cleanup block {} reached (block {} in epoch)",
                                current_block, block_in_epoch
                            );
                            {
                                let mut has_processed = state
                                    .substrate_fetcher
                                    .has_processed_assignments
                                    .lock()
                                    .map_err(|e| format!("Mutex lock failed: {}", e))?;
                                *has_processed = false;
                            }
                            // Pause block processing
                            {
                                let mut is_paused = state
                                    .substrate_fetcher
                                    .is_paused
                                    .lock()
                                    .map_err(|e| format!("Mutex lock failed: {}", e))?;
                                *is_paused = false;
                                println!(
                                    "Paused block processing for IPFS fetch at block {}",
                                    current_block
                                );
                            }
                            clear_profile_tables(state).await?;
                        }

                        println!(
                            "Paused block processing for IPFS fetch, blocks passed since epoch  {}",
                            block_in_epoch
                        );
                    }

                    // Call the standard epoch initialization
                    log_miner_epoch_info(state).await?;
                    Ok(())
                }
                Ok(false) => {
                    println!(
                        "Not in current validator's epoch window for block {}",
                        current_block
                    );
                    Ok(())
                }
                Err(e) => {
                    eprintln!("Error checking epoch status: {}", e);
                    Err(format!("Error checking epoch status: {}", e))
                }
            }
        }
        Ok(false) => {
            println!(
                "We are not the current epoch validator for block {}",
                current_block
            );
            Ok(())
        }
        Err(e) => {
            eprintln!(
                "Error checking if we are the current epoch validator: {}",
                e
            );
            Err(format!(
                "Error checking if we are the current epoch validator: {}",
                e
            ))
        }
    }
}

// pub async fn add_failed_unpin_request(
//     state: &AppState,
//     cid: &str,
//     owner: &str,
// ) -> Result<(), (StatusCode, String)> {
//     // Get current failure count, if any
//     let check_query = "SELECT failure_count FROM blockchain.failed_unpin_requests WHERE file_hash = $1 AND owner = $2";

//     // Use sqlx query pattern
//     let failure_count: i32 = match sqlx::query_scalar::<_, i32>(check_query)
//         .bind(cid)
//         .bind(owner)
//         .fetch_optional(&*state.session)
//         .await
//     {
//         Ok(Some(count)) => count + 1,
//         Ok(None) => 1,
//         Err(e) => {
//             eprintln!("Error checking failed unpin requests: {}", e);
//             1 // Default to 1 on error
//         }
//     };

//     // Upsert query with updated failure count and timestamp
//     let upsert_failed_query = "
//         INSERT INTO blockchain.failed_unpin_requests (file_hash, owner, failure_count, last_failed_at)
//         VALUES ($1, $2, $3, NOW())
//         ON CONFLICT (file_hash, owner) 
//         DO UPDATE SET failure_count = $3, last_failed_at = NOW()
//     ";

//     // Use sqlx query pattern
//     sqlx::query(upsert_failed_query)
//         .bind(cid)
//         .bind(owner)
//         .bind(failure_count)
//         .execute(&*state.session)
//         .await
//         .map_err(|e| {
//             eprintln!("Error updating failed unpin requests: {}", e);
//             (
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 format!("Failed to update failure count: {}", e),
//             )
//         })?;

//     // If the failure count reaches 3 or more, delete from in-progress
//     if failure_count >= 3 {
//         // Use the delete function we updated earlier
//         let delete_failed_query =
//             "DELETE FROM blockchain.failed_unpin_requests WHERE file_hash = $1 AND owner = $2";

//         sqlx::query(delete_failed_query)
//             .bind(cid)
//             .bind(owner)
//             .execute(&*state.session)
//             .await
//             .map_err(|e| {
//                 eprintln!("Error deleting failed unpin request: {}", e);
//                 (
//                     StatusCode::INTERNAL_SERVER_ERROR,
//                     format!("Failed to delete failed request: {}", e),
//                 )
//             })?;
//     }

//     Ok(())
// }

// Function to process storage requests and submit transactions
pub async fn process_storage_requests_and_profiles(
    app_state: &Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Query processed storage requests
    let query = "
        SELECT file_hash, owner
        FROM blockchain.processed_storage_requests
    ";
    let processed_rows: Vec<(String, String)> = sqlx::query_as(query)
        .fetch_all(&*app_state.session)
        .await
        .map_err(|e| format!("Failed to query processed storage requests: {}", e))?;

    if processed_rows.is_empty() {
        println!("No processed storage requests found.");
        return Ok(());
    }

    // Step 2: Process each request
    let mut responses: Vec<ProcessStorageResponse> = Vec::new();

    for (file_hash, owner) in processed_rows {
        // Start a transaction for each request
        let mut tx = app_state
            .session
            .begin()
            .await
            .map_err(|e| format!("Failed to start transaction for {}: {}", owner, e))?;

        // Read user profile JSON
        let user_path = format!("profile/users/{}.json", owner);
        let profile_content = match fs::read(&user_path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Failed to read user profile {}: {}", user_path, e);
                continue;
            }
        };

        let profile_content_str = match String::from_utf8(profile_content.clone()) {
            Ok(content) => content,
            Err(e) => {
                eprintln!(
                    "Failed to convert user profile {} to UTF-8: {}",
                    user_path, e
                );
                continue;
            }
        };

        // Parse JSON as an array
        let profile_array: Vec<serde_json::Value> = match serde_json::from_slice(&profile_content) {
            Ok(array) => array,
            Err(e) => {
                eprintln!(
                    "Failed to parse user profile {} as JSON array: {}",
                    user_path, e
                );
                continue;
            }
        };

        // Step 3: Calculate total file size
        let total_file_size: u64 = profile_array
            .iter()
            .map(|json| json["file_size_in_bytes"].as_u64().unwrap_or(0))
            .sum();

        // Ensure file size fits in u32
        let file_size_u32 = total_file_size
            .try_into()
            .map_err(|_| format!("Total file size for user {} exceeds u32 limit", owner))?;

        // Step 4: Pin profile content to IPFS
        let user_profile_cid = match pin_content_to_ipfs(app_state, &profile_content_str).await {
            Ok(cid) => cid,
            Err(e) => {
                eprintln!("Failed to pin user profile {} to IPFS: {}", user_path, e);
                continue;
            }
        };

        let response = ProcessStorageResponse {
            storage_request_owner: owner.clone(),
            storage_request_file_hash: file_hash.clone(),
            file_size: file_size_u32,
            user_profile_cid,
        };

        // Step 6: Delete from processed_storage_requests
        sqlx::query(
            "DELETE FROM blockchain.processed_storage_requests WHERE file_hash = $1 AND owner = $2",
        )
        .bind(&file_hash)
        .bind(&owner)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to delete processed request for {}: {}", owner, e))?;

        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction for {}: {}", owner, e))?;

        // Add to responses
        responses.push(response);
    }

    // Step 1: Read all files in profile/miners/
    let miners_dir = std::path::Path::new("profile/miners");
    if !miners_dir.exists() {
        println!("Miners directory profile/miners does not exist.");
        return Ok(());
    }

    let mut entries = tokio::fs::read_dir(miners_dir)
        .await
        .map_err(|e| format!("Failed to read miners directory: {}", e))?;

    let mut miner_profiles: Vec<MinerProfileItem> = Vec::new();

    // Step 2: Process each miner profile file
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("Failed to read directory entry: {}", e))?
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue; // Skip non-JSON files
        }

        // Extract miner_node_id from filename (e.g., miner1 from miner1.json)
        let miner_node_id = match path.file_stem().and_then(|s| s.to_str()) {
            Some(id) => id.to_string(),
            None => {
                eprintln!("Invalid filename for miner profile: {:?}", path);
                continue;
            }
        };

        // Read profile JSON
        let profile_content = match tokio::fs::read(&path).await {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Failed to read miner profile {}: {}", path.display(), e);
                continue;
            }
        };

        // Convert to String for pin_content_to_ipfs
        let profile_content_str = match String::from_utf8(profile_content) {
            Ok(content) => content,
            Err(e) => {
                eprintln!(
                    "Failed to convert miner profile {} to UTF-8: {}",
                    path.display(),
                    e
                );
                continue;
            }
        };

        // Parse JSON as an array
        let profile_array: Vec<serde_json::Value> = match serde_json::from_str(&profile_content_str)
        {
            Ok(array) => array,
            Err(e) => {
                eprintln!(
                    "Failed to parse miner profile {} as JSON array: {}",
                    path.display(),
                    e
                );
                continue;
            }
        };

        // Step 3: Calculate files_count and files_size
        let files_count = profile_array
            .len()
            .try_into()
            .map_err(|_| format!("Files count for miner {} exceeds u32 limit", miner_node_id))?;

        let files_size: u64 = profile_array
            .iter()
            .map(|json| json["file_size_in_bytes"].as_u64().unwrap_or(0))
            .sum();

        let files_size_u32 = files_size.try_into().map_err(|_| {
            format!(
                "Total file size for miner {} exceeds u32 limit",
                miner_node_id
            )
        })?;

        // Step 4: Pin profile content to IPFS
        let cid = match pin_content_to_ipfs(app_state, &profile_content_str).await {
            Ok(cid) => cid,
            Err(e) => {
                eprintln!(
                    "Failed to pin miner profile {} to IPFS: {}",
                    path.display(),
                    e
                );
                continue;
            }
        };

        // Step 5: Create MinerProfileItem
        let profile_item = MinerProfileItem {
            miner_node_id,
            cid,
            files_count,
            files_size: files_size_u32,
        };

        miner_profiles.push(profile_item);
    }

    // Step 7: Submit transactions
    if !responses.is_empty() {
        submit_storage_requests_transactions(app_state, &responses, &miner_profiles)
            .await
            .map_err(|e| format!("Failed to submit storage request transactions: {}", e))?;
    } else {
        println!("No valid ProcessStorageResponse entries to submit.");
    }

    Ok(())
}