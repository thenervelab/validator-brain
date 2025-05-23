use hex;
use parity_scale_codec::{Decode, DecodeAll};
use sqlx::postgres::PgPool;
use serde_json::{json, Value as JsonValue};
use std::str::FromStr;
use subxt::backend::rpc::RpcClient;
use subxt::metadata::types::{StorageEntryMetadata, StorageEntryType};
use subxt::{OnlineClient, PolkadotConfig, storage::StorageKeyValuePair, utils::AccountId32};
use types::*;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use crate::substrate_fetcher::custom_runtime::runtime_types::pallet_execution_unit::types::{NetworkType, NodeMetricsData};
use crate::substrate_fetcher::custom_runtime::registration::calls::types::register_node_with_hotkey::NodeType;
use crate::substrate_fetcher::custom_runtime::runtime_types::{ pallet_registration::types::{Status, NodeInfo}};
use crate::types;
use crate::AppState;
use crate::config::Config;
use crate::api::run_epoch_verification;
use crate::types::StorageRequest;
use crate::ipfs_utils::verify_miners_subset;
use crate::transactions::submit_update_pin_check_metrics_transaction;
use governor::Quota;
use std::num::NonZeroU32;
use governor::RateLimiter;
pub mod helper;
use helper::{load_hips_account_id, decode_value , decode_double_key, decode_key};
pub mod db;
use db::initialize_database;

#[subxt::subxt(runtime_metadata_path = "metadata.scale")]
pub mod custom_runtime {}

#[derive(Clone)]
pub struct SubstrateFetcher {
    pub api: OnlineClient<PolkadotConfig>,
    pub session: Arc<PgPool>,
    pub ipfs_entries: Vec<String>,
    pub ipfs_client: reqwest::Client,
    pub ipfs_node_url: String,
    pub hips_account_id: Option<AccountId32>,
    pub is_paused: Arc<Mutex<bool>>,
    pub has_processed_assignments: Arc<Mutex<bool>>,
    pub current_epoch_validator: Arc<Mutex<Option<(AccountId32, u64)>>>,
    pub tx_submission_lock: Arc<tokio::sync::Mutex<()>>,
}

impl SubstrateFetcher {
    pub async fn new(
        rpc_url: &str,
        session: Arc<PgPool>,
        ipfs_entries: Vec<String>,
        ipfs_node_url: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc = RpcClient::from_url(rpc_url).await?;
        let api = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc).await?;

        // Load hips key from config or keystore
        let hips_account_id = if let Some(validator_addr) = &Config::get().validator_address {
            // Use the validator address from config if provided
            println!("Using validator address from config: {}", validator_addr);
            match AccountId32::from_str(validator_addr) {
                Ok(account_id) => Some(account_id),
                Err(e) => {
                    eprintln!(
                        "Invalid validator address in config, falling back to keystore: {}",
                        e
                    );
                    load_hips_account_id().ok()
                }
            }
        } else {
            // Fall back to loading from keystore
            load_hips_account_id().ok()
        };

        if let Some(account) = &hips_account_id {
            println!("Using validator account: {}", account);
        } else {
            println!("No validator account configured");
        }

        // Call the database initialization function
        initialize_database(session.clone()).await?;

        Ok(Self {
            api,
            session,
            ipfs_entries,
            ipfs_client: reqwest::Client::new(),
            ipfs_node_url,
            hips_account_id,
            is_paused: Arc::new(Mutex::new(false)),
            has_processed_assignments: Arc::new(Mutex::new(false)),
            current_epoch_validator: Arc::new(Mutex::new(None)),
            tx_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub async fn process_blocks(&self) -> Result<(), Box<dyn std::error::Error>> {
        let metadata = self.api.metadata();
        let mut blocks = self.api.blocks().subscribe_finalized().await?;

        println!(
            "Starting to process blocks - connected to node: {}",
            Config::get().rpc_url
        );

        let verification_limit = 5; // Process 5 miners at a time

        while let Some(block) = blocks.next().await {
            match block {
                Ok(block) => {
                    let block_hash = block.hash();
                    let block_number = block.number();
                    println!(
                        "Processing block number {:?} with hash {:?}",
                        block_number, block_hash
                    );

                    // Configure rate limiting (e.g., 5 requests per second)
                    let quota = Quota::per_second(NonZeroU32::new(5).unwrap());
                    let rate_limiter = Arc::new(RateLimiter::direct(quota));
                    // Run epoch verification on every block
                    let app_state = AppState {
                        session: self.session.clone(),
                        ipfs_entries: self.ipfs_entries.clone(),
                        ipfs_client: self.ipfs_client.clone(),
                        ipfs_node_url: self.ipfs_node_url.clone(),
                        substrate_fetcher: Arc::new(self.clone()),
                        rate_limiter,
                    };

                    let app_state = Arc::new(app_state);
                    let metadata_clone = metadata.clone(); // Clone metadata for the task
                    match run_epoch_verification(
                        &app_state,
                        block_number.into(),
                        block_hash,
                        &metadata_clone,
                    )
                    .await
                    {
                        Ok(()) => {
                            // Successfully completed epoch verification
                        }
                        Err(err) => {
                            eprintln!(
                                "run_epoch_verification failed for block {}: {}",
                                block_number, err
                            );
                        }
                    }

                    // Check if processing is paused
                    {
                        let is_paused = *self.is_paused.lock().unwrap();
                        if is_paused {
                            println!("Block processing paused, waiting...");
                            sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }

                    // Check if this is the start of a new epoch (every 100 blocks starting from 38)
                    let is_epoch_start = (block_number - 38) % 100 == 0;

                    if !is_epoch_start {
                        // Set is_paused to true before verification
                        *self.is_paused.lock().unwrap() = false;
                        // Set is_paused to true before verification
                        *self.has_processed_assignments.lock().unwrap() = false;
                        // Fetch the current epoch validator periodically
                        if block_number % 10 == 0 {
                            match self.fetch_current_epoch_validator().await {
                                Ok(Some((validator, epoch_start))) => {
                                    println!(
                                        "Current epoch validator: {} (started at block {})",
                                        validator, epoch_start
                                    );

                                    // Check if we are the current validator
                                    if let Some(hips_account) = &self.hips_account_id {
                                        if *hips_account == validator {
                                            println!("We are the current epoch validator!");
                                        } else {
                                            println!("We are NOT the current epoch validator.");
                                        }
                                    }

                                    // Check if we're in the current epoch
                                    let epoch_length = Config::get().epoch_length;
                                    let epoch_end = epoch_start + epoch_length;
                                    let current_block_u64 = u64::from(block_number);
                                    if current_block_u64 >= epoch_start && current_block_u64 < epoch_end
                                    {
                                        println!(
                                            "Currently in epoch: block {} of {} (ends at {})",
                                            current_block_u64 - epoch_start,
                                            epoch_length,
                                            epoch_end
                                        );
                                    } else {
                                        println!(
                                            "Not in active epoch. Current block: {}, Epoch: {}-{}",
                                            current_block_u64, epoch_start, epoch_end
                                        );
                                    }
                                }
                                Ok(None) => {
                                    println!("No current epoch validator set");
                                }
                                Err(e) => {
                                    eprintln!("Error fetching current epoch validator: {}", e);
                                }
                            }
                        }

                        // Perform miner verification every 20 blocks, regardless of validator status
                        if block_number % 20 == 0 {
                            // Set is_paused to true before verification
                            *self.is_paused.lock().unwrap() = true;
                            let session_clone = self.session.clone();
                            let ipfs_entries_clone = self.ipfs_entries.clone();
                            let ipfs_client_clone = self.ipfs_client.clone();
                            let ipfs_node_url_clone = self.ipfs_node_url.clone();
                            let substrate_fetcher_clone = Arc::new(self.clone());
                            // Configure rate limiting (e.g., 5 requests per second)
                            let quota = Quota::per_second(NonZeroU32::new(5).unwrap());
                            let rate_limiter = Arc::new(RateLimiter::direct(quota));

                            let app_state = AppState {
                                session: session_clone,
                                ipfs_entries: ipfs_entries_clone,
                                ipfs_client: ipfs_client_clone,
                                ipfs_node_url: ipfs_node_url_clone,
                                substrate_fetcher: substrate_fetcher_clone,
                                rate_limiter,
                            };

                            let app_state_arc = Arc::new(app_state);

                            // Run the miner verification inline
                            match verify_miners_subset(
                                &app_state_arc,
                                3,
                                2,
                                verification_limit,
                            )
                            .await
                            {
                                Ok(metrics) => {

                                    // If enough metrics collected, submit them to the chain
                                    if !metrics.is_empty() {
                                        // Create another AppState for the transaction submission
                                        let app_state_for_tx = app_state_arc.clone();
                                        match submit_update_pin_check_metrics_transaction(
                                            &app_state_for_tx,
                                            metrics,
                                        )
                                        .await
                                        {
                                            Ok(_) => {
                                                println!(
                                                    "Successfully submitted miner verification metrics"
                                                );
                                                // Set is_paused back to false on successful transaction
                                                *self.is_paused.lock().unwrap() = false;
                                            }
                                            Err(e) => {
                                                eprintln!(
                                                    "Failed to submit miner verification metrics: {}",
                                                    e
                                                );
                                                // Set is_paused back to false on successful transaction
                                                *self.is_paused.lock().unwrap() = false;
                                            }
                                        }
                                    } else {
                                        // Set is_paused back to false if no metrics to submit
                                        *self.is_paused.lock().unwrap() = false;
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to verify miners: {}", e);
                                    // Set is_paused back to false on verification failure
                                    *self.is_paused.lock().unwrap() = false;
                                }
                            }
                        }

                        // Process data only every 15 blocks
                        if block_number % 15 == 0 {
                            println!(
                                "Processing execution unit and registration data for block {}",
                                block_number
                            );

                            // Handle ExecutionUnit data errors
                            match self
                                .fetch_and_update_execution_unit_data(block_hash, &metadata)
                                .await
                            {
                                Ok(_) => println!(
                                    "Successfully processed ExecutionUnit data for block {}",
                                    block_number
                                ),
                                Err(e) => {
                                    eprintln!(
                                        "Error processing ExecutionUnit data for block {}: {:?}",
                                        block_number, e
                                    );
                                    continue; // Skip to next block
                                }
                            }

                            // Handle Registration data errors
                            match self
                                .fetch_and_update_registration_data(block_hash, &metadata)
                                .await
                            {
                                Ok(_) => println!(
                                    "Successfully processed Registration data for block {}",
                                    block_number
                                ),
                                Err(e) => {
                                    eprintln!(
                                        "Error processing Registration data for block {}: {:?}",
                                        block_number, e
                                    );
                                    continue; // Skip to next block
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error fetching block: {:?}", e);
                    continue; // Skip to next block
                }
            }
        }

        Ok(())
    }

    pub async fn fetch_and_update_ipfs_data(
        &self,
        block_hash: subxt::utils::H256,
        metadata: &subxt::metadata::Metadata,
        state: &Arc<AppState>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let mut processed_request = false;
        for pallet in metadata.pallets() {
            if pallet.name() != "IpfsPallet" && pallet.name() != "ExecutionUnit" {
                continue;
            }

            if let Some(storage) = pallet.storage() {
                for entry in storage.entries() {
                    let entry_name = entry.name();

                    if !self.ipfs_entries.iter().any(|e| e == entry_name) {
                        continue;
                    }

                    match entry.entry_type() {
                        StorageEntryType::Map { hashers, .. } => {
                            if hashers.len() == 1 {
                                let storage_query =
                                    subxt::dynamic::storage("IpfsPallet", entry_name, vec![]);
                                let mut iter = self
                                    .api
                                    .storage()
                                    .at(block_hash)
                                    .iter(storage_query)
                                    .await?;

                                while let Some(result) = iter.next().await {
                                    match result {
                                        Ok(subxt::storage::StorageKeyValuePair {
                                            key_bytes,
                                            value,
                                            ..
                                        }) => {
                                            let decoded_key =
                                                decode_key(&key_bytes, &entry, metadata)
                                                    .unwrap_or_else(|_| hex::encode(&key_bytes));
                                            let decoded_value = decode_value(
                                                &value.encoded(),
                                                &entry,
                                                metadata,
                                            )?;

                                            let storage_value =
                                                serde_json::to_string(&decoded_value)?;
                                            self.update_blockchain_state(
                                                entry_name,
                                                decoded_key.clone(),
                                                storage_value.clone(),
                                            )
                                            .await?;
                                            if entry_name == "UserProfile" {
                                                if let serde_json::Value::String(cid) =
                                                    decoded_value
                                                {
                                                    match crate::ipfs_utils::fetch_ipfs_content(
                                                        &state, &cid,
                                                    )
                                                    .await
                                                    {
                                                        Ok(content) => {
                                                            if let Ok(content_json) =
                                                                serde_json::from_slice::<
                                                                    Vec<serde_json::Value>,
                                                                >(
                                                                    &content
                                                                )
                                                            {
                                                                for item in content_json {
                                                                    let file_hash = item
                                                                        .get("file_hash")
                                                                        .and_then(|fh| fh.as_array())
                                                                        .ok_or_else(|| {
                                                                            format!(
                                                                                "Missing or invalid file_hash in CID {}",
                                                                                cid
                                                                            )
                                                                        })?
                                                                        .iter()
                                                                        .map(|v| {
                                                                            v.as_u64().ok_or_else(|| {
                                                                                format!(
                                                                                    "Invalid file_hash element in CID {}",
                                                                                    cid
                                                                                )
                                                                            })
                                                                        })
                                                                        .collect::<Result<Vec<u64>, String>>()?
                                                                        .into_iter()
                                                                        .map(|v| v as u8)
                                                                        .collect::<Vec<u8>>();
                                                                    let decoded_file_hash =
                                                                        hex::decode(&file_hash)
                                                                            .unwrap();
                                                                    let file_hash_str =
                                                                        String::from_utf8_lossy(
                                                                            &decoded_file_hash,
                                                                        )
                                                                        .to_string();
                                                                    let item_value =
                                                                        serde_json::to_string(
                                                                            &item,
                                                                        )?;
                                                                    self.update_cids_info(
                                                                        &file_hash_str,
                                                                        &item_value,
                                                                    )
                                                                    .await?;
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            eprintln!(
                                                                "Error iterating map entry {}: {:?}",
                                                                entry_name, e
                                                            );
                                                            continue; // Skip to next entry
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Error iterating map entry {}: {:?}",
                                                entry_name, e
                                            );
                                            continue; // Skip to next entry
                                        }
                                    }
                                }
                            } else if hashers.len() == 2 {
                                let storage_query =
                                    subxt::dynamic::storage("IpfsPallet", entry_name, vec![]);
                                let mut iter = self
                                    .api
                                    .storage()
                                    .at(block_hash)
                                    .iter(storage_query)
                                    .await?;

                                while let Some(result) = iter.next().await {
                                    match result {
                                        Ok(subxt::storage::StorageKeyValuePair {
                                            key_bytes,
                                            value,
                                            ..
                                        }) => {
                                            let (decoded_key1, decoded_key2) =
                                                decode_double_key(
                                                    &key_bytes, &entry, metadata,
                                                )
                                                .unwrap_or_else(|_| {
                                                    (hex::encode(&key_bytes), String::new())
                                                });
                                            let decoded_value = decode_value(
                                                &value.encoded(),
                                                &entry,
                                                metadata,
                                            )?;

                                            let storage_key =
                                                format!("{}_{}", decoded_key1, decoded_key2);
                                            let storage_value =
                                                serde_json::to_string(&decoded_value)?;
                                            println!(
                                                "inserting key 1 : {:?}, key 2 {:?}: value : {:?}",
                                                decoded_key1, decoded_key2, decoded_value
                                            );
                                            self.update_blockchain_state(
                                                entry_name,
                                                storage_key,
                                                storage_value,
                                            )
                                            .await?;
                                            // Handle User Storage Requests for assignment
                                            if entry_name == "UserStorageRequests"
                                                && !decoded_value.is_null()
                                            {
                                                println!("found storage requests");
                                                processed_request = true;
                                                if let Some(_hips_account_id) = &self.hips_account_id
                                                {
                                                    let storage_request: StorageRequest =
                                                        serde_json::from_value(
                                                            decoded_value.clone(),
                                                        )?;
                                                    println!(
                                                        "found storage requests with hips key matched"
                                                    );
                                                    // Check if the storage request has already been processed
                                                    let check_query = "SELECT file_hash FROM blockchain.processed_storage_requests WHERE file_hash = $1 AND owner = $2";
                                                    // Adapted for sqlx: Using fetch_optional to check for existence
                                                    let already_processed: bool =
                                                        sqlx::query(check_query)
                                                            .bind(&storage_request.file_hash)
                                                            .bind(&storage_request.owner)
                                                            .fetch_optional(&*self.session)
                                                            .await?
                                                            .is_some();

                                                    if already_processed {
                                                        println!(
                                                            "storage requests already processed {:?}, decoded value is {:?}",
                                                            storage_request,
                                                            decoded_value.clone()
                                                        );
                                                        continue;
                                                    }

                                                    // Check if the storage request is currently in progress
                                                    let check_in_progress_query = "SELECT file_hash FROM blockchain.in_progress_storage_requests WHERE file_hash = $1 AND owner = $2";
                                                    // Adapted for sqlx
                                                    let is_in_progress: bool =
                                                        sqlx::query(check_in_progress_query)
                                                            .bind(&storage_request.file_hash)
                                                            .bind(&storage_request.owner)
                                                            .fetch_optional(&*self.session)
                                                            .await?
                                                            .is_some();

                                                    if is_in_progress {
                                                        println!(
                                                            "storage requests already In Progress {:?}, decoded value is {:?}",
                                                            storage_request,
                                                            decoded_value.clone()
                                                        );
                                                        continue;
                                                    }

                                                    // Mark the request as in-progress
                                                    let insert_in_progress_query = "INSERT INTO blockchain.in_progress_storage_requests (file_hash, owner) VALUES ($1, $2)";
                                                    // Adapted for sqlx
                                                    sqlx::query(insert_in_progress_query)
                                                        .bind(&storage_request.file_hash)
                                                        .bind(&storage_request.owner)
                                                        .execute(&*self.session)
                                                        .await?;
                                                   
                                                } else {
                                                    println!("hips key not found");
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Error iterating map entry {}: {:?}",
                                                entry_name, e
                                            );
                                            continue; // Skip to next entry
                                        }
                                    }
                                }
                            }
                        }
                        StorageEntryType::Plain(_) => {
                            let address = subxt::dynamic::storage("IpfsPallet", entry_name, vec![]);
                            if let Ok(Some(value)) =
                                self.api.storage().at(block_hash).fetch(&address).await
                            {
                                let decoded_value =
                                    decode_value(&value.encoded(), &entry, metadata)?;
                                let storage_value = serde_json::to_string(&decoded_value)?;

                                // Handle UserUnpinRequests for unpinning
                                if entry_name == "UserUnpinRequests" && !decoded_value.is_null() {
                                    processed_request = true;
                                    if let Some(_hips_account_id) = &self.hips_account_id {
                                        // Decode as Vec<UserUnpinRequest>
                                        let unpin_requests: Vec<UserUnpinRequest> =
                                            serde_json::from_value(decoded_value.clone())?;
                                        // Only proceed if there's at least one request
                                        if unpin_requests.len() > 0 {
                                            for unpin_request in unpin_requests {
                                                // Decode the file_hash from hex
                                                let decoded_file_hash = hex::decode(
                                                    &unpin_request.file_hash,
                                                )
                                                .map_err(|e| {
                                                    format!(
                                                        "Failed to decode file_hash {}: {}",
                                                        unpin_request.file_hash, e
                                                    )
                                                })?;
                                                let file_hash_str = String::from_utf8(decoded_file_hash.clone())
                                                    .map_err(|e| format!("Failed to convert decoded file_hash to string: {}", e))?;

                                                // Check if the unpin request has already been processed
                                                let check_query = "SELECT file_hash FROM blockchain.processed_unpin_requests WHERE file_hash = $1 AND owner = $2";
                                                // Adapted for sqlx
                                                let already_processed: bool =
                                                    sqlx::query(check_query)
                                                        .bind(&file_hash_str)
                                                        .bind(&unpin_request.owner)
                                                        .fetch_optional(&*self.session)
                                                        .await?
                                                        .is_some();

                                                if already_processed {
                                                    continue;
                                                }

                                                // Check if the unpin request is currently in progress
                                                let check_in_progress_query = "SELECT file_hash FROM blockchain.in_progress_unpin_requests WHERE file_hash = $1 AND owner = $2";
                                                // Adapted for sqlx
                                                let is_in_progress: bool =
                                                    sqlx::query(check_in_progress_query)
                                                        .bind(&file_hash_str)
                                                        .bind(&unpin_request.owner)
                                                        .fetch_optional(&*self.session)
                                                        .await?
                                                        .is_some();

                                                if is_in_progress {
                                                    continue;
                                                }

                                                // Mark the request as in-progress
                                                let insert_in_progress_query = "INSERT INTO blockchain.in_progress_unpin_requests (file_hash, owner) VALUES ($1, $2)";
                                                // Adapted for sqlx
                                                sqlx::query(insert_in_progress_query)
                                                    .bind(&file_hash_str)
                                                    .bind(&unpin_request.owner)
                                                    .execute(&*self.session)
                                                    .await
                                                    .map_err(|e| format!("Failed to mark unpin request as in-progress for file_hash {}: {}", file_hash_str, e))?;
                                                
                                            }
                                        } else {
                                            println!("No Unpin Requests yet!")
                                        }
                                    }
                                } else if entry_name == "RebalanceRequest" {
                                    if let Some(_hips_account_id) = &self.hips_account_id {
                                        if let serde_json::Value::Array(requests) = decoded_value {
                                            for request in requests {
                                                let miner_profile_id = request
                                                    .get("miner_profile_id")
                                                    .and_then(|v| v.as_str())
                                                    .ok_or_else(|| "Missing miner_profile_id")?
                                                    .to_string();
                                                let old_miner_id = request
                                                    .get("node_id")
                                                    .and_then(|v| v.as_str())
                                                    .ok_or_else(|| "Missing node_id")?
                                                    .to_string();

                                                // Skip if miner_profile_id is not empty (assuming empty string means None)
                                                if !miner_profile_id.is_empty() {
                                                    continue;
                                                }

                                                // Check if the rebalance request has already been processed
                                                let check_query = "SELECT old_miner_id FROM blockchain.processed_rebalance_requests WHERE old_miner_id = $1 AND miner_profile_id = $2";
                                                // Adapted for sqlx
                                                let already_processed: bool =
                                                    sqlx::query(check_query)
                                                        .bind(&old_miner_id)
                                                        .bind(&miner_profile_id)
                                                        .fetch_optional(&*self.session)
                                                        .await?
                                                        .is_some();

                                                if already_processed {
                                                    continue;
                                                }

                                                // Check if the rebalance request is currently in progress
                                                let check_in_progress_query = "SELECT old_miner_id FROM blockchain.in_progress_rebalance_requests WHERE old_miner_id = $1 AND miner_profile_id = $2";
                                                // Adapted for sqlx
                                                let is_in_progress: bool =
                                                    sqlx::query(check_in_progress_query)
                                                        .bind(&old_miner_id)
                                                        .bind(&miner_profile_id)
                                                        .fetch_optional(&*self.session)
                                                        .await?
                                                        .is_some();

                                                if is_in_progress {
                                                    continue;
                                                }

                                                // Mark the request as in-progress
                                                let insert_in_progress_query = "INSERT INTO blockchain.in_progress_rebalance_requests (old_miner_id, miner_profile_id) VALUES ($1, $2)";
                                                // Adapted for sqlx
                                                sqlx::query(insert_in_progress_query)
                                                    .bind(&old_miner_id)
                                                    .bind(&miner_profile_id)
                                                    .execute(&*self.session)
                                                    .await?;
                                            }
                                        }
                                    }
                                }

                                self.update_blockchain_state(
                                    entry_name,
                                    entry_name.to_string(),
                                    storage_value,
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
        }
        Ok(processed_request)
    }

    async fn update_blockchain_state(
        &self,
        table_name: &str,
        storage_key: String,
        storage_value: String,
    ) -> Result<(), Box<dyn std::error::Error>> {

        // Special case for nodemetrics data - parse the JSON and insert individual columns
        if table_name == "nodemetrics" {
            println!("Special handling for nodemetrics data");

            // Parse the JSON value
            let metrics_data: serde_json::Value = serde_json::from_str(&storage_value)?;

            // Extract fields
            let miner_id = metrics_data
                .get("miner_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let bandwidth_mbps = metrics_data
                .get("bandwidth_mbps")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let current_storage_bytes = metrics_data
                .get("current_storage_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let total_storage_bytes = metrics_data
                .get("total_storage_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let geolocation = metrics_data
                .get("geolocation")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let successful_pin_checks = metrics_data
                .get("successful_pin_checks")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let total_pin_checks = metrics_data
                .get("total_pin_checks")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let storage_proof_time_ms = metrics_data
                .get("storage_proof_time_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let storage_growth_rate = metrics_data
                .get("storage_growth_rate")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let latency_ms = metrics_data
                .get("latency_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let total_latency_ms = metrics_data
                .get("total_latency_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let total_times_latency_checked = metrics_data
                .get("total_times_latency_checked")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let avg_response_time_ms = metrics_data
                .get("avg_response_time_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let peer_count = metrics_data
                .get("peer_count")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let failed_challenges_count = metrics_data
                .get("failed_challenges_count")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let successful_challenges = metrics_data
                .get("successful_challenges")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let total_challenges = metrics_data
                .get("total_challenges")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let uptime_minutes = metrics_data
                .get("uptime_minutes")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let total_minutes = metrics_data
                .get("total_minutes")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let consecutive_reliable_days = metrics_data
                .get("consecutive_reliable_days")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let recent_downtime_hours = metrics_data
                .get("recent_downtime_hours")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let is_sev_enabled = metrics_data
                .get("is_sev_enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or_default();
            let ipfs_zfs_pool_size = metrics_data
                .get("ipfs_zfs_pool_size")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let ipfs_zfs_pool_alloc = metrics_data
                .get("ipfs_zfs_pool_alloc")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let ipfs_zfs_pool_free = metrics_data
                .get("ipfs_zfs_pool_free")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let ipfs_repo_size = metrics_data
                .get("ipfs_repo_size")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let ipfs_storage_max = metrics_data
                .get("ipfs_storage_max")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let cpu_model = metrics_data
                .get("cpu_model")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let cpu_cores = metrics_data
                .get("cpu_cores")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let memory_mb = metrics_data
                .get("memory_mb")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let free_memory_mb = metrics_data
                .get("free_memory_mb")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            let gpu_name = metrics_data
                .get("gpu_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let gpu_memory_mb = metrics_data
                .get("gpu_memory_mb")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;
            let hypervisor_disk_type = metrics_data
                .get("hypervisor_disk_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let vm_pool_disk_type = metrics_data
                .get("vm_pool_disk_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let vm_count = metrics_data
                .get("vm_count")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as i32;

            // JSON fields
            let disks = metrics_data
                .get("disks")
                .cloned()
                .unwrap_or(serde_json::Value::Array(Vec::new()));
            let disk_info = metrics_data
                .get("disk_info")
                .cloned()
                .unwrap_or(serde_json::Value::Array(Vec::new()));
            let zfs_info = metrics_data
                .get("zfs_info")
                .cloned()
                .unwrap_or(serde_json::Value::Array(Vec::new()));
            let raid_info = metrics_data
                .get("raid_info")
                .cloned()
                .unwrap_or(serde_json::Value::Array(Vec::new()));
            let primary_network_interface = metrics_data
                .get("primary_network_interface")
                .cloned()
                .unwrap_or(serde_json::Value::Null);

            // First check if the node_id exists with an UPDATE query
            let update_query = "
                UPDATE blockchain.ipfs_nodemetrics SET 
                    miner_id = $1,
                    bandwidth_mbps = $2,
                    current_storage_bytes = $3,
                    total_storage_bytes = $4,
                    geolocation = $5,
                    successful_pin_checks = $6,
                    total_pin_checks = $7,
                    storage_proof_time_ms = $8,
                    storage_growth_rate = $9,
                    latency_ms = $10,
                    total_latency_ms = $11,
                    total_times_latency_checked = $12,
                    avg_response_time_ms = $13,
                    peer_count = $14,
                    failed_challenges_count = $15,
                    successful_challenges = $16,
                    total_challenges = $17,
                    uptime_minutes = $18,
                    total_minutes = $19,
                    consecutive_reliable_days = $20,
                    recent_downtime_hours = $21,
                    is_sev_enabled = $22,
                    ipfs_zfs_pool_size = $23,
                    ipfs_zfs_pool_alloc = $24,
                    ipfs_zfs_pool_free = $25,
                    ipfs_repo_size = $26,
                    ipfs_storage_max = $27,
                    cpu_model = $28,
                    cpu_cores = $29,
                    memory_mb = $30,
                    free_memory_mb = $31,
                    gpu_name = $32,
                    gpu_memory_mb = $33,
                    hypervisor_disk_type = $34,
                    vm_pool_disk_type = $35,
                    vm_count = $36,
                    disks = $37,
                    disk_info = $38,
                    zfs_info = $39,
                    raid_info = $40,
                    primary_network_interface = $41,
                    updated_at = NOW()
                WHERE node_id = $42
            ";

            let result = sqlx::query(update_query)
                .bind(&miner_id)
                .bind(bandwidth_mbps as i64)
                .bind(current_storage_bytes as i64)
                .bind(total_storage_bytes as i64)
                .bind(&geolocation)
                .bind(successful_pin_checks)
                .bind(total_pin_checks)
                .bind(storage_proof_time_ms)
                .bind(storage_growth_rate as i64)
                .bind(latency_ms)
                .bind(total_latency_ms as i64)
                .bind(total_times_latency_checked)
                .bind(avg_response_time_ms)
                .bind(peer_count)
                .bind(failed_challenges_count)
                .bind(successful_challenges)
                .bind(total_challenges)
                .bind(uptime_minutes)
                .bind(total_minutes)
                .bind(consecutive_reliable_days)
                .bind(recent_downtime_hours)
                .bind(is_sev_enabled)
                .bind(&ipfs_zfs_pool_size)
                .bind(&ipfs_zfs_pool_alloc)
                .bind(&ipfs_zfs_pool_free)
                .bind(ipfs_repo_size as i64)
                .bind(ipfs_storage_max as i64)
                .bind(&cpu_model)
                .bind(cpu_cores)
                .bind(memory_mb as i64)
                .bind(free_memory_mb as i64)
                .bind(&gpu_name)
                .bind(gpu_memory_mb)
                .bind(&hypervisor_disk_type)
                .bind(&vm_pool_disk_type)
                .bind(vm_count)
                .bind(sqlx::types::Json(disks.clone()))
                .bind(sqlx::types::Json(disk_info.clone()))
                .bind(sqlx::types::Json(zfs_info.clone()))
                .bind(sqlx::types::Json(raid_info.clone()))
                .bind(sqlx::types::Json(primary_network_interface.clone()))
                .bind(&storage_key) // This is the node_id
                .execute(&*self.session)
                .await;

            match result {
                Ok(pgresult) => {
                    if pgresult.rows_affected() == 0 {
                        // No rows updated, means key doesn't exist yet, try inserting
                        println!(
                            "No existing row found for {}.{}, inserting new row",
                            table_name, storage_key
                        );

                        let insert_query = "
                            INSERT INTO blockchain.ipfs_nodemetrics (
                                node_id, miner_id, bandwidth_mbps, current_storage_bytes, total_storage_bytes,
                                geolocation, successful_pin_checks, total_pin_checks, storage_proof_time_ms,
                                storage_growth_rate, latency_ms, total_latency_ms, total_times_latency_checked,
                                avg_response_time_ms, peer_count, failed_challenges_count, successful_challenges,
                                total_challenges, uptime_minutes, total_minutes, consecutive_reliable_days,
                                recent_downtime_hours, is_sev_enabled, ipfs_zfs_pool_size, ipfs_zfs_pool_alloc,
                                ipfs_zfs_pool_free, ipfs_repo_size, ipfs_storage_max, cpu_model, cpu_cores,
                                memory_mb, free_memory_mb, gpu_name, gpu_memory_mb, hypervisor_disk_type,
                                vm_pool_disk_type, vm_count, disks, disk_info, zfs_info, raid_info,
                                primary_network_interface
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                                $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
                                $33, $34, $35, $36, $37, $38, $39, $40, $41, $42
                            )
                        ";

                        match sqlx::query(insert_query)
                            .bind(&storage_key) // This is the node_id
                            .bind(&miner_id)
                            .bind(bandwidth_mbps as i64)
                            .bind(current_storage_bytes as i64)
                            .bind(total_storage_bytes as i64)
                            .bind(&geolocation)
                            .bind(successful_pin_checks)
                            .bind(total_pin_checks)
                            .bind(storage_proof_time_ms)
                            .bind(storage_growth_rate as i64)
                            .bind(latency_ms)
                            .bind(total_latency_ms as i64)
                            .bind(total_times_latency_checked)
                            .bind(avg_response_time_ms)
                            .bind(peer_count)
                            .bind(failed_challenges_count)
                            .bind(successful_challenges)
                            .bind(total_challenges)
                            .bind(uptime_minutes)
                            .bind(total_minutes)
                            .bind(consecutive_reliable_days)
                            .bind(recent_downtime_hours)
                            .bind(is_sev_enabled)
                            .bind(&ipfs_zfs_pool_size)
                            .bind(&ipfs_zfs_pool_alloc)
                            .bind(&ipfs_zfs_pool_free)
                            .bind(ipfs_repo_size as i64)
                            .bind(ipfs_storage_max as i64)
                            .bind(&cpu_model)
                            .bind(cpu_cores)
                            .bind(memory_mb as i64)
                            .bind(free_memory_mb as i64)
                            .bind(&gpu_name)
                            .bind(gpu_memory_mb)
                            .bind(&hypervisor_disk_type)
                            .bind(&vm_pool_disk_type)
                            .bind(vm_count)
                            .bind(sqlx::types::Json(disks.clone()))
                            .bind(sqlx::types::Json(disk_info.clone()))
                            .bind(sqlx::types::Json(zfs_info.clone()))
                            .bind(sqlx::types::Json(raid_info.clone()))
                            .bind(sqlx::types::Json(primary_network_interface.clone()))
                            .execute(&*self.session)
                            .await
                        {
                            Ok(insert_result) => {
                                println!(
                                    "Successfully inserted row for {}.{}, rows affected: {}",
                                    table_name,
                                    storage_key,
                                    insert_result.rows_affected()
                                );
                            }
                            Err(e) => {
                                eprintln!(
                                    "Error inserting row for {}.{}: {}",
                                    table_name, storage_key, e
                                );
                                return Err(Box::new(e));
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Error updating row for {}.{}: {}",
                        table_name, storage_key, e
                    );
                    return Err(Box::new(e));
                }
            }

            return Ok(());
        }

        if table_name == "registration" {
            println!("Special handling for registration data");

            // Parse the JSON value
            let registration_data: serde_json::Value = serde_json::from_str(&storage_value)?;

            // Extract fields
            let node_id = registration_data
                .get("node_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let owner = registration_data
                .get("owner")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let status = registration_data
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let node_type = registration_data
                .get("node_type")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let ipfs_node_id = registration_data
                .get("ipfs_node_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let registered_at = registration_data
                .get("registered_at")
                .and_then(|v| v.as_u64())
                .unwrap_or_default();

            // First check if the node_id exists with an UPDATE query
            let update_query = "
                UPDATE blockchain.ipfs_registration SET 
                    owner = $1,
                    status = $2,
                    node_type = $3,
                    ipfs_node_id = $4,
                    registered_at = $5,
                    storage_value = $6
                WHERE node_id = $7
            ";

            let result = sqlx::query(update_query)
                .bind(&owner)
                .bind(&status)
                .bind(&node_type)
                .bind(&ipfs_node_id)
                .bind(registered_at as i64)
                .bind(sqlx::types::Json(registration_data.clone()))
                .bind(&node_id) // Use node_id from the JSON, not storage_key
                .execute(&*self.session)
                .await;

            match result {
                Ok(pgresult) => {
                    if pgresult.rows_affected() == 0 {
                        // No rows updated, means key doesn't exist yet, try inserting
                        println!(
                            "No existing row found for {}.{}, inserting new row",
                            table_name, node_id
                        );

                        let insert_query = "
                            INSERT INTO blockchain.ipfs_registration (
                                node_id, owner, status, node_type, ipfs_node_id, registered_at, storage_value
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7
                            )
                        ";

                        match sqlx::query(insert_query)
                            .bind(&node_id) // Use node_id from the JSON, not storage_key
                            .bind(&owner)
                            .bind(&status)
                            .bind(&node_type)
                            .bind(&ipfs_node_id)
                            .bind(registered_at as i64)
                            .bind(sqlx::types::Json(registration_data.clone()))
                            .execute(&*self.session)
                            .await
                        {
                            Ok(insert_result) => {
                                println!(
                                    "Successfully inserted row for {}.{}, rows affected: {}",
                                    table_name,
                                    node_id,
                                    insert_result.rows_affected()
                                );
                            }
                            Err(e) => {
                                eprintln!(
                                    "Error inserting row for {}.{}: {}",
                                    table_name, node_id, e
                                );
                                return Err(Box::new(e));
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error updating row for {}.{}: {}", table_name, node_id, e);
                    return Err(Box::new(e));
                }
            }

            // Also update the general registration table
            let reg_query =
                "UPDATE blockchain.registration SET storage_value = $1 WHERE storage_key = $2";

            // Adapted for sqlx
            let result = sqlx::query(reg_query)
                .bind(&storage_value) // Use the original storage_value string
                .bind(&storage_key) // Use the original storage_key
                .execute(&*self.session)
                .await;

            match result {
                Ok(pgresult) => {
                    if pgresult.rows_affected() == 0 {
                        // No rows updated, means key doesn't exist yet, try inserting
                        println!(
                            "No existing row found for registration.{}, inserting new row",
                            storage_key
                        );

                        let insert_query = "INSERT INTO blockchain.registration (storage_key, storage_value) VALUES ($1, $2)";

                        match sqlx::query(insert_query)
                            .bind(&storage_key)
                            .bind(&storage_value)
                            .execute(&*self.session)
                            .await
                        {
                            Ok(insert_result) => {
                                println!(
                                    "Successfully inserted row for registration.{}, rows affected: {}",
                                    storage_key,
                                    insert_result.rows_affected()
                                );
                            }
                            Err(e) => {
                                eprintln!(
                                    "Error inserting row for registration.{}: {}",
                                    storage_key, e
                                );
                                return Err(Box::new(e));
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error updating row for registration.{}: {}", storage_key, e);
                    return Err(Box::new(e));
                }
            }

            return Ok(());
        }

        // Regular handling for other tables
        // Validate JSON
        if serde_json::from_str::<serde_json::Value>(&storage_value).is_err() {
            eprintln!(
                "Invalid JSON for table {} and key {}: {}",
                table_name, storage_key, storage_value
            );
            return Err(format!(
                "Invalid JSON for table {} and key {}",
                table_name, storage_key
            )
            .into());
        }
        // Parse storage_value to JsonValue for sqlx binding
        let json_storage_value: JsonValue = serde_json::from_str(&storage_value)?;

        let query = format!(
            "UPDATE blockchain.ipfs_{} SET storage_value = $1 WHERE storage_key = $2",
            table_name.to_lowercase()
        );

        // Adapted for sqlx
        let result = sqlx::query(&query)
            .bind(sqlx::types::Json(json_storage_value)) // Explicitly wrap with Json wrapper
            .bind(&storage_key)
            .execute(&*self.session)
            .await;

        match result {
            Ok(pgresult) => {
                if pgresult.rows_affected() == 0 {
                    // No rows updated, means key doesn't exist yet, try inserting
                    println!(
                        "No existing row found for {}.{}, inserting new row",
                        table_name, storage_key
                    );

                    let insert_query = format!(
                        "INSERT INTO blockchain.ipfs_{} (storage_key, storage_value) VALUES ($1, $2)",
                        table_name.to_lowercase()
                    );

                    // Reparse to get a fresh JsonValue
                    let json_for_insert: JsonValue = serde_json::from_str(&storage_value)?;

                    match sqlx::query(&insert_query)
                        .bind(&storage_key)
                        .bind(sqlx::types::Json(json_for_insert))
                        .execute(&*self.session)
                        .await
                    {
                        Ok(insert_result) => {
                            println!(
                                "Successfully inserted row for {}.{}, rows affected: {}",
                                table_name,
                                storage_key,
                                insert_result.rows_affected()
                            );
                        }
                        Err(e) => {
                            eprintln!(
                                "Error inserting row for {}.{}: {}",
                                table_name, storage_key, e
                            );
                            return Err(Box::new(e));
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Error updating row for {}.{}: {}",
                    table_name, storage_key, e
                );
                return Err(Box::new(e));
            }
        }

        Ok(())
    }

    // store json object of user profile having this cid 
    // here cid is decoded 
    async fn update_cids_info(
        &self,
        hash_key: &str,
        item_value: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Parse item_value to JsonValue for sqlx binding
        let json_item_value: JsonValue = serde_json::from_str(item_value)?;

        let query = "INSERT INTO blockchain.cidsInfo (hash_key, item_value) VALUES ($1, $2)";

        // Adapted for sqlx
        sqlx::query(query)
            .bind(hash_key)
            .bind(sqlx::types::Json(json_item_value)) // Explicitly wrap with Json wrapper
            .execute(&*self.session)
            .await?;
        Ok(())
    }

    async fn fetch_and_update_registration_data(
        &self,
        block_hash: subxt::utils::H256,
        metadata: &subxt::metadata::Metadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!(
            "Starting fetch_and_update_registration_data for block hash: {:?}",
            block_hash
        );

        for pallet in metadata.pallets() {
            if pallet.name() != "Registration" {
                continue;
            }

            println!("Found Registration pallet in metadata");

            if let Some(storage) = pallet.storage() {
                for entry in storage.entries() {
                    let entry_name = entry.name();

                    // We're specifically interested in NodeMetrics
                    if entry_name != "ColdkeyNodeRegistration" && entry_name != "NodeRegistration" {
                        continue;
                    }

                    println!("Processing registration entry: {}", entry_name);

                    match entry.entry_type() {
                        StorageEntryType::Map { hashers, .. } => {
                            if hashers.len() == 1 {
                                let storage_query =
                                    subxt::dynamic::storage("Registration", entry_name, vec![]);
                                println!("Querying storage for Registration.{}", entry_name);

                                let mut iter = self
                                    .api
                                    .storage()
                                    .at(block_hash)
                                    .iter(storage_query)
                                    .await?;

                                let mut count = 0;
                                while let Some(result) = iter.next().await {
                                    match result {
                                        Ok(StorageKeyValuePair {
                                            key_bytes, value, ..
                                        }) => {
                                            count += 1;
                                            let decoded_key =
                                                decode_key(&key_bytes, &entry, metadata)
                                                    .unwrap_or_else(|_| hex::encode(&key_bytes));
                                            let decoded_value = Self::decode_registration_value(
                                                &value.encoded(),
                                                &entry,
                                                metadata,
                                            )?;

                                            let storage_value =
                                                serde_json::to_string(&decoded_value)?;

                                            println!(
                                                "Updating registration data for key: {}",
                                                decoded_key
                                            );
                                            self.update_blockchain_state(
                                                "registration",
                                                decoded_key.clone(),
                                                storage_value,
                                            )
                                            .await?;
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Error iterating map entry {}: {:?}",
                                                entry_name, e
                                            );
                                            continue; // Skip to next entry
                                        }
                                    }
                                }

                                println!(
                                    "Processed {} entries for Registration.{}",
                                    count, entry_name
                                );
                            }
                        }
                        _ => continue,
                    }
                }
            }
        }

        println!("Completed fetch_and_update_registration_data");
        Ok(())
    }

    async fn fetch_and_update_execution_unit_data(
        &self,
        block_hash: subxt::utils::H256,
        metadata: &subxt::metadata::Metadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!(
            "Starting fetch_and_update_execution_unit_data for block hash: {:?}",
            block_hash
        );

        for pallet in metadata.pallets() {
            if pallet.name() != "ExecutionUnit" {
                continue;
            }

            println!("Found ExecutionUnit pallet in metadata");

            if let Some(storage) = pallet.storage() {
                for entry in storage.entries() {
                    let entry_name = entry.name();

                    // We're specifically interested in NodeMetrics
                    if entry_name != "NodeMetrics" {
                        continue;
                    }

                    println!("Processing execution unit entry: {}", entry_name);

                    match entry.entry_type() {
                        StorageEntryType::Map { hashers, .. } => {
                            if hashers.len() == 1 {
                                let storage_query =
                                    subxt::dynamic::storage("ExecutionUnit", entry_name, vec![]);
                                println!("Querying storage for ExecutionUnit.{}", entry_name);

                                let mut iter = self
                                    .api
                                    .storage()
                                    .at(block_hash)
                                    .iter(storage_query)
                                    .await?;

                                let mut count = 0;
                                while let Some(result) = iter.next().await {
                                    match result {
                                        Ok(StorageKeyValuePair {
                                            key_bytes, value, ..
                                        }) => {
                                            count += 1;
                                            let decoded_key =
                                                decode_key(&key_bytes, &entry, metadata)
                                                    .unwrap_or_else(|_| hex::encode(&key_bytes));
                                            let decoded_value = Self::decode_execution_unit_value(
                                                &value.encoded(),
                                                &entry,
                                                metadata,
                                            )?;

                                            let storage_value =
                                                serde_json::to_string(&decoded_value)?;

                                            println!(
                                                "Updating nodemetrics data for key: {}",
                                                decoded_key
                                            );
                                            self.update_blockchain_state(
                                                "nodemetrics", // Store in nodemetrics table
                                                decoded_key.clone(),
                                                storage_value,
                                            )
                                            .await?;
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Error iterating map entry {}: {:?}",
                                                entry_name, e
                                            );
                                            continue; // Skip to next entry
                                        }
                                    }
                                }

                                println!(
                                    "Processed {} entries for ExecutionUnit.{}",
                                    count, entry_name
                                );
                            }
                        }
                        _ => continue,
                    }
                }
            }
        }

        println!("Completed fetch_and_update_execution_unit_data");
        Ok(())
    }

    fn decode_registration_value(
        value: &[u8],
        entry: &StorageEntryMetadata,
        _metadata: &subxt::metadata::Metadata,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut input = &value[..];
        let entry_name = entry.name();
        if entry_name == "ColdkeyNodeRegistration" {
            match Option::<NodeInfo<u32, AccountId32>>::decode(&mut input) {
                Ok(option_decoded) => {
                    return match option_decoded {
                        Some(node_info) => {
                            let node_id = String::from_utf8(node_info.node_id.clone())
                                .unwrap_or_else(|_| hex::encode(&node_info.node_id));

                            let ipfs_node_id = node_info.ipfs_node_id.map(|id| {
                                String::from_utf8(id.clone()).unwrap_or_else(|_| hex::encode(&id))
                            });

                            // input = &input[input.len().saturating_sub(32)..];
                            // if input.len() < 32 {
                            //     return Err(format!(
                            //         "Not enough data to decode AccountId32: only {} bytes remaining",
                            //         input.len()
                            //     )
                            //     .into());
                            // }
                            // let owner = AccountId32::decode(&mut input)
                            // .map_err(|e| format!("Failed to decode owner: {}", e))?;

                            Ok(json!({
                                "node_id": node_id,
                                "node_type": match node_info.node_type {
                                    NodeType::Validator => "Validator",
                                    NodeType::StorageMiner => "StorageMiner",
                                    NodeType::StorageS3 => "StorageS3",
                                    NodeType::ComputeMiner => "ComputeMiner",
                                    NodeType::GpuMiner => "GpuMiner",
                                },
                                "ipfs_node_id": ipfs_node_id,
                                "status": match node_info.status {
                                    Status::Online => "Online",
                                    Status::Degraded => "Degraded",
                                    Status::Offline => "Offline",
                                },
                                "registered_at": node_info.registered_at,
                                "owner": "",
                            }))
                        }
                        None => Ok(json!(null)),
                    };
                }
                Err(_e) => {
                    return Ok(json!(hex::encode(value)));
                }
            }
        } else if entry_name == "NodeRegistration" {
            match Option::<NodeInfo<u32, AccountId32>>::decode(&mut input) {
                Ok(option_decoded) => {
                    return match option_decoded {
                        Some(node_info) => {
                            let node_id = String::from_utf8(node_info.node_id.clone())
                                .unwrap_or_else(|_| hex::encode(&node_info.node_id));

                            let ipfs_node_id = node_info.ipfs_node_id.map(|id| {
                                String::from_utf8(id.clone()).unwrap_or_else(|_| hex::encode(&id))
                            });
                            // input = &input[input.len().saturating_sub(32)..];
                            // let owner = AccountId32::decode(&mut input)
                            // .map_err(|e| format!("Failed to decode owner: {}", e))?;

                            Ok(json!({
                                "node_id": node_id,
                                "node_type": match node_info.node_type {
                                    NodeType::Validator => "Validator",
                                    NodeType::StorageMiner => "StorageMiner",
                                    NodeType::StorageS3 => "StorageS3",
                                    NodeType::ComputeMiner => "ComputeMiner",
                                    NodeType::GpuMiner => "GpuMiner",
                                },
                                "ipfs_node_id": ipfs_node_id,
                                "status": match node_info.status {
                                    Status::Online => "Online",
                                    Status::Degraded => "Degraded",
                                    Status::Offline => "Offline",
                                },
                                "registered_at": node_info.registered_at,
                                "owner": "",
                            }))
                        }
                        None => Ok(json!(null)),
                    };
                }
                Err(_e) => {
                    return Ok(json!(hex::encode(value)));
                }
            }
        }
        Ok(json!({}))
    }

    fn decode_execution_unit_value(
        value: &[u8],
        entry: &StorageEntryMetadata,
        _metadata: &subxt::metadata::Metadata,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut input = &value[..];
        let entry_name = entry.name();
        if entry_name == "NodeMetrics" {
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
                return Ok(json!(null));
            }
        }
        Ok(json!(hex::encode(value)))
    }

    /// Fetches the current epoch validator from the chain
    pub async fn fetch_current_epoch_validator(
        &self,
    ) -> Result<Option<(AccountId32, u64)>, String> {
        println!("Fetching current epoch validator...");

        // Create the storage query for IpfsPallet.currentEpochValidator
        let storage_query = subxt::dynamic::storage("IpfsPallet", "CurrentEpochValidator", vec![]);

        // Fetch the latest finalized block
        let block_hash = self
            .api
            .blocks()
            .at_latest()
            .await
            .map_err(|e| format!("Failed to get latest block: {}", e))?
            .hash();

        // Execute the query
        let result = self
            .api
            .storage()
            .at(block_hash)
            .fetch(&storage_query)
            .await
            .map_err(|e| format!("Failed to fetch storage: {}", e))?;

        if let Some(value) = result {
            // Decode the value as Option<(AccountId32, u64)>
            let mut input = &value.encoded()[..];

            // Check if the option is Some (1) or None (0)
            let option_byte = input.first().ok_or("Empty input for Option")?;
            if *option_byte != 1 {
                // It's None
                return Ok(None);
            }

            // Skip the Some byte
            input = &input[1..];

            // Decode the AccountId32
            let account_id = AccountId32::decode(&mut input)
                .map_err(|e| format!("Failed to decode account ID: {}", e))?;

            // Decode the u64 block number
            let block_number = u64::decode(&mut input)
                .map_err(|e| format!("Failed to decode block number: {}", e))?;

            println!(
                "Current epoch validator: {} at block {}",
                account_id, block_number
            );

            // Update the cached value
            let mut cached_value = self.current_epoch_validator.lock().unwrap();
            *cached_value = Some((account_id.clone(), block_number));

            return Ok(Some((account_id, block_number)));
        }

        Ok(None)
    }

    /// Check if the local validator is the current epoch validator
    pub async fn is_current_epoch_validator(&self) -> Result<bool, String> {
        // First, update the current epoch validator info
        let epoch_validator = self.fetch_current_epoch_validator().await?;

        // Then check if our validator matches
        if let Some((validator_account, _block_number)) = epoch_validator {
            if let Some(hips_account) = &self.hips_account_id {
                return Ok(*hips_account == validator_account);
            }
        }

        Ok(false)
    }

    /// Check if we're in the current validator's epoch
    pub async fn is_in_current_validator_epoch(&self, current_block: u64) -> Result<bool, String> {
        // Get the current epoch validator
        let epoch_validator = self.fetch_current_epoch_validator().await?;

        if let Some((_validator_account, epoch_start_block)) = epoch_validator {
            // Get the epoch length from config
            let epoch_length = Config::get().epoch_length;

            // Check if the current block is within the current epoch
            let epoch_end_block = epoch_start_block + epoch_length;

            return Ok(current_block >= epoch_start_block && current_block < epoch_end_block);
        }

        Ok(false)
    }
}