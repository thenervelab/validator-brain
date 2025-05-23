use crate::config::Config;
use crate::substrate_fetcher::custom_runtime;
use crate::substrate_fetcher::custom_runtime::runtime_types::ipfs_pallet::types::StorageRequestUpdate;
use crate::substrate_fetcher::custom_runtime::runtime_types::pallet_execution_unit::types::MinerPinMetrics;
use crate::types::*;
use std::fs;
use std::str::FromStr;
use subxt::ext::sp_core::crypto::Pair as SubxtPair;
use subxt::ext::sp_core::sr25519;
use subxt::tx::PairSigner;
use subxt::utils::AccountId32;
use crate::helpers::find_hips_key;

pub async fn submit_update_pin_check_metrics_transaction(
    state: &AppState,
    miners_metrics: Vec<MinerPinMetrics>,
) -> Result<(), String> {
    // Acquire the lock to ensure exclusive transaction submission
    let _lock = state.substrate_fetcher.tx_submission_lock.lock().await;
    // Load key file
    let key_file = find_hips_key(&Config::get().keystore_path)
        .map_err(|e| format!("Failed to find HIPS key: {}", e))?
        .ok_or("HIPS key not found in keystore")?;

    let key_path = std::path::Path::new(&Config::get().keystore_path).join(key_file);

    let raw =
        fs::read_to_string(&key_path).map_err(|e| format!("Failed to read key file: {}", e))?;
    let phrase = raw.trim().trim_matches('"');
    let keypair = sr25519::Pair::from_string(phrase, None)
        .map_err(|e| format!("Failed to parse keypair: {}", e))?;

    let signer = PairSigner::new(keypair.clone());
    let api = &state.substrate_fetcher.api;

    // Construct the transaction
    let tx = custom_runtime::tx()
        .execution_unit() // Adjust to match your pallet name
        .update_pin_check_metrics(miners_metrics);

    let tx_hash = api
        .tx()
        .sign_and_submit_then_watch_default(&tx, &signer)
        .await
        .map_err(|e| format!("Failed to submit transaction: {}", e))?
        .wait_for_finalized_success()
        .await
        .map_err(|e| format!("Transaction failed: {}", e))?
        .extrinsic_hash();

    println!("Transaction submitted with hash: {:?}", tx_hash);

    println!(
        "update_pin_check_metrics transaction submitted with hash: {:?}",
        tx_hash
    );
    Ok(())
}

// pub async fn delete_in_progress_unpin_request(
//     session: &Arc<PgPool>,
//     _state: &AppState,
//     cid: &str,
//     owner: &str,
// ) -> Result<(), String> {
//     let delete_query =
//         "DELETE FROM blockchain.in_progress_unpin_requests WHERE file_hash = $1 AND owner = $2";

//     // Dereference the Arc<PgPool> to get &PgPool which implements Executor
//     sqlx::query(delete_query)
//         .bind(cid)
//         .bind(owner)
//         .execute(&**session) // Note the double dereference
//         .await
//         .map_err(|e| format!("Failed to delete in-progress unpin request: {}", e))?;

//     Ok(())
// }

// pub async fn submit_remove_bad_unpin_request_transaction(
//     state: &AppState,
//     file_hash: String,
// ) -> Result<(), String> {
//     use crate::substrate_fetcher::custom_runtime::runtime_types::bounded_collections::bounded_vec::BoundedVec;
//     let main_req_hash = BoundedVec(file_hash.as_bytes().to_vec());

//     // Create the transaction
//     let tx = custom_runtime::tx()
//         .ipfs_pallet()
//         .remove_bad_unpin_request(main_req_hash);

//     // Load key file
//     let key_file = find_hips_key(&Config::get().keystore_path)
//         .map_err(|e| format!("Failed to find HIPS key: {}", e))?
//         .ok_or("HIPS key not found in keystore")?;
//     let key_path = std::path::Path::new(&Config::get().keystore_path).join(key_file);
//     let raw =
//         fs::read_to_string(&key_path).map_err(|e| format!("Failed to read key file: {}", e))?;
//     let phrase = raw.trim().trim_matches('"');
//     let keypair = sr25519::Pair::from_string(phrase, None)
//         .map_err(|e| format!("Failed to parse keypair: {}", e))?;
//     let signer = PairSigner::new(keypair);

//     // Submit transaction
//     let api = &state.substrate_fetcher.api;
//     let tx_hash = api
//         .tx()
//         .sign_and_submit_then_watch_default(&tx, &signer)
//         .await
//         .map_err(|e| format!("Failed to submit transaction: {}", e))?
//         .wait_for_finalized_success()
//         .await
//         .map_err(|e| format!("Transaction failed: {}", e))?
//         .extrinsic_hash();

//     println!(
//         "remove_bad_unpin_request transaction submitted with hash: {:?}",
//         tx_hash
//     );
//     Ok(())
// }

// /// Remove a bad storage request from the blockchain
// pub async fn remove_bad_storage_request_via_subxt(
//     state: &AppState,
//     file_hash: &str,
//     _bounded_vec: &str,
//     _storage_request_owner: AccountId32,
// ) -> Result<(), String> {
//     use custom_runtime::runtime_types::bounded_collections::bounded_vec::BoundedVec;

//     let file_hash = BoundedVec(file_hash.as_bytes().to_vec());
//     // let new_blacklist_hash = BoundedVec(bounded_vec.as_bytes().to_vec());

//     let tx = custom_runtime::tx()
//         .ipfs_pallet()
//         .remove_bad_storage_request(
//             file_hash,
//             // new_blacklist_hash,
//             // storage_request_owner
//         );

//     // Load key file
//     let key_file = find_hips_key(&Config::get().keystore_path)
//         .map_err(|e| format!("Failed to find HIPS key: {}", e))?
//         .ok_or("HIPS key not found in keystore")?;

//     let key_path = std::path::Path::new(&Config::get().keystore_path).join(key_file);

//     let raw =
//         fs::read_to_string(&key_path).map_err(|e| format!("Failed to read key file: {}", e))?;
//     // Trim the surrounding quotes if present
//     let phrase = raw.trim().trim_matches('"');
//     let keypair = sr25519::Pair::from_string(phrase, None)
//         .map_err(|e| format!("Failed to parse keypair: {}", e))?;

//     let signer = PairSigner::new(keypair);

//     // Submit tx
//     println!("Submitting assign pin transaction");
//     let api = &state.substrate_fetcher.api;
//     let tx_hash = api
//         .tx()
//         .sign_and_submit_then_watch_default(&tx, &signer)
//         .await
//         .map_err(|e| format!("Failed to submit transaction: {}", e))?
//         .wait_for_finalized_success()
//         .await
//         .map_err(|e| format!("Transaction failed: {}", e))?
//         .extrinsic_hash();

//     println!("Transaction submitted with hash: {:?}", tx_hash);
//     Ok(())
// }

pub async fn submit_storage_requests_transactions(
    state: &AppState,
    results: &Vec<ProcessStorageResponse>,
    miner_profiles: &Vec<MinerProfileItem>,
) -> Result<(), String> {
    use custom_runtime::runtime_types::bounded_collections::bounded_vec::BoundedVec;
    use custom_runtime::runtime_types::ipfs_pallet::types::MinerProfileItem;
    // Acquire the lock to ensure exclusive transaction submission
    let _lock = state.substrate_fetcher.tx_submission_lock.lock();

    let mut update_requests: Vec<StorageRequestUpdate<AccountId32>> = Vec::new();

    for result in results {
        let storage_request_owner = AccountId32::from_str(&result.storage_request_owner)
            .map_err(|e| format!("Invalid owner address: {}", e))?;

        let file_size = result.file_size;
        let user_profile_cid = BoundedVec(result.user_profile_cid.as_bytes().to_vec());
        let main_req_hash = BoundedVec(result.storage_request_file_hash.as_bytes().to_vec());

        let updated_storage_request = StorageRequestUpdate {
            storage_request_owner,
            storage_request_file_hash: main_req_hash,
            file_size,
            user_profile_cid,
        };
        update_requests.push(updated_storage_request);
    }

    let updated_miner_profiles: Vec<MinerProfileItem> = miner_profiles
        .iter()
        .map(|item| MinerProfileItem {
            miner_node_id: BoundedVec(item.miner_node_id.as_bytes().to_vec()),
            cid: BoundedVec(item.cid.as_bytes().to_vec()),
            files_count: item.files_count,
            files_size: item.files_size,
        })
        .collect();

    let tx = custom_runtime::tx()
        .ipfs_pallet()
        .update_pin_and_storage_requests(update_requests, updated_miner_profiles);

    // Load key file
    let key_file = find_hips_key(&Config::get().keystore_path)
        .map_err(|e| format!("Failed to find HIPS key: {}", e))?
        .ok_or("HIPS key not found in keystore")?;

    let key_path = std::path::Path::new(&Config::get().keystore_path).join(key_file);

    let raw =
        fs::read_to_string(&key_path).map_err(|e| format!("Failed to read key file: {}", e))?;
    // Trim the surrounding quotes if present
    let phrase = raw.trim().trim_matches('"');
    let keypair = sr25519::Pair::from_string(phrase, None)
        .map_err(|e| format!("Failed to parse keypair: {}", e))?;

    let signer = PairSigner::new(keypair.clone());
    let api = &state.substrate_fetcher.api;

    let tx_hash = api
        .tx()
        .sign_and_submit_then_watch_default(&tx, &signer)
        .await
        .map_err(|e| format!("Failed to submit transaction: {}", e))?
        .wait_for_finalized_success()
        .await
        .map_err(|e| format!("Transaction failed: {}", e))?
        .extrinsic_hash();

    println!("Transaction submitted with hash: {:?}", tx_hash);

    Ok(())
}

