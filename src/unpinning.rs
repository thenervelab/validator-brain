use crate::AppState;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::error::Error;
use crate::helpers::{read_json_array, write_json_array};

pub async fn process_pending_unpin_requests(
    app_state: &AppState,
) -> Result<(), Box<dyn Error>> {
    // Start a database transaction
    let mut tx = app_state.session.begin().await?;

    // Step 1: Query in-progress unpin requests within transaction
    let in_progress_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT file_hash, owner FROM blockchain.in_progress_unpin_requests"
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("Failed to query in-progress unpin requests: {}", e))?;

    if in_progress_rows.is_empty() {
        println!("No in-progress unpin requests found.");
        return Ok(());
    }

    // Prepare collections for results
    let mut miner_unpin_requests: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut processed_requests = Vec::new();

    // Process each unpin request
    for (file_hash, owner) in in_progress_rows {
        // Step 2: Fetch CID info from cidsInfo table
        let cid_info: Option<(String,)> = sqlx::query_as(
            "SELECT item_value FROM blockchain.cidsInfo WHERE hash_key = $1"
        )
        .bind(&file_hash)
        .fetch_optional(&mut *tx)
        .await?;

        let cid_info = match cid_info {
            Some((info,)) => info,
            None => {
                eprintln!("No CID info found for file hash: {}", file_hash);
                continue;
            }
        };

        // Parse the CID info JSON
        let cid_data: Value = serde_json::from_str(&cid_info)?;
        
        // Get miner IDs from the CID info
        let miner_ids = match cid_data["miner_ids"].as_array() {
            Some(ids) => ids.iter()
                .filter_map(|id| id.as_str().map(String::from))
                .collect::<Vec<String>>(),
            None => {
                eprintln!("No miner IDs found in CID info for file hash: {}", file_hash);
                continue;
            }
        };

        // Step 3: Process each miner's profile to remove the CID
        for miner_id in miner_ids {
            let path = format!("profile/miners/{}.json", miner_id);
            let mut existing = read_json_array(&path).unwrap_or_default();
            
            // Filter out the entry with this file_hash
            let original_len = existing.len();
            existing.retain(|entry| {
                if let Some(entry_hash) = entry["file_hash"].as_str() {
                    entry_hash != file_hash
                } else {
                    true
                }
            });
            
            // If we removed something, update the file
            if existing.len() != original_len {
                write_json_array(&path, &existing)?;
                
                // Track the unpin request for this miner (without timestamp)
                let unpin_request = json!({
                    "file_hash": file_hash.clone(),
                    "owner": owner.clone()
                });
                
                miner_unpin_requests.entry(miner_id)
                    .or_default()
                    .push(unpin_request);
            }
        }

        // Mark this request for processing
        processed_requests.push((file_hash, owner));
    }

    // Step 4: Batch update database
    for (file_hash, owner) in processed_requests {
        // Delete from in-progress
        sqlx::query(
            "DELETE FROM blockchain.in_progress_unpin_requests WHERE file_hash = $1 AND owner = $2"
        )
        .bind(&file_hash)
        .bind(&owner)
        .execute(&mut *tx)
        .await?;

        // Insert to processed if not exists (using NOW() in the query)
        let exists: Option<(i32,)> = sqlx::query_as(
            "SELECT 1 FROM blockchain.processed_unpin_requests WHERE file_hash = $1 AND owner = $2"
        )
        .bind(&file_hash)
        .bind(&owner)
        .fetch_optional(&mut *tx)
        .await?;

        if exists.is_none() {
            sqlx::query(
                "INSERT INTO blockchain.processed_unpin_requests (file_hash, owner, processed_at) 
                 VALUES ($1, $2, NOW())"
            )
            .bind(&file_hash)
            .bind(&owner)
            .execute(&mut *tx)
            .await?;
        }
    }

    // Commit transaction
    tx.commit().await?;

    // Step 5: Write unpin confirmation to miner logs if needed
    for (miner_id, requests) in miner_unpin_requests {
        let path = format!("profile/miners/{}_unpins.json", miner_id);
        let mut existing = read_json_array(&path).unwrap_or_default();
        existing.extend(requests);
        write_json_array(&path, &existing)?;
    }

    Ok(())
}