use crate::AppState;
use crate::ipfs_utils::fetch_ipfs_contents;
use std::fs;

pub async fn reconstruct_profiles_to_files(
    app_state: &AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create profile directories if they don't exist
    fs::create_dir_all("profile/miners")
        .map_err(|e| format!("Failed to create profile/miners directory: {}", e))?;
    fs::create_dir_all("profile/users")
        .map_err(|e| format!("Failed to create profile/users directory: {}", e))?;

    // Fetch all miner profiles
    let miner_query = "
        SELECT storage_key, storage_value
        FROM blockchain.ipfs_minerprofile
    ";
    let miner_rows: Vec<(String, String)> = sqlx::query_as(miner_query)
        .fetch_all(&*app_state.session)
        .await
        .map_err(|e| format!("Failed to query miner profiles: {}", e))?;

    // Batch fetch all miner profile contents
    let miner_cids: Vec<String> = miner_rows.iter().map(|(_, cid)| cid.clone()).collect();
    let miner_contents = fetch_ipfs_contents(app_state, &miner_cids).await;

    // Process miner profiles
    for (miner_id, cid) in miner_rows {
        match miner_contents.get(&cid) {
            Some(Ok(content)) => match serde_json::from_slice::<serde_json::Value>(content) {
                Ok(json) => {
                    let miner_path = format!("profile/miners/{}.json", miner_id);
                    let json_str = serde_json::to_string_pretty(&json).map_err(|e| {
                        format!("Failed to serialize JSON for miner {}: {}", miner_id, e)
                    })?;
                    fs::write(&miner_path, json_str)
                        .map_err(|e| format!("Failed to write file {}: {}", miner_path, e))?;
                    println!("Saved miner profile for {} to {}", miner_id, miner_path);
                }
                Err(e) => {
                    eprintln!(
                        "Failed to parse IPFS content as JSON for miner {} (CID: {}): {}",
                        miner_id, cid, e
                    );
                }
            },
            Some(Err(e)) => {
                eprintln!(
                    "Failed to fetch IPFS content for miner {} (CID: {}): {}",
                    miner_id, cid, e
                );
            }
            None => {
                eprintln!("No content found for miner {} (CID: {})", miner_id, cid);
            }
        }
    }

    // Fetch all user profiles
    let user_query = "
        SELECT storage_key, storage_value
        FROM blockchain.ipfs_userprofile
    ";
    let user_rows: Vec<(String, String)> = sqlx::query_as(user_query)
        .fetch_all(&*app_state.session)
        .await
        .map_err(|e| format!("Failed to query user profiles: {}", e))?;

    // Batch fetch all user profile contents
    let user_cids: Vec<String> = user_rows.iter().map(|(_, cid)| cid.clone()).collect();
    let user_contents = fetch_ipfs_contents(app_state, &user_cids).await;

    // Process user profiles
    for (user_id, cid) in user_rows {
        match user_contents.get(&cid) {
            Some(Ok(content)) => match serde_json::from_slice::<serde_json::Value>(content) {
                Ok(json) => {
                    let user_path = format!("profile/users/{}.json", user_id);
                    let json_str = serde_json::to_string_pretty(&json).map_err(|e| {
                        format!("Failed to serialize JSON for user {}: {}", user_id, e)
                    })?;
                    fs::write(&user_path, json_str)
                        .map_err(|e| format!("Failed to write file {}: {}", user_path, e))?;
                    println!("Saved user profile for {} to {}", user_id, user_path);
                }
                Err(e) => {
                    eprintln!(
                        "Failed to parse IPFS content as JSON for user {} (CID: {}): {}",
                        user_id, cid, e
                    );
                }
            },
            Some(Err(e)) => {
                eprintln!(
                    "Failed to fetch IPFS content for user {} (CID: {}): {}",
                    user_id, cid, e
                );
            }
            None => {
                eprintln!("No content found for user {} (CID: {})", user_id, cid);
            }
        }
    }

    Ok(())
}