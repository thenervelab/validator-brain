use crate::substrate_fetcher::custom_runtime::runtime_types::pallet_execution_unit::types::MinerPinMetrics;
use crate::types::*;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
};
use cid::Cid;
use rand::SeedableRng;
// use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use reqwest::Client;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task;
use tokio::time::Duration;

// // Fetch and parse the badbits.deny blacklist
// pub async fn fetch_badbits_blacklist() -> Result<HashSet<String>, String> {
//     let url = "https://badbits.dwebops.pub/badbits.deny";
//     let response = reqwest::get(url)
//         .await
//         .map_err(|e| format!("Failed to fetch badbits.deny: {}", e))?;

//     let text = response
//         .text()
//         .await
//         .map_err(|e| format!("Failed to read badbits.deny response: {}", e))?;

//     let blacklist: HashSet<String> = text
//         .lines()
//         .filter(|line| line.starts_with("//"))
//         .map(|line| line.trim_start_matches("//").to_lowercase())
//         .collect();

//     Ok(blacklist)
// }

// // Check if a CID is blacklisted by hashing it
// pub fn is_cid_blacklisted(cid: &str, blacklist: &HashSet<String>) -> bool {
//     let mut hasher = Sha256::new();
//     hasher.update(cid); // Hash the CID as a string
//     let hash = hasher.finalize();
//     let hex_hash = format!("{:x}", hash);
//     blacklist.contains(&hex_hash)
// }

pub async fn pin_cid_to_ipfs(state: &AppState, cid: &str) -> Result<String, String> {
    // Clean the CID by removing quotes and slashes
    let clean_cid = cid.trim_matches(|c| c == '"' || c == '/');

    // Validate the CID
    let _cid_parsed = Cid::from_str(clean_cid)
        .map_err(|e| format!("Invalid CID: {} - Error: {}", clean_cid, e))?;

    let url = format!("{}/api/v0/pin/add?arg={}", state.ipfs_node_url, clean_cid);

    let response = state
        .ipfs_client
        .post(&url)
        .timeout(std::time::Duration::from_secs(20)) // Reduced timeout
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                format!("Request timed out for CID: {}", clean_cid)
            } else if e.is_connect() {
                format!(
                    "Connection error for CID: {} - Check IPFS node URL",
                    clean_cid
                )
            } else {
                format!(
                    "Failed to send request for CID: {} - Error: {}",
                    clean_cid, e
                )
            }
        })?;
    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_default();
        return Err(format!(
            "Unexpected status code: {} for CID: {}. Response: {}",
            status, clean_cid, error_body
        ));
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response for CID {}: {}", clean_cid, e))?;

    // Handle different possible response formats
    if let Some(pins) = body["Pins"].as_array() {
        if let Some(pinned_cid) = pins.get(0).and_then(|v| v.as_str()) {
            // Validate that the pinned CID matches the input
            if pinned_cid != clean_cid {
                return Err(format!(
                    "Pinned CID {} does not match input CID {}",
                    pinned_cid, clean_cid
                ));
            }
            Ok(pinned_cid.to_string())
        } else {
            Err(format!(
                "No valid CID in Pins array for response: {:?}",
                body
            ))
        }
    } else if let Some(cid) = body["Hash"].as_str() {
        // Validate that the pinned CID matches the input
        if cid != clean_cid {
            return Err(format!(
                "Pinned CID {} does not match input CID {}",
                cid, clean_cid
            ));
        }
        Ok(cid.to_string())
    } else {
        Err(format!("Unexpected response format: {:?}", body))
    }
}

// pub async fn unpin_cid_from_ipfs(state: &AppState, cid: &str) -> Result<String, String> {
//     // Clean the CID by removing quotes and slashes
//     let clean_cid = cid.trim_matches(|c| c == '"' || c == '/');

//     // Validate the CID
//     let _cid_parsed = Cid::from_str(clean_cid)
//         .map_err(|e| format!("Invalid CID: {} - Error: {}", clean_cid, e))?;

//     let url = format!("{}/api/v0/pin/rm?arg={}", state.ipfs_node_url, clean_cid);

//     let response = state
//         .ipfs_client
//         .post(&url)
//         .timeout(std::time::Duration::from_secs(20))
//         .send()
//         .await
//         .map_err(|e| {
//             if e.is_timeout() {
//                 format!("Request timed out for CID: {}", clean_cid)
//             } else if e.is_connect() {
//                 format!(
//                     "Connection error for CID: {} - Check IPFS node URL",
//                     clean_cid
//                 )
//             } else {
//                 format!(
//                     "Failed to send request for CID: {} - Error: {}",
//                     clean_cid, e
//                 )
//             }
//         })?;

//     let status = response.status();
//     if !status.is_success() {
//         let error_body = response.text().await.unwrap_or_default();
//         return Err(format!(
//             "Unexpected status code: {} for CID: {}. Response: {}",
//             status, clean_cid, error_body
//         ));
//     }

//     let body: serde_json::Value = response
//         .json()
//         .await
//         .map_err(|e| format!("Failed to parse response for CID {}: {}", clean_cid, e))?;

//     // Handle response format (IPFS pin/rm typically returns Pins array)
//     if let Some(pins) = body["Pins"].as_array() {
//         if let Some(unpinned_cid) = pins.get(0).and_then(|v| v.as_str()) {
//             // Validate that the unpinned CID matches the input
//             if unpinned_cid != clean_cid {
//                 return Err(format!(
//                     "Unpinned CID {} does not match input CID {}",
//                     unpinned_cid, clean_cid
//                 ));
//             }
//             Ok(unpinned_cid.to_string())
//         } else {
//             Err(format!(
//                 "No valid CID in Pins array for response: {:?}",
//                 body
//             ))
//         }
//     } else {
//         Err(format!("Unexpected response format: {:?}", body))
//     }
// }

pub async fn pin_cid_to_hippius_network(cid: &str) -> Result<String, String> {
    // Clean the CID by removing quotes and slashes
    let clean_cid = cid.trim_matches(|c| c == '"' || c == '/');

    // Validate the CID
    let _cid_parsed = Cid::from_str(clean_cid)
        .map_err(|e| format!("Invalid CID: {} - Error: {}", clean_cid, e))?;

    let url = format!(
        "https://store.hippius.network/api/v0/pin/add?arg={}",
        clean_cid
    );

    let client = Client::new();
    let response = client
        .post(&url)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                format!("Request timed out for CID: {}", clean_cid)
            } else if e.is_connect() {
                format!(
                    "Connection error for CID: {} - Check Hippius network URL",
                    clean_cid
                )
            } else {
                format!(
                    "Failed to send request for CID: {} - Error: {}",
                    clean_cid, e
                )
            }
        })?;
    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await.unwrap_or_default();
        return Err(format!(
            "Unexpected status code: {} for CID: {}. Response: {}",
            status, clean_cid, error_body
        ));
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response for CID {}: {}", clean_cid, e))?;

    if let Some(pins) = body["Pins"].as_array() {
        if let Some(pinned_cid) = pins.get(0).and_then(|v| v.as_str()) {
            if pinned_cid != clean_cid {
                return Err(format!(
                    "Pinned CID {} does not match input CID {}",
                    pinned_cid, clean_cid
                ));
            }
            Ok(pinned_cid.to_string())
        } else {
            Err(format!(
                "No valid CID in Pins array for response: {:?}",
                body
            ))
        }
    } else if let Some(cid) = body["Hash"].as_str() {
        if cid != clean_cid {
            return Err(format!(
                "Pinned CID {} does not match input CID {}",
                cid, clean_cid
            ));
        }
        Ok(cid.to_string())
    } else {
        Err(format!("Unexpected response format: {:?}", body))
    }
}

pub async fn fetch_ipfs_file_size(state: &AppState, file_hash: &str) -> Result<u32, String> {
    // Wait for rate limiter before proceeding
    state.rate_limiter.until_ready().await;

    // Update URL to use /api/v0/files/stat with /ipfs/<CID> path
    let url = format!("{}/api/v0/files/stat?arg=/ipfs/{}", state.ipfs_node_url, file_hash);

    let response = state
        .ipfs_client
        .post(&url)
        .header("Content-Type", "application/json")
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                format!("Request timed out for file hash: {}", file_hash)
            } else if e.is_connect() {
                format!(
                    "Connection error for file hash: {} - Check IPFS node URL",
                    file_hash
                )
            } else {
                format!(
                    "Failed to send request for file hash: {} - Error: {}",
                    file_hash, e
                )
            }
        })?;

    if !response.status().is_success() {
        return Err(format!(
            "Unexpected status code: {} for file hash: {}",
            response.status(),
            file_hash
        ));
    }

    let body = response.text().await.map_err(|e| {
        format!(
            "Failed to read response body for file hash {}: {}",
            file_hash, e
        )
    })?;

    // Parse JSON response using serde_json
    let json: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
        format!(
            "Failed to parse JSON response for file hash {}: {}",
            file_hash, e
        )
    })?;

    // Extract the Size field and ensure it fits in u32
    if let Some(size) = json.get("Size").and_then(|v| v.as_u64()) {
        if size > u32::MAX as u64 {
            return Err(format!(
                "File size {} exceeds u32 maximum ({}) for file hash: {}",
                size, u32::MAX, file_hash
            ));
        }
        Ok(size as u32)
    } else {
        Err(format!(
            "Failed to extract Size field from response for file hash: {}",
            file_hash
        ))
    }
}

pub async fn pin_profiles(
    State(state): State<AppState>,
) -> Result<Json<PinResponse>, (StatusCode, String)> {
    // Fetch UserProfile CIDs
    let user_query = "SELECT storage_key, storage_value FROM blockchain.ipfs_userprofile";

    // Use sqlx query pattern
    let user_rows: Vec<(String, String)> = sqlx::query_as(user_query)
        .fetch_all(&*state.session)
        .await
        .map_err(|e| {
            eprintln!("Failed to query user profiles: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to query user profiles: {}", e),
            )
        })?;

    let user_cids: Vec<String> = user_rows
        .into_iter()
        .filter_map(|(storage_key, cid)| {
            if storage_key.trim().is_empty() {
                eprintln!("Skipping empty storage_key in ipfs_userprofile");
                None
            } else if cid.trim().is_empty() {
                eprintln!(
                    "Skipping empty CID for storage_key {} in ipfs_userprofile",
                    storage_key
                );
                None
            } else {
                Some(cid)
            }
        })
        .collect();

    // Fetch MinerProfile CIDs
    let miner_query = "SELECT storage_key, storage_value FROM blockchain.ipfs_minerprofile";

    // Use sqlx query pattern
    let miner_rows: Vec<(String, String)> = sqlx::query_as(miner_query)
        .fetch_all(&*state.session)
        .await
        .map_err(|e| {
            eprintln!("Failed to query miner profiles: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to query miner profiles: {}", e),
            )
        })?;

    let miner_cids: Vec<String> = miner_rows
        .into_iter()
        .filter_map(|(storage_key, cid)| {
            if storage_key.trim().is_empty() {
                eprintln!("Skipping empty storage_key in ipfs_minerprofile");
                None
            } else if cid.trim().is_empty() {
                eprintln!(
                    "Skipping empty CID for storage_key {} in ipfs_minerprofile",
                    storage_key
                );
                None
            } else {
                Some(cid)
            }
        })
        .collect();

    // Combine all CIDs
    let all_cids: Vec<String> = [user_cids, miner_cids].concat();

    // If no valid CIDs, return early
    if all_cids.is_empty() {
        return Ok(Json(PinResponse {
            pinned: vec![],
            failed: vec![],
        }));
    }

    let mut pinned = Vec::new();
    let mut failed = Vec::new();

    // Create tasks for each CID to pin to both IPFS and Hippius
    let tasks: Vec<_> = all_cids
        .into_iter()
        .map(|cid| {
            let state = state.clone();
            let cid_clone = cid.clone();
            let cid_clone_hippius = cid.clone();
            (
                cid,
                task::spawn(async move {
                    // Pin to local IPFS
                    let ipfs_result = pin_cid_to_ipfs(&state, &cid_clone).await;
                    // Pin to Hippius network
                    let hippius_result = pin_cid_to_hippius_network(&cid_clone_hippius).await;

                    // Combine results
                    match (ipfs_result, hippius_result) {
                        (Ok(_), Ok(_)) => Ok(cid_clone), // Both succeeded
                        (Err(ipfs_err), Err(hippius_err)) => Err(format!(
                            "IPFS error: {}; Hippius error: {}",
                            ipfs_err, hippius_err
                        )), // Both failed
                        (Err(ipfs_err), Ok(_)) => Err(format!("IPFS error: {}", ipfs_err)), // IPFS failed
                        (Ok(_), Err(hippius_err)) => Err(format!("Hippius error: {}", hippius_err)), // Hippius failed
                    }
                }),
            )
        })
        .collect();

    // Await all tasks and collect results
    for (cid, task) in tasks {
        match task.await {
            Ok(result) => match result {
                Ok(_) => pinned.push(cid.clone()),
                Err(e) => {
                    failed.push(cid.clone());
                    eprintln!("Failed to pin CID {}: {}", cid, e);
                }
            },
            Err(e) => {
                failed.push(cid.clone());
                eprintln!("Task failed for CID {}: {}", cid, e);
            }
        }
    }

    Ok(Json(PinResponse { pinned, failed }))
}

pub async fn fetch_ipfs_content(state: &AppState, cid: &str) -> Result<Vec<u8>, String> {
    // Wait for rate limiter before proceeding
    state.rate_limiter.until_ready().await;

    // Remove surrounding quotes and slashes from the CID
    let clean_cid = cid.trim_matches(|c| c == '"' || c == '/');

    // Validate the CID
    let _cid_parsed = Cid::from_str(clean_cid)
        .map_err(|e| format!("Invalid CID: {} - Error: {}", clean_cid, e))?;

    let url = format!("{}/api/v0/cat?arg={}", state.ipfs_node_url, clean_cid);

    let response = state
        .ipfs_client
        .post(&url)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await;

    match response {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                resp.bytes()
                    .await
                    .map(|body| body.to_vec())
                    .map_err(|e| format!("Failed to read response body: {}", e))
            } else {
                let body_text = resp.text().await.unwrap_or_default();
                Err(format!(
                    "Unexpected status code: {} for CID: {}. Response: {}",
                    status, clean_cid, body_text
                ))
            }
        }
        Err(e) => {
            if e.is_timeout() {
                Err(format!("Request timed out for CID: {}", clean_cid))
            } else if e.is_connect() {
                Err(format!(
                    "Connection error for CID: {} - Check IPFS node URL",
                    clean_cid
                ))
            } else {
                Err(format!(
                    "Failed to send request for CID: {} - Error: {}",
                    clean_cid, e
                ))
            }
        }
    }
}

// Fetch pinned nodes for a list of CIDs (batched)
// async fn fetch_cid_pinned_nodes(
//     state: &AppState,
//     cids: Vec<String>,
// ) -> Result<Vec<(String, Vec<String>)>, String> {
//     let mut results = Vec::new();

//     // Parallelize CID queries
//     let tasks = cids.into_iter().map(|cid| {
//         let state = state.clone();
//         let cid_clone = cid.clone();
//         tokio::spawn(async move {
//             let url = format!(
//                 "{}/api/v0/routing/findprovs?arg={}",
//                 state.ipfs_node_url, cid_clone
//             );
//             let response = state
//                 .ipfs_client
//                 .post(&url)
//                 .header("Content-Type", "application/json")
//                 .body("{}")
//                 .timeout(std::time::Duration::from_secs(20)) // Reduced timeout
//                 .send()
//                 .await;

//             match response {
//                 Ok(resp) => {
//                     if resp.status().is_success() {
//                         let body = resp.text().await.map_err(|e| {
//                             format!("Failed to read response body for CID {}: {}", cid_clone, e)
//                         })?;

//                         // Parse JSON lines in a blocking thread
//                         let node_ids = tokio::task::spawn_blocking(move || {
//                             let json_strings: Vec<&str> =
//                                 body.split('\n').filter(|s| !s.trim().is_empty()).collect();
//                             let mut node_ids = Vec::new();

//                             for json_str in json_strings {
//                                 if let Ok(parsed) =
//                                     serde_json::from_str::<serde_json::Value>(json_str)
//                                 {
//                                     if let Some(responses) = parsed["Responses"].as_array() {
//                                         for response in responses {
//                                             if let Some(id) = response["ID"].as_str() {
//                                                 node_ids.push(id.to_string());
//                                             }
//                                         }
//                                     }
//                                 }
//                             }
//                             node_ids
//                         })
//                         .await
//                         .map_err(|e| {
//                             format!("Failed to parse JSON for CID {}: {}", cid_clone, e)
//                         })?;

//                         Ok((cid_clone, node_ids))
//                     } else {
//                         Err(format!(
//                             "Unexpected status code: {} for CID: {}",
//                             resp.status(),
//                             cid_clone
//                         ))
//                     }
//                 }
//                 Err(e) => {
//                     if e.is_timeout() {
//                         Err(format!("Request timed out for CID: {}", cid_clone))
//                     } else if e.is_connect() {
//                         Err(format!(
//                             "Connection error for CID: {} - Check IPFS node URL",
//                             cid_clone
//                         ))
//                     } else {
//                         Err(format!(
//                             "Failed to send request for CID: {} - Error: {}",
//                             cid_clone, e
//                         ))
//                     }
//                 }
//             }
//         })
//     });

//     // Collect results
//     for task in stream::iter(tasks)
//         .buffer_unordered(10)
//         .collect::<Vec<_>>()
//         .await
//     {
//         match task {
//             Ok(Ok(result)) => results.push(result),
//             Ok(Err(e)) => return Err(e),
//             Err(e) => return Err(format!("Task failed: {}", e)),
//         }
//     }

//     Ok(results)
// }



/// Checks if a miner is online by pinging their peer ID via the IPFS API.
async fn is_miner_online(state: &AppState, peer_id: &str) -> Result<bool, String> {
    println!("Checking if IPFS node {} is online", peer_id);

    // Validate input
    if peer_id.is_empty() {
        return Err("Empty peer ID provided".to_string());
    }

    // Make sure we have a valid-looking IPFS peer ID format (typically start with Qm or 12D3)
    // But allow other formats as they might be valid in different IPFS versions
    if peer_id.len() < 10 {
        println!("⚠️ Warning: Peer ID {} looks unusually short", peer_id);
    }

    // First try to connect to the peer before pinging
    let connect_url = format!(
        "{}/api/v0/swarm/connect?arg=/p2p/{}",
        state.ipfs_node_url, peer_id
    );
    println!("Attempting to connect to IPFS peer: {}", peer_id);

    let connect_result = state.ipfs_client.post(&connect_url).send().await;

    match connect_result {
        Ok(response) => {
            if response.status().is_success() {
                println!("Successfully connected to IPFS peer {}", peer_id);
                // Proceed with ping
            } else {
                let status = response.status();
                let error_text = response.text().await.unwrap_or_default();
                println!(
                    "Warning: IPFS connect returned status {}: {}",
                    status, error_text
                );
                if error_text.contains("already connected") {
                    println!(
                        "Peer {} is already connected, proceeding with ping",
                        peer_id
                    );
                    // Proceed to ping anyway as we're already connected
                } else {
                    // Still proceed with ping as it might work anyway
                    println!("Will try ping despite connect failure");
                }
            }
        }
        Err(e) => {
            println!("Warning: Failed to connect to IPFS peer {}: {}", peer_id, e);
            // Continue with ping anyway as it might still work
        }
    }

    // Now try to ping the peer
    let url = format!(
        "{}/api/v0/ping?arg={}&count=1",
        state.ipfs_node_url, peer_id
    );
    println!("Sending ping request to IPFS peer: {}", peer_id);

    match state.ipfs_client.post(&url).send().await {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                // Try to parse the response to check for actual success
                match response.text().await {
                    Ok(text) => {
                        println!("Ping response for {}: {}", peer_id, text);

                        // Check if we got a successful ping response
                        if text.contains("Success") || text.contains("Pong") {
                            println!("✅ IPFS node {} is online", peer_id);
                            return Ok(true);
                        } else {
                            // Sometimes the IPFS API returns 200 without Success/Pong
                            // Look for other indicators of success
                            if text.contains("peer") && text.contains("time") {
                                println!(
                                    "✅ IPFS node {} appears to be online (based on response data)",
                                    peer_id
                                );
                                return Ok(true);
                            }

                            println!(
                                "⚠️ IPFS node {} ping returned success status but no success message: {}",
                                peer_id, text
                            );

                            // As a last resort, try to get peer info
                            return check_peer_info(state, peer_id).await;
                        }
                    }
                    Err(e) => {
                        println!("⚠️ Error reading ping response for {}: {}", peer_id, e);
                        // Try the last resort peer info check
                        return check_peer_info(state, peer_id).await;
                    }
                }
            } else {
                println!(
                    "❌ Ping request for {} failed with status: {}",
                    peer_id, status
                );
                // Try the last resort peer info check
                return check_peer_info(state, peer_id).await;
            }
        }
        Err(e) => {
            println!("❌ Error sending ping request to {}: {}", peer_id, e);
            // Try the last resort peer info check
            return check_peer_info(state, peer_id).await;
        }
    }
}

/// Fallback check to determine if a peer is online by requesting peer info
async fn check_peer_info(state: &AppState, peer_id: &str) -> Result<bool, String> {
    println!("Trying fallback peer info check for {}", peer_id);

    let info_url = format!("{}/api/v0/id?arg={}", state.ipfs_node_url, peer_id);

    match state.ipfs_client.post(&info_url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(text) => {
                        if text.contains(peer_id) {
                            println!(
                                "✅ IPFS node {} is online (confirmed via peer info)",
                                peer_id
                            );
                            return Ok(true);
                        } else {
                            println!("❌ Peer info check failed: response doesn't contain peer ID");
                            return Ok(false);
                        }
                    }
                    Err(e) => {
                        println!("❌ Error reading peer info response: {}", e);
                        return Ok(false);
                    }
                }
            } else {
                println!(
                    "❌ Peer info request failed with status: {}",
                    response.status()
                );
                return Ok(false);
            }
        }
        Err(e) => {
            println!("❌ Error sending peer info request: {}", e);
            return Ok(false);
        }
    }
}

// /// Verifies that a miner is storing a file (or folder) by sampling and checking blocks.
// async fn verify_file_storage(
//     state: &AppState,
//     peer_id: &str,
//     cid_str: &str,
//     blocks_to_check: usize,
// ) -> Result<bool, String> {
//     println!(
//         "Starting file storage verification for miner {}, CID {}",
//         peer_id, cid_str
//     );

//     // Parse the CID
//     let _cid = Cid::from_str(cid_str).map_err(|e| format!("Invalid CID {}: {}", cid_str, e))?;

//     println!("Extracting block CIDs from DAG...");
//     let all_block_cids = extract_all_block_cids(state, cid_str).await?;

//     if all_block_cids.is_empty() {
//         return Err("No blocks found in the DAG for the given CID".to_string());
//     }

//     println!(
//         "Found {} blocks in DAG for CID {}",
//         all_block_cids.len(),
//         cid_str
//     );
//     let check_blocks =
//         sample_random_blocks(&all_block_cids, blocks_to_check.min(all_block_cids.len()));
//     println!(
//         "Selected {} blocks to check: {:?}",
//         check_blocks.len(),
//         check_blocks
//     );

//     println!("Connecting to miner IPFS node: {}", peer_id);
//     let connect_url = format!(
//         "{}/api/v0/swarm/connect?arg=/p2p/{}",
//         state.ipfs_node_url, peer_id
//     );
//     match state.ipfs_client.post(&connect_url).send().await {
//         Ok(response) => {
//             if response.status().is_success() {
//                 println!("Successfully connected to miner IPFS node {}", peer_id);
//             } else {
//                 let status = response.status();
//                 let body = response.text().await.unwrap_or_default();
//                 println!(
//                     "Failed to connect to miner {}: Status {}, Body: {}",
//                     peer_id, status, body
//                 );
//                 // Continue anyway to see if we can fetch blocks
//             }
//         }
//         Err(e) => {
//             println!("Error connecting to miner {}: {}", peer_id, e);
//             // Continue anyway to see if we can fetch blocks
//         }
//     }

//     // Try to get peer info to double check connection
//     let peer_info_url = format!("{}/api/v0/id?arg={}", state.ipfs_node_url, peer_id);
//     match state.ipfs_client.post(&peer_info_url).send().await {
//         Ok(response) => {
//             if response.status().is_success() {
//                 println!("Successfully retrieved peer info for {}", peer_id);
//             } else {
//                 println!(
//                     "Failed to get peer info for {}: Status {}",
//                     peer_id,
//                     response.status()
//                 );
//             }
//         }
//         Err(e) => {
//             println!("Error getting peer info for {}: {}", peer_id, e);
//         }
//     }

//     println!(
//         "Starting block verification for {} blocks",
//         check_blocks.len()
//     );
//     let mut successful_blocks = 0;

//     for (i, block_cid_str) in check_blocks.iter().enumerate() {
//         println!(
//             "[{}/{}] Checking block {}",
//             i + 1,
//             check_blocks.len(),
//             block_cid_str
//         );

//         let block_url = format!(
//             "{}/api/v0/block/get?arg={}",
//             state.ipfs_node_url, block_cid_str
//         );
//         let block_response = match state.ipfs_client.post(&block_url).send().await {
//             Ok(resp) => {
//                 if resp.status().is_success() {
//                     println!("Successfully retrieved block {}", block_cid_str);
//                     resp
//                 } else {
//                     println!(
//                         "Failed to retrieve block {}: Status {}",
//                         block_cid_str,
//                         resp.status()
//                     );
//                     continue;
//                 }
//             }
//             Err(e) => {
//                 println!("Error retrieving block {}: {}", block_cid_str, e);
//                 continue;
//             }
//         };

//         let block_data = match block_response.bytes().await {
//             Ok(data) => {
//                 println!(
//                     "Successfully read {} bytes from block {}",
//                     data.len(),
//                     block_cid_str
//                 );
//                 data
//             }
//             Err(e) => {
//                 println!("Failed to read block data {}: {}", block_cid_str, e);
//                 continue;
//             }
//         };

//         let computed_cid = match compute_cid(&block_data) {
//             Ok(cid) => {
//                 println!(
//                     "Successfully computed CID for block {}: {}",
//                     block_cid_str, cid
//                 );
//                 cid
//             }
//             Err(e) => {
//                 println!("Failed to compute CID for block {}: {}", block_cid_str, e);
//                 continue;
//             }
//         };

//         if computed_cid.to_string() != *block_cid_str {
//             println!(
//                 "Block {} CID mismatch: expected {}, got {}",
//                 block_cid_str, block_cid_str, computed_cid
//             );
//             continue;
//         }

//         // Block verified successfully
//         successful_blocks += 1;
//         println!(
//             "Successfully verified block {} ({}/{})",
//             block_cid_str,
//             successful_blocks,
//             check_blocks.len()
//         );
//     }

//     let success = successful_blocks > 0;
//     println!(
//         "File storage verification complete for CID {}: {} of {} blocks verified, result: {}",
//         cid_str,
//         successful_blocks,
//         check_blocks.len(),
//         if success { "SUCCESS" } else { "FAILED" }
//     );

//     Ok(success)
// }

// /// Extracts all block CIDs recursively from a CID's DAG using the IPFS refs API.
// async fn extract_all_block_cids(state: &AppState, cid: &str) -> Result<Vec<String>, String> {
//     println!("Extracting block CIDs for {}", cid);

//     let mut cids = HashSet::new();
//     let url = format!(
//         "{}/api/v0/refs?arg={}&recursive=true",
//         state.ipfs_node_url, cid
//     );
//     println!("Sending IPFS refs request to: {}", url);

//     let response = match state.ipfs_client.post(&url).send().await {
//         Ok(resp) => {
//             if resp.status().is_success() {
//                 println!("Successfully received refs response for CID {}", cid);
//                 resp
//             } else {
//                 let status = resp.status();
//                 let error_body = resp.text().await.unwrap_or_default();
//                 return Err(format!(
//                     "Failed to fetch refs for CID {}: Status {} - {}",
//                     cid, status, error_body
//                 ));
//             }
//         }
//         Err(e) => {
//             return Err(format!(
//                 "Failed to send refs request for CID {}: {}",
//                 cid, e
//             ));
//         }
//     };

//     let text = match response.text().await {
//         Ok(text) => {
//             println!("Received {} bytes of refs data for CID {}", text.len(), cid);
//             text
//         }
//         Err(e) => {
//             return Err(format!(
//                 "Failed to read refs response for CID {}: {}",
//                 cid, e
//             ));
//         }
//     };

//     // Parse each line as JSON
//     let mut line_count = 0;
//     let mut valid_refs = 0;
//     for line in text.lines() {
//         line_count += 1;
//         if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
//             if let Some(ref_field) = json.get("Ref").and_then(|r| r.as_str()) {
//                 cids.insert(ref_field.to_string());
//                 valid_refs += 1;
//             }
//         }
//     }

//     let result = cids.into_iter().collect::<Vec<String>>();
//     println!(
//         "Extracted {} valid refs from {} lines for CID {} (final count: {})",
//         valid_refs,
//         line_count,
//         cid,
//         result.len()
//     );

//     Ok(result)
// }

// /// Samples a random subset of block CIDs.
// fn sample_random_blocks(cids: &[String], count: usize) -> Vec<String> {
//     let mut rng = StdRng::from_entropy();
//     cids.choose_multiple(&mut rng, count).cloned().collect()
// }

// /// Computes the CID of a block's raw data (simplified using SHA2-256 multihash).
// fn compute_cid(data: &[u8]) -> Result<Cid, String> {
//     let hash = Code::Sha2_256.digest(data);
//     Ok(Cid::new_v1(0x55, hash)) // 0x55 is the raw codec
// }

/// Verifies a subset of StorageMiners for the current epoch interval.
/// Called every 50 blocks to process a portion of miners.
pub async fn verify_miners_subset(
    state: &Arc<AppState>,
    cids_per_miner: usize,          // Number of CIDs to check per miner
    _blocks_to_check_per_cid: usize, // Number of blocks to check per CID
    offset: usize,                  // Offset for pagination
    limit: usize,                   // Number of miners to process in this interval
) -> Result<Vec<MinerPinMetrics>, String> {
    println!(
        "Starting miner verification with limit={}, offset={}",
        limit, offset
    );
    println!("======================================");

    // Use only 'StorageMiner' type nodes
    let miners_query = "SELECT node_id, ipfs_node_id FROM blockchain.ipfs_registration WHERE node_type = 'StorageMiner' LIMIT $1 OFFSET $2";

    // Use sqlx query pattern
    let rows: Vec<(String, String)> = sqlx::query_as(miners_query)
        .bind(limit as i32)
        .bind(offset as i32)
        .fetch_all(&*state.session)
        .await
        .map_err(|e| format!("Failed to fetch registered miners: {}", e))?;

    println!("Found {} registered miners for verification", rows.len());
    println!("======================================");

    let mut results = Vec::new();

    // Process each miner
    for (idx, (node_id, ipfs_node_id)) in rows.iter().enumerate() {
        println!("======================================");
        println!(
            "Processing miner [{}/{}]: node_id={}, ipfs_node_id={}",
            idx + 1,
            rows.len(),
            node_id,
            ipfs_node_id
        );

        // Create metrics for this miner
        let mut metrics = MinerPinMetrics {
            node_id: node_id.clone().into_bytes(),
            successful_pin_checks: 0,
            total_pin_checks: 0,
        };

        // First check if the miner's IPFS node is online
        println!("Checking if miner's IPFS node is online...");
        let is_online = match is_miner_online(state, ipfs_node_id).await {
            Ok(true) => {
                println!("✅ Miner's IPFS node {} is ONLINE", ipfs_node_id);
                metrics.total_pin_checks += 1;
                metrics.successful_pin_checks += 1;
                true
            }
            Ok(false) => {
                println!("❌ Miner's IPFS node {} is OFFLINE", ipfs_node_id);
                false
            }
            Err(e) => {
                println!(
                    "⚠️ Error checking if miner's IPFS node {} is online: {}",
                    ipfs_node_id, e
                );
                false
            }
        };

        if !is_online {
            println!("Skipping verification for offline miner {}", node_id);
            results.push(metrics);
            continue;
        }

        // Fetch miner profile CID
        let profile_query =
            "SELECT storage_value FROM blockchain.ipfs_minerprofile WHERE storage_key = $1";

        // Use sqlx query pattern
        let profile_cid: Option<String> = sqlx::query_scalar::<_, String>(profile_query)
            .bind(node_id)
            .fetch_optional(&*state.session)
            .await
            .map_err(|e| format!("Failed to fetch miner profile: {}", e))?;

        match &profile_cid {
            Some(cid) => println!("Found miner profile with CID: {}", cid),
            None => println!("No profile found for miner node_id={}", node_id),
        }

        // Check if the miner has a profile
        match profile_cid {
            Some(cid) => {
                // Fetch profile content
                match fetch_ipfs_content(state, &cid).await {
                    Ok(content) => {
                        println!(
                            "Successfully fetched profile content ({} bytes)",
                            content.len()
                        );
                        match serde_json::from_slice::<Vec<serde_json::Value>>(&content) {
                            Ok(pin_items) => {
                                println!("Miner has {} pinned items in profile", pin_items.len());

                                // Sample CIDs to verify
                                let sample_size = std::cmp::min(cids_per_miner, pin_items.len());
                                if sample_size > 0 {
                                    println!(
                                        "Will verify {} CIDs from miner's profile",
                                        sample_size
                                    );

                                    // Use rand::SeedableRng instead of thread_rng() for tokio tasks
                                    let mut rng = rand::rngs::StdRng::from_entropy();
                                    let indices: Vec<usize> = (0..pin_items.len()).collect();
                                    let sampled_indices: Vec<usize> = indices
                                        .choose_multiple(&mut rng, sample_size)
                                        .cloned()
                                        .collect();

                                    for (check_idx, idx) in sampled_indices.iter().enumerate() {
                                        if let Some(file_hash) = pin_items[*idx].get("file_hash") {
                                            if file_hash.is_array() {
                                                let file_hash_bytes: Vec<u8> = file_hash
                                                    .as_array()
                                                    .unwrap()
                                                    .iter()
                                                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                                                    .collect();

                                                let cid_vec = hex::decode(&file_hash_bytes)
                                                    .map_err(|e| {
                                                        format!("Invalid file hash: {}", e)
                                                    })?;

                                                let file_hash_str = match String::from_utf8(
                                                    cid_vec.clone(),
                                                ) {
                                                    Ok(s) => s,
                                                    Err(_) => {
                                                        println!(
                                                            "Cannot convert file hash bytes to string, skipping"
                                                        );
                                                        continue;
                                                    }
                                                };

                                                // Verify that miner has the CID
                                                metrics.total_pin_checks += 1;
                                                println!("------------------------------");
                                                println!(
                                                    "Verification check [{}/{}] for miner {}:",
                                                    check_idx + 1,
                                                    sample_size,
                                                    node_id
                                                );
                                                println!("CID: {}", file_hash_str);
                                                println!("IPFS node ID: {}", ipfs_node_id);

                                                // IMPORTANT: Use ipfs_node_id here instead of node_id
                                                match check_cid_is_pinned(
                                                    state,
                                                    &ipfs_node_id,
                                                    &file_hash_str,
                                                )
                                                .await
                                                {
                                                    Ok(true) => {
                                                        metrics.successful_pin_checks += 1;
                                                        println!(
                                                            "✅ Verification SUCCESSFUL - miner {} has CID {} ({}/{})",
                                                            node_id,
                                                            file_hash_str,
                                                            metrics.successful_pin_checks,
                                                            metrics.total_pin_checks
                                                        );
                                                    }
                                                    Ok(false) => {
                                                        println!(
                                                            "❌ Verification FAILED - miner {} does not have CID {} ({}/{})",
                                                            node_id,
                                                            file_hash_str,
                                                            metrics.successful_pin_checks,
                                                            metrics.total_pin_checks
                                                        );
                                                    }
                                                    Err(e) => {
                                                        eprintln!(
                                                            "⚠️ Verification ERROR - miner {} / CID {}: {}",
                                                            node_id, file_hash_str, e
                                                        );
                                                    }
                                                }
                                                println!("------------------------------");
                                            } else {
                                                println!(
                                                    "file_hash is not an array, skipping item"
                                                );
                                            }
                                        } else {
                                            println!("No file_hash found in item, skipping");
                                        }
                                    }
                                } else {
                                    println!("No CIDs to verify in miner's profile");
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to parse miner profile: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        // Verify that miner has the CID
                        eprintln!("Failed to fetch miner profile content: {}", e);
                    }
                }
            }
            None => {
                eprintln!("No profile found for miner {}", node_id);
            }
        }

        // Add miner's verification results to the overall results
        println!(
            "Miner {} verification results: {}/{} successful checks",
            node_id, metrics.successful_pin_checks, metrics.total_pin_checks
        );
        results.push(metrics);
        println!("======================================");
    }

    println!("======================================");
    println!("Miner verification summary:");
    println!(
        " - Verified {} miners with pagination: offset={}, limit={}",
        results.len(),
        offset,
        limit
    );

    // Count how many miners had successful verifications
    let miners_with_successful_checks = results
        .iter()
        .filter(|m| m.successful_pin_checks > 0)
        .count();

    println!(
        " - {} miners had successful pin checks",
        miners_with_successful_checks
    );

    // Sum up totals
    let total_successful = results.iter().map(|m| m.successful_pin_checks).sum::<u32>();
    let total_checks = results.iter().map(|m| m.total_pin_checks).sum::<u32>();

    println!(
        " - Total pin checks: {}/{} successful ({:.1}%)",
        total_successful,
        total_checks,
        if total_checks > 0 {
            (total_successful as f64 / total_checks as f64) * 100.0
        } else {
            0.0
        }
    );
    println!("======================================");

    Ok(results)
}


/// A comprehensive verification method that checks if a CID is pinned by a miner's IPFS node
/// using multiple verification strategies.
async fn check_cid_is_pinned(
    state: &AppState,
    peer_id: &str,
    cid_str: &str,
) -> Result<bool, String> {
    println!(
        "Checking if CID {} is pinned by IPFS node {}",
        cid_str, peer_id
    );

    // Validate inputs
    if peer_id.is_empty() {
        return Err("Empty peer ID provided".to_string());
    }

    if cid_str.is_empty() {
        return Err("Empty CID provided".to_string());
    }

    // Clean the CID (remove any quotes, slashes)
    let clean_cid = cid_str.trim_matches(|c| c == '"' || c == '/');

    // Try a direct pin check on our local node first (fastest method)
    let local_pinned = check_local_pin(state, clean_cid).await;
    if local_pinned {
        println!(
            "✅ CID {} is locally pinned - verification passed",
            clean_cid
        );
        return Ok(true);
    }

    // Try to connect to the peer if not already connected
    if !ensure_peer_connected(state, peer_id).await {
        println!(
            "Warning: Could not connect to peer {} - verification may fail",
            peer_id
        );
        // Continue anyway as some verification methods might still work
    }

    // Try multiple verification methods in order of reliability and speed

    // Method 1: Check if the peer provides the CID via DHT
    match check_peer_provides_cid(state, peer_id, clean_cid).await {
        Ok(true) => {
            println!(
                "✅ Verification passed: Peer {} is a provider for CID {}",
                peer_id, clean_cid
            );
            return Ok(true);
        }
        Ok(false) => {
            println!("DHT provider check negative, trying other methods...");
            // Continue to other methods
        }
        Err(e) => {
            println!("DHT provider check failed: {}, trying other methods...", e);
            // Continue to other methods
        }
    }

    // Method 2: Try to fetch the CID through the peer
    match fetch_through_peer(state, peer_id, clean_cid).await {
        Ok(true) => {
            println!(
                "✅ Verification passed: Successfully fetched CID {} through peer {}",
                clean_cid, peer_id
            );
            return Ok(true);
        }
        Ok(false) => {
            println!("Failed to fetch CID through peer, trying other methods...");
            // Continue to other methods
        }
        Err(e) => {
            println!(
                "Error fetching through peer: {}, trying other methods...",
                e
            );
            // Continue to other methods
        }
    }

    // Method 3: Use IPFS bitswap wantlist to check if peer has the CID
    match check_bitswap_wantlist(state, peer_id, clean_cid).await {
        Ok(true) => {
            println!(
                "✅ Verification passed: CID {} found in peer's {} bitswap wantlist",
                clean_cid, peer_id
            );
            return Ok(true);
        }
        Ok(false) => {
            println!("CID not found in bitswap wantlist, verification failed");
            // All methods tried, return false
            return Ok(false);
        }
        Err(e) => {
            println!(
                "Error checking bitswap wantlist: {}, verification failed",
                e
            );
            // All methods tried, return false
            return Ok(false);
        }
    }
}

/// Check if a CID is pinned on our local IPFS node
async fn check_local_pin(state: &AppState, cid: &str) -> bool {
    let url = format!("{}/api/v0/pin/ls?arg={}", state.ipfs_node_url, cid);

    match state.ipfs_client.post(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(text) => {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(pins) = json["Keys"].as_object() {
                                if pins.contains_key(cid) {
                                    println!("CID {} is locally pinned", cid);
                                    return true;
                                }
                            }
                        }
                        false
                    }
                    Err(_) => false,
                }
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

/// Ensure we're connected to the peer
async fn ensure_peer_connected(state: &AppState, peer_id: &str) -> bool {
    let connect_url = format!(
        "{}/api/v0/swarm/connect?arg=/p2p/{}",
        state.ipfs_node_url, peer_id
    );

    match state.ipfs_client.post(&connect_url).send().await {
        Ok(response) => {
            let connected = response.status().is_success();
            if connected {
                println!("Successfully connected to peer {}", peer_id);
            } else {
                // Check if we're already connected (common error response)
                match response.text().await {
                    Ok(text) => {
                        if text.contains("already connected") {
                            println!("Already connected to peer {}", peer_id);
                            return true;
                        }
                        println!("Failed to connect to peer {}: {}", peer_id, text);
                    }
                    Err(_) => {
                        println!("Failed to connect to peer {}", peer_id);
                    }
                }
            }
            connected
        }
        Err(e) => {
            println!("Error connecting to peer {}: {}", peer_id, e);
            false
        }
    }
}

/// Check if a peer provides the CID via DHT
async fn check_peer_provides_cid(
    state: &AppState,
    peer_id: &str,
    cid: &str,
) -> Result<bool, String> {
    let url = format!(
        "{}/api/v0/dht/findprovs?arg={}&num-providers=50",
        state.ipfs_node_url, cid
    );

    match state.ipfs_client.post(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(text) => {
                        println!("Checking if peer {} provides CID {}", peer_id, cid);
                        // Process each line as separate JSON object (IPFS API returns ndjson)
                        for line in text.lines() {
                            if line.trim().is_empty() {
                                continue;
                            }

                            match serde_json::from_str::<serde_json::Value>(line) {
                                Ok(json) => {
                                    if let Some(responses) = json["Responses"].as_array() {
                                        for provider in responses {
                                            if let Some(id) = provider["ID"].as_str() {
                                                if id == peer_id {
                                                    println!(
                                                        "Found peer {} as provider for CID {}",
                                                        peer_id, cid
                                                    );
                                                    return Ok(true);
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("Error parsing DHT findprovs JSON: {}", e);
                                }
                            }
                        }
                        Ok(false)
                    }
                    Err(e) => Err(format!("Failed to read DHT findprovs response: {}", e)),
                }
            } else {
                Err(format!(
                    "DHT findprovs request failed with status: {}",
                    response.status()
                ))
            }
        }
        Err(e) => Err(format!("Failed to send DHT findprovs request: {}", e)),
    }
}

/// Try to fetch the CID through the specific peer
async fn fetch_through_peer(state: &AppState, peer_id: &str, cid: &str) -> Result<bool, String> {
    // First set the specific peer as the only one to fetch from
    let timeout_secs = 10;

    // Use the /api/v0/block/get endpoint to try to get the CID only from this peer
    let block_url = format!(
        "{}/api/v0/block/get?arg={}&timeout={}s",
        state.ipfs_node_url, cid, timeout_secs
    );

    // Add special headers to target the specific peer
    let block_response = match tokio::time::timeout(
        Duration::from_secs(timeout_secs + 5), // Add 5s buffer
        state
            .ipfs_client
            .post(&block_url)
            .header("X-Ipfs-Gw-Peering-Peer", format!("/p2p/{}", peer_id))
            .send(),
    )
    .await
    {
        Ok(result) => match result {
            Ok(resp) => resp,
            Err(e) => return Err(format!("Error getting block: {}", e)),
        },
        Err(_) => return Err("Block get request timed out".to_string()),
    };

    if block_response.status().is_success() {
        println!(
            "Successfully fetched CID {} (likely through peer {})",
            cid, peer_id
        );
        Ok(true)
    } else {
        println!(
            "Failed to fetch CID {} through peer {}: {}",
            cid,
            peer_id,
            block_response.status()
        );
        Ok(false)
    }
}

/// Check the peer's bitswap wantlist for the CID
async fn check_bitswap_wantlist(
    state: &AppState,
    peer_id: &str,
    cid: &str,
) -> Result<bool, String> {
    let url = format!(
        "{}/api/v0/bitswap/wantlist?peer={}",
        state.ipfs_node_url, peer_id
    );

    match state.ipfs_client.post(&url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                match response.text().await {
                    Ok(text) => {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(keys) = json["Keys"].as_array() {
                                for key in keys {
                                    if let Some(key_str) = key.as_str() {
                                        if key_str == cid {
                                            return Ok(true);
                                        }
                                    }
                                }
                            }
                            Ok(false)
                        } else {
                            Err("Invalid JSON in bitswap wantlist response".to_string())
                        }
                    }
                    Err(e) => Err(format!("Failed to read bitswap wantlist response: {}", e)),
                }
            } else {
                Err(format!(
                    "Bitswap wantlist request failed with status: {}",
                    response.status()
                ))
            }
        }
        Err(e) => Err(format!("Failed to send bitswap wantlist request: {}", e)),
    }
}

pub async fn fetch_ipfs_file_sizes(
    state: &AppState,
    cids: &[String],
) -> Result<BTreeMap<String, u32>, String> {
    use futures::future::join_all;

    let fetches = cids.iter().map(|cid| async move {
        let size = fetch_ipfs_file_size(state, cid).await;
        (cid.clone(), size)
    });

    let results = join_all(fetches).await;

    let mut sizes = BTreeMap::new();
    for (cid, result) in results {
        match result {
            Ok(size) => {
                sizes.insert(cid, size);
            }
            Err(e) => {
                eprintln!("Failed to fetch size for CID {}: {}", cid, e);
                sizes.insert(cid, 0); // Default to 0 on error
            }
        }
    }

    Ok(sizes)
}

pub async fn fetch_ipfs_contents(
    state: &AppState,
    cids: &[String],
) -> BTreeMap<String, Result<Vec<u8>, String>> {
    use futures::future::join_all;

    let fetches = cids.iter().map(|cid| async move {
        let content = fetch_ipfs_content(state, cid).await;
        (cid.clone(), content)
    });

    let results = join_all(fetches).await;

    let mut contents = BTreeMap::new();
    for (cid, result) in results {
        contents.insert(cid, result);
    }

    contents
}

// pub async fn run_ipfs_gc(app_state: &AppState) -> Result<(), std::io::Error> {
//     let url = format!("{}/api/v0/repo/gc", app_state.ipfs_node_url);

//     // Send POST request (IPFS GC expects POST, no body required)
//     let response = app_state
//         .ipfs_client
//         .post(&url)
//         .header("Content-Type", "application/json")
//         .send()
//         .await
//         .map_err(|e| {
//             eprintln!("Error making request to run IPFS GC: {}", e);
//             Error::new(ErrorKind::Other, format!("IPFS GC request failed: {}", e))
//         })?;

//     // Check response status
//     if response.status() != StatusCode::OK {
//         let status = response.status();
//         eprintln!("Unexpected status code from IPFS GC request: {}", status);
//         return Err(Error::new(
//             ErrorKind::Other,
//             format!("IPFS GC request returned status: {}", status),
//         ));
//     }

//     // Log success (optional, for debugging)
//     println!("IPFS garbage collection triggered successfully");

//     Ok(())
// }

pub async fn pin_content_to_ipfs(state: &AppState, json_string: &str) -> Result<String, String> {
    // Use cid-version=1 to generate CIDv1
    let url = format!("https://store.hippius.network/api/v0/add?cid-version=1");
    // Create multipart form data
    let boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW";
    let body = format!(
        "--{}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"data.json\"\r\nContent-Type: application/json\r\n\r\n{}\r\n--{}--\r\n",
        boundary, json_string, boundary
    );

    // Send the request
    let response = state
        .ipfs_client
        .post(&url)
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(body)
        .timeout(std::time::Duration::from_secs(20)) // Increased timeout
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                format!("Request timed out: {}", e)
            } else if e.is_connect() {
                format!("Connection error: Check IPFS node URL - {}", e)
            } else {
                format!("Failed to send request: {}", e)
            }
        })?;

    if !response.status().is_success() {
        return Err(format!("Unexpected status code: {}", response.status()));
    }

    // Parse the response
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    // Extract and validate the CID
    if let Some(cid_str) = body["Hash"].as_str() {
        // Validate CID
        let _cid = Cid::from_str(cid_str)
            .map_err(|e| format!("Invalid CID returned: {} - Error: {}", cid_str, e))?;
        Ok(cid_str.to_string())
    } else {
        Err("No CID found in response".to_string())
    }
}


    // async fn pin_cids_concurrently(
    //     app_state: &AppState,
    //     cids: Vec<String>,
    // ) -> Result<Vec<String>, Vec<String>> {
    //     let mut pin_errors = Vec::new();
    //     let mut pinned_cids = Vec::new();

    //     // Collect all pinning futures for IPFS and Hippius
    //     let mut ipfs_pin_futures = cids
    //         .iter()
    //         .map(|cid| pin_cid_to_ipfs(app_state, cid))
    //         .collect::<Vec<_>>();

    //     let mut hippius_pin_futures = cids
    //         .iter()
    //         .map(|cid| pin_cid_to_hippius_network(cid))
    //         .collect::<Vec<_>>();

    //     // Batch processing parameters
    //     const BATCH_SIZE: usize = 10;
    //     const DELAY_SECS: u64 = 1;

    //     // Process batches by draining futures
    //     let mut batch_idx = 0;
    //     while !ipfs_pin_futures.is_empty() && !hippius_pin_futures.is_empty() {
    //         // Determine the number of futures to drain for this batch
    //         let batch_size = std::cmp::min(BATCH_SIZE, ipfs_pin_futures.len());

    //         // Drain futures for the current batch
    //         let ipfs_batch: Vec<_> = ipfs_pin_futures.drain(..batch_size).collect();
    //         let hippius_batch: Vec<_> = hippius_pin_futures.drain(..batch_size).collect();

    //         // Execute IPFS and Hippius pinning concurrently for the current batch
    //         let ipfs_results = join_all(ipfs_batch).await;
    //         let hippius_results = join_all(hippius_batch).await;

    //         // Process IPFS results
    //         let batch_start_idx = batch_idx * BATCH_SIZE;
    //         for (result_idx, result) in ipfs_results.into_iter().enumerate() {
    //             let global_idx = batch_start_idx + result_idx;
    //             let cid = cids
    //                 .get(global_idx)
    //                 .map(|cid| cid.as_str())
    //                 .unwrap_or_default();

    //             match result {
    //                 Ok(pinned_cid) => {
    //                     pinned_cids.push(pinned_cid);
    //                 }
    //                 Err(e) => {
    //                     pin_errors.push(format!("CID {}: {}", cid, e));
    //                 }
    //             }
    //         }

    //         // Process Hippius results
    //         for (result_idx, result) in hippius_results.into_iter().enumerate() {
    //             let global_idx = batch_start_idx + result_idx;
    //             let cid = cids
    //                 .get(global_idx)
    //                 .map(|cid| cid.as_str())
    //                 .unwrap_or_default();

    //             if let Err(e) = result {
    //                 pin_errors.push(format!("CID {} (Hippius): {}", cid, e));
    //             }
    //         }

    //         // Delay before the next batch (skip for the last batch)
    //         batch_idx += 1;
    //         if !ipfs_pin_futures.is_empty() {
    //             sleep(Duration::from_secs(DELAY_SECS)).await;
    //         }
    //     }

    //     if pin_errors.is_empty() {
    //         Ok(pinned_cids)
    //     } else {
    //         Err(pin_errors)
    //     }
    // }