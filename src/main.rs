#![recursion_limit = "256"]
mod api;
mod assignment;
mod unpinning;
mod config;
mod types;
mod ipfs_utils;
mod transactions;
mod reconstruct_profile;
mod helpers;
mod substrate_fetcher;
use api::create_router;
use config::Config;
use governor::Quota;
use governor::RateLimiter;
use reqwest::Client;
use sqlx::Executor;
use sqlx::postgres::PgPool;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio;
use types::*;

// Placeholder for your actual database initialization logic if it's separate
// If initialize_database was only creating tables now in SubstrateFetcher::new,
// this function might be simplified or removed if not creating the DB itself.
async fn initialize_database(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Example: Create schema if it doesn't exist.
    // Table creations are now in SubstrateFetcher::new.
    // You might want to ensure the 'blockchain' schema exists.
    pool.execute("CREATE SCHEMA IF NOT EXISTS blockchain;")
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let config = Config::from_env();

    // Connect to PostgreSQL using our config
    let pool = Arc::new(
        config
            .create_pg_pool()
            .await
            .expect("Failed to connect to PostgreSQL"),
    );

    // Initialize database (e.g., create schema if not exists)
    // Table creations are handled in SubstrateFetcher::new
    initialize_database(&pool).await?;

    // Define IpfsPallet entries
    let ipfs_entries = vec![
        "UserStorageRequests".to_string(),
        "Blacklist".to_string(),
        "UserUnpinRequests".to_string(),
        "MinerProfile".to_string(),
        "UserProfile".to_string(),
        "PinningEnabled".to_string(),
        "AssignmentEnabled".to_string(),
        "Registration".to_string(),
        "RebalanceRequest".to_string(),
    ];

    // Initialize SubstrateFetcher
    let substrate_fetcher = Arc::new(
        substrate_fetcher::SubstrateFetcher::new(
            &config.rpc_url,
            pool.clone(),
            ipfs_entries.clone(),
            config.ipfs_node_url.clone(),
        )
        .await?,
    ) as Arc<substrate_fetcher::SubstrateFetcher>;

    // Configure rate limiting (e.g., 5 requests per second)
    let quota = Quota::per_second(NonZeroU32::new(5).unwrap());
    let rate_limiter = Arc::new(RateLimiter::direct(quota));

    // Create AppState for Axum
    let app_state = AppState {
        session: pool.clone(),
        ipfs_entries: ipfs_entries.clone(),
        ipfs_client: Client::new(),
        ipfs_node_url: config.ipfs_node_url.clone(),
        substrate_fetcher: substrate_fetcher.clone(),
        rate_limiter,
    };

    // Spawn Axum server
    let server = tokio::spawn(async move {
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], config.api_port));
        println!("API server running at http://{}", addr);
        axum::Server::bind(&addr)
            .serve(create_router(app_state).into_make_service())
            .await
            .expect("Failed to start API server");
    });

    // Spawn SubstrateFetcher to process blocks
    let fetcher_handle = tokio::spawn(async move {
        if let Err(e) = substrate_fetcher.process_blocks().await {
            eprintln!("Substrate fetcher error: {}", e);
        }
    });

    // Wait for both tasks to complete
    tokio::try_join!(server, fetcher_handle)?;

    Ok(())
}
