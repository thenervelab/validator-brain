use dotenv::dotenv;
use sqlx::postgres::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::sync::OnceLock;
static CONFIG: OnceLock<Config> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct Config {
    // Substrate/chain settings
    pub rpc_url: String,
    pub keystore_path: String,
    pub epoch_length: u64,
    pub epoch_cleanup_block: u64,
    pub validator_address: Option<String>,

    // API settings
    pub api_port: u16,

    // PostgreSQL database settings
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_user: String,
    pub postgres_password: String,
    pub postgres_database: String,
    pub postgres_max_connections: u32,

    // IPFS settings
    pub ipfs_node_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        dotenv().ok();

        let rpc_url =
            env::var("HIPPIUS_RPC_URL").unwrap_or_else(|_| "ws://127.0.0.1:9944".to_string());
        let api_port = env::var("HIPPIUS_API_PORT")
            .unwrap_or_else(|_| "3000".to_string())
            .parse()
            .unwrap_or(3000);
        let keystore_path = env::var("HIPPIUS_KEYSTORE_PATH")
            .unwrap_or_else(|_| "/opt/hippius/data/chains/hippius-mainnet/keystore/".to_string());
        let epoch_length = env::var("HIPPIUS_EPOCH_LENGTH")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100);
        let epoch_cleanup_block = env::var("HIPPIUS_EPOCH_CLEANUP_BLOCK")
            .unwrap_or_else(|_| "98".to_string())
            .parse()
            .unwrap_or(98);
        // Set validator address from environment if provided, otherwise None
        let validator_address = env::var("HIPPIUS_VALIDATOR_ADDRESS").ok();

        // Database configuration
        let postgres_host = env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
        let postgres_port = env::var("POSTGRES_PORT")
            .unwrap_or_else(|_| "5432".to_string())
            .parse()
            .unwrap_or(5432);
        let postgres_user = env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string());
        let postgres_password =
            env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "password".to_string());
        let postgres_database = env::var("POSTGRES_DB").unwrap_or_else(|_| "hippius".to_string());
        let postgres_max_connections = env::var("POSTGRES_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .unwrap_or(5);

        // IPFS configuration
        let ipfs_node_url = env::var("IPFS_NODE_URL")
            .unwrap_or_else(|_| "https://store.hippius.network".to_string());
        // let ipfs_network_url =
        //     env::var("IPFS_NETWORK_URL").unwrap_or_else(|_| "get.hippius.network".to_string());

        Self {
            rpc_url,
            api_port,
            keystore_path,
            epoch_length,
            epoch_cleanup_block,
            validator_address,
            postgres_host,
            postgres_port,
            postgres_user,
            postgres_password,
            postgres_database,
            postgres_max_connections,
            ipfs_node_url,
            
        }
    }

    pub fn postgres_connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.postgres_user,
            self.postgres_password,
            self.postgres_host,
            self.postgres_port,
            self.postgres_database
        )
    }

    pub async fn create_pg_pool(&self) -> Result<PgPool, sqlx::Error> {
        PgPoolOptions::new()
            .max_connections(self.postgres_max_connections)
            .connect(&self.postgres_connection_string())
            .await
    }

    // Static method to get singleton instance
    pub fn get() -> &'static Config {
        CONFIG.get_or_init(|| Config::from_env())
    }
}
