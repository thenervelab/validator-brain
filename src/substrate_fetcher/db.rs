use sqlx::PgPool;
use std::sync::Arc;

pub async fn initialize_database(session: Arc<PgPool>) -> Result<(), Box<dyn std::error::Error>> {
        // Create processed_storage_requests table if it doesn't exist
        let create_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.processed_storage_requests (
                file_hash TEXT,
                owner TEXT,
                processed_at TIMESTAMP WITHOUT TIME ZONE,
                PRIMARY KEY (file_hash, owner)
            )
        ";
        sqlx::query(create_table_query).execute(&*session).await?;

        // Create in_progress_storage_requests table if it doesn't exist
        let create_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.in_progress_storage_requests (
                file_hash TEXT,
                owner TEXT,
                PRIMARY KEY (file_hash, owner)
            )
        ";
        sqlx::query(create_table_query).execute(&*session).await?;

        // Create processed_unpin_requests table if it doesn't exist
        let create_unpin_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.processed_unpin_requests (
                file_hash TEXT,
                owner TEXT,
                processed_at TIMESTAMP WITHOUT TIME ZONE,
                PRIMARY KEY (file_hash, owner)
            )
        ";
        sqlx::query(create_unpin_table_query)
            .execute(&*session)
            .await?;

        // Create in_progress_unpin_requests table if it doesn't exist
        let create_in_progress_unpin_table_query = "
        CREATE TABLE IF NOT EXISTS blockchain.in_progress_unpin_requests (
            file_hash TEXT,
            owner TEXT,
            PRIMARY KEY (file_hash, owner)
        )
        ";
        sqlx::query(create_in_progress_unpin_table_query)
            .execute(&*session)
            .await?;

        let create_failed_unpin_table_query = "
        CREATE TABLE IF NOT EXISTS blockchain.failed_unpin_requests (
            file_hash TEXT,
            owner TEXT,
            failure_count INTEGER,
            last_failed_at TIMESTAMP WITHOUT TIME ZONE,
            PRIMARY KEY (file_hash, owner)
        )";
        sqlx::query(create_failed_unpin_table_query)
            .execute(&*session)
            .await?;

        // Create epoch_miners table if it doesn't exist
        let create_epoch_miners_query = "
            CREATE TABLE IF NOT EXISTS blockchain.epoch_miners (
                miner_id TEXT,
                ipfs_node_id TEXT,
                PRIMARY KEY (miner_id)
            )
        ";
        sqlx::query(create_epoch_miners_query)
            .execute(&*session)
            .await?;

        // Create processed_rebalance_requests table if it doesn't exist
        let create_processed_rebalance_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.processed_rebalance_requests (
                old_miner_id TEXT,
                miner_profile_id TEXT,
                processed_at TIMESTAMP WITHOUT TIME ZONE,
                PRIMARY KEY (old_miner_id, miner_profile_id)
            )
        ";
        sqlx::query(create_processed_rebalance_table_query)
            .execute(&*session)
            .await?;

        // Create in_progress_rebalance_requests table if it doesn't exist
        let create_in_progress_rebalance_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.in_progress_rebalance_requests (
                old_miner_id TEXT,
                miner_profile_id TEXT,
                PRIMARY KEY (old_miner_id, miner_profile_id)
            )
        ";
        sqlx::query(create_in_progress_rebalance_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_userstoragerequest table if it doesn't exist
        let create_userstoragereq_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_userstoragerequests (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_userstoragereq_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_nodemetrics table if it doesn't exist
        let create_nodemetrics_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_nodemetrics (
                node_id TEXT PRIMARY KEY,
                miner_id TEXT,
                bandwidth_mbps BIGINT,
                current_storage_bytes BIGINT,
                total_storage_bytes BIGINT,
                geolocation TEXT,
                successful_pin_checks INT,
                total_pin_checks INT,
                storage_proof_time_ms INT,
                storage_growth_rate BIGINT,
                latency_ms INT,
                total_latency_ms BIGINT,
                total_times_latency_checked INT,
                avg_response_time_ms INT,
                peer_count INT,
                failed_challenges_count INT,
                successful_challenges INT,
                total_challenges INT,
                uptime_minutes INT,
                total_minutes INT,
                consecutive_reliable_days INT,
                recent_downtime_hours INT,
                is_sev_enabled BOOLEAN,
                ipfs_zfs_pool_size TEXT,
                ipfs_zfs_pool_alloc TEXT,
                ipfs_zfs_pool_free TEXT,
                ipfs_repo_size BIGINT,
                ipfs_storage_max BIGINT,
                cpu_model TEXT,
                cpu_cores INT,
                memory_mb BIGINT,
                free_memory_mb BIGINT,
                gpu_name TEXT,
                gpu_memory_mb INT,
                hypervisor_disk_type TEXT,
                vm_pool_disk_type TEXT,
                vm_count INT,
                disks JSONB,
                disk_info JSONB,
                zfs_info JSONB,
                raid_info JSONB,
                primary_network_interface JSONB,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ";
        sqlx::query(create_nodemetrics_table_query)
            .execute(&*session)
            .await?;

        let create_registration_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_registration (
                node_id TEXT PRIMARY KEY,
                owner TEXT,
                status TEXT,
                node_type TEXT,
                ipfs_node_id TEXT,
                registered_at BIGINT,
                storage_value JSONB
            )
        ";
        sqlx::query(create_registration_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_minerprofile table if it doesn't exist
        let create_minerprofile_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_minerprofile (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_minerprofile_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_userprofile table if it doesn't exist
        let create_userprofile_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_userprofile (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_userprofile_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_blacklist table if it doesn't exist
        let create_blacklist_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_blacklist (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_blacklist_table_query)
            .execute(&*session)
            .await?;

        // Create cidsInfo table if it doesn't exist
        let create_cidsinfo_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.cidsInfo (
                hash_key TEXT PRIMARY KEY,
                item_value JSONB
            )
        ";
        sqlx::query(create_cidsinfo_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_userunpinrequest table if it doesn't exist
        let create_userunpinreq_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_userunpinrequests (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_userunpinreq_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_pinningenabled table if it doesn't exist
        let create_pinningenabled_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_pinningenabled (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_pinningenabled_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_assignmentenabled table if it doesn't exist
        let create_assignmentenabled_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_assignmentenabled (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_assignmentenabled_table_query)
            .execute(&*session)
            .await?;

        // Create ipfs_rebalancerequest table if it doesn't exist
        let create_rebalancereq_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.ipfs_rebalancerequest (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_rebalancereq_table_query)
            .execute(&*session)
            .await?;

        // Create registration table if it doesn't exist
        let create_registration_table_query = "
            CREATE TABLE IF NOT EXISTS blockchain.registration (
                storage_key TEXT PRIMARY KEY,
                storage_value TEXT
            )
        ";
        sqlx::query(create_registration_table_query)
            .execute(&*session)
            .await?;
    Ok(())
}
