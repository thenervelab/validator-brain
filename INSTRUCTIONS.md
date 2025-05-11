# IPFS Substrate Indexer - Instructions

This document provides instructions on how to set up and run the IPFS Substrate Indexer service.

## 1. Prerequisites

Before you begin, ensure you have the following installed on your system:

*   **Python**: Version 3.10 or higher (including `pip` and `venv`).
*   **Docker**: For running the PostgreSQL database.
*   **Docker Compose**: For easily managing the Docker services.
*   **Git**: For cloning the repository (if you haven't already).
*   **A running Substrate Node**: The indexer needs to connect to your specific Substrate node that includes the `IpfsPallet`.

## 2. Configuration

Configuration for the indexer is managed through an `.env` file in the project root.

1.  **Create `.env.example` (if it doesn't exist or you want to refresh it from the latest guidance):
    ```env
    # Substrate Node Configuration
    SUBSTRATE_NODE_URL="ws://127.0.0.1:9944"

    # PostgreSQL Database Configuration
    POSTGRES_USER="user"
    POSTGRES_PASSWORD="password"
    POSTGRES_DB="ipfs_substrate_indexer"
    POSTGRES_HOST="localhost"
    POSTGRES_PORT="5432"

    # Optional: For verbose SQL logging from SQLAlchemy
    # ECHO_SQL="False"

    # Optional: Polling interval for checking new blocks
    # POLLING_INTERVAL_SECONDS="12"
    ```
    Commit `.env.example` to your repository as a template for other developers.

2.  **Create your actual `.env` file**:
    Copy `.env.example` to `.env` in the project root:
    ```bash
    cp .env.example .env
    ```
    **Edit the `.env` file** with your specific configuration details:
    *   `SUBSTRATE_NODE_URL`: The WebSocket URL of your running Substrate node (e.g., `ws://your-node-ip:9944`).
    *   `POSTGRES_USER`: The username for your PostgreSQL database.
    *   `POSTGRES_PASSWORD`: The password for your PostgreSQL database.
    *   `POSTGRES_DB`: The name of the PostgreSQL database to use.
    *   `POSTGRES_HOST`: Should usually be `localhost` if running Postgres via Docker on the same machine.
    *   `POSTGRES_PORT`: Should usually be `5432`.
    *   `ECHO_SQL` (Optional): Set to `True` to see SQLAlchemy generated SQL queries in the logs (useful for debugging).
    *   `POLLING_INTERVAL_SECONDS` (Optional): How often the indexer checks for new blocks (default is 12 seconds).

    **Important**: The `.env` file contains sensitive credentials and should **NOT** be committed to your Git repository. Ensure it is listed in your `.gitignore` file (it should be, via `*.env`).

## 3. Install Dependencies

1.  **Create and activate a Python virtual environment** (if you haven't already):
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
    (On Windows, use `venv\Scripts\activate`)

2.  **Install the required Python packages**:
    ```bash
    pip install -r requirements.txt
    ```

## 4. Run PostgreSQL Database

The project uses Docker Compose to manage the PostgreSQL database service.

1.  **Ensure Docker Desktop is running.**
2.  **Start the PostgreSQL service** from the project root directory:
    ```bash
    docker-compose up -d
    ```
    The `-d` flag runs the service in detached mode (in the background).
    *   The first time you run this, Docker will download the `postgres:15-alpine` image.
    *   The database credentials and name used by Docker Compose are defined in `docker-compose.yml`. Make sure these match what you've set in your `.env` file (specifically `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` in `.env` should match the `environment` section for the `db` service in `docker-compose.yml`). The provided `docker-compose.yml` uses `user`, `password`, and `ipfs_substrate_indexer` by default.

3.  **To check the logs** of the database container:
    ```bash
    docker-compose logs -f db
    ```

4.  **To stop the database service**:
    ```bash
    docker-compose down
    ```
    If you want to remove the persisted data volume as well (to start with a completely fresh database next time), use:
    ```bash
    docker-compose down -v
    ```

## 5. Ensure Your Substrate Node is Running

This indexer service needs to connect to your specific Substrate node that implements the `IpfsPallet` (or equivalent). Ensure your Substrate node is running and accessible at the `SUBSTRATE_NODE_URL` you configured in your `.env` file.

## 6. Run the Indexer Application

Once your `.env` file is configured, dependencies are installed, PostgreSQL is running, and your Substrate node is running, you can start the indexer:

1.  **Activate your virtual environment** (if not already active):
    ```bash
    source venv/bin/activate
    ```
2.  **Run the main application script** from the project root directory:
    ```bash
    python main.py
    ```

The indexer will start, attempt to connect to the Substrate node and the database, create database tables if they don't exist, and then begin polling for new blocks to process.

*   You should see log messages indicating its progress.
*   To stop the indexer, press `Ctrl+C` in the terminal where it's running. It should attempt a graceful shutdown.

## 7. Testing and Data Verification (Crucial Next Steps)

As outlined in previous discussions, to ensure the indexer is working correctly for your specific pallet, you **must** verify and potentially adjust:

1.  **`_convert_file_hash_to_cid_string()` in `main.py`**: Test this function with actual `file_hash` arrays from your chain to ensure it correctly produces the CID strings your system expects. Add print/debug statements if needed.

2.  **`UserProfile` Bytes Decoding in `main.py`**: Verify that `bytes.fromhex(profile_bytes_hex).decode('utf-8')` and `json.loads()` correctly decodes the `profile_bytes` (from `substrate_fetcher.py`) into the expected list-of-dictionaries structure for user files. If the encoding is different (e.g., SCALE, non-UTF8 JSON), adjust this logic.

3.  **Role of Request Queues**: Understand if `PinningRequest`, `RebalanceRequest`, etc., from your pallet need to be directly processed to determine the current state of pinned CIDs, or if the `UserProfile.miner_ids` is the sole authoritative source. The current implementation relies on `UserProfile.miner_ids`.

4.  **Global vs. Per-Miner Stats**: Confirm if `minerTotalFilesPinned` and `minerTotalFilesSize` are truly global or if they exist per-miner in your pallet's storage. Adjust fetching and `upsert_miner` if they are per-miner.

Refer to the console logs from `main.py` during its operation to observe the data being fetched and processed. Add more detailed logging (e.g., `logger.debug()`) around critical data transformation steps as needed during your testing and verification phase.

Good luck! 