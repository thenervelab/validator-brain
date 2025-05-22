"""FastAPI application entry point."""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from app.core.validator import validator_main_loop, handle_signals
from app.db.connection import init_db_pool, close_db_pool
from app.services.substrate_client import init_substrate_client, close_substrate_client, substrate_client
from app.services.substrate_fetcher import fetch_and_store_blockchain_data, periodic_blockchain_sync
from app.utils.http import close_http_client
from app.utils.logging import configure_logging, logger

# Main background tasks
validator_task = None
blockchain_task = None


async def blockchain_subscription_loop():
    """Background task to subscribe to blockchain events."""
    logger.info("Starting blockchain subscription loop...")
    
    # Connect to Substrate node
    node_url = os.environ.get("NODE_URL")
    logger.info(f"Using Substrate node at {node_url}")
    
    # Initialize the client and subscribe to blocks
    await init_substrate_client()
    await substrate_client.subscribe_to_blocks()
    
    # Keep running until cancelled
    while True:
        await asyncio.sleep(60)  # Just keep the task alive


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan manager for startup and shutdown events.
    
    Args:
        app: FastAPI application instance
    """
    global validator_task, blockchain_task
    
    # Startup
    logger.info("Initializing IPFS Service Validator...")
    
    # Initialize database pool
    await init_db_pool()
    logger.info("Database pool initialized")
    
    # Set up signal handlers
    handle_signals()
    
    # Start blockchain subscription
    blockchain_task = asyncio.create_task(blockchain_subscription_loop())
    
    # Initialize substrate client and fetch blockchain data
    await init_substrate_client()
    logger.info("Fetching initial blockchain data...")
    await fetch_and_store_blockchain_data()
    logger.info("Initial blockchain data fetched")
    
    # Start periodic blockchain sync in background
    asyncio.create_task(periodic_blockchain_sync())
    logger.info("Periodic blockchain sync started")
    
    # Start validator main loop
    validator_task = asyncio.create_task(validator_main_loop())
    logger.info("Validator main loop started")
    
    # Yield control back to FastAPI
    yield
    
    # Shutdown
    logger.info("Shutting down IPFS Service Validator...")
    
    # Cancel tasks
    validator_task.cancel()
    blockchain_task.cancel()
    
    # Close connections
    await close_substrate_client()
    await close_db_pool()
    await close_http_client()
    
    logger.info("Shutdown complete")


# Configure FastAPI app
app = FastAPI(
    title="IPFS Service Validator",
    description="Validator service for IPFS storage on Substrate-based blockchain networks",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development - restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint that returns basic service information."""
    return {
        "service": "IPFS Service Validator",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/metrics")
async def metrics():
    """Simple metrics endpoint for monitoring."""
    from app.services.db_manager import get_latest_block_number, get_storage_miners, get_storage_requests_count
    from app.services.substrate_client import substrate_client
    
    # Get actual metrics
    latest_block = await get_latest_block_number() or 0
    
    # Get count of miners and storage requests
    miners = await get_storage_miners()
    miners_count = len(miners) if miners else 0
    
    # Get count of storage requests
    storage_requests_count = await get_storage_requests_count()
    
    return {
        "validators_running": 1,
        "blocks_processed": latest_block,
        "substrate_connected": substrate_client.connected,
        "node_url": substrate_client.node_url,
        "miners_monitored": miners_count,
        "storage_requests_processed": storage_requests_count
    }
    
    
@app.get("/substrate-status")
async def substrate_status():
    """Get information about the Substrate node connection."""
    from app.services.substrate_client import substrate_client
    
    if not substrate_client.connected:
        return {
            "status": "disconnected",
            "url": substrate_client.node_url
        }
    
    # Get some basic chain information
    loop = asyncio.get_event_loop()
    system_name = await loop.run_in_executor(
        None, 
        lambda: substrate_client.substrate.get_constant(
            module_name="System",
            constant_name="Version"
        )
    )
    
    latest_finalized = await loop.run_in_executor(
        None,
        lambda: substrate_client.substrate.get_block_number(
            substrate_client.substrate.get_chain_finalised_head()
        )
    )
    
    return {
        "status": "connected",
        "url": substrate_client.node_url,
        "system_name": system_name.value if system_name else "Unknown",
        "latest_finalized_block": latest_finalized
    }


@app.post("/fetch-blockchain-data")
async def fetch_blockchain_data(background_tasks: BackgroundTasks):
    """Manually trigger fetching of blockchain data."""
    from app.services.substrate_fetcher import fetch_and_store_blockchain_data
    
    # Execute in background to avoid blocking the request
    background_tasks.add_task(fetch_and_store_blockchain_data)
    
    return {
        "status": "fetching",
        "message": "Blockchain data fetch initiated in background"
    }


# Initialize logging
configure_logging(logging.INFO, "validator.log")