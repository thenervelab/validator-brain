"""Main entry point for the IPFS service validator."""

import asyncio
import os
import signal
import sys
from typing import Optional

from dotenv import load_dotenv

from app.db.connection import close_db_pool, get_db_pool, init_db_pool
from app.services.substrate_client import close_substrate_client, init_substrate_client
from app.utils.logging import configure_logging, logger
from substrate_fetcher.health_monitor import HealthCheck
from substrate_fetcher.monitoring import initialize_monitoring, shutdown_monitoring
from epoch_orchestrator import main as orchestrator_main

load_dotenv()


async def main(validator_account_id: Optional[str] = None):
    """
    Main entry point for the IPFS service validator.

    Args:
        validator_account_id: Optional validator account ID override
    """
    # Configure logging
    configure_logging(log_level=os.environ.get("LOG_LEVEL", "INFO"))

    # Use provided account ID or get from environment
    account_id = validator_account_id or os.environ.get("VALIDATOR_ACCOUNT_ID")

    if not account_id:
        logger.error(
            "No validator account ID provided. Set VALIDATOR_ACCOUNT_ID environment variable.",
        )
        return

    logger.info(f"Starting IPFS service validator with account: {account_id}")
    logger.info("🚀 Using NEW Epoch Orchestrator (preserves health data)")

    # Initialize services
    await init_substrate_client()

    # Initialize database pool
    await init_db_pool()
    logger.info("Database pool initialized")

    # Initialize monitoring system
    db_pool = get_db_pool()
    await initialize_monitoring(db_pool)
    logger.info("Monitoring system initialized")

    # Start health monitoring
    health_check = HealthCheck(db_pool)
    await health_check.start()
    logger.info("Health monitoring started")

    # Setup signal handlers for graceful shutdown
    def signal_handler():
        logger.info("Shutdown signal received, stopping validator...")
        asyncio.create_task(shutdown(health_check))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(sig, signal_handler)

    # FIXED: Use new orchestrator instead of old validator workflow
    try:
        logger.info("🎯 Starting Epoch Orchestrator (NEW SYSTEM)")
        await orchestrator_main()
    except Exception as e:
        logger.exception(f"Error running orchestrator: {str(e)}")
        await shutdown(health_check)


async def shutdown(health_check):
    """
    Gracefully shut down the validator and close connections.

    Args:
        health_check: HealthCheck instance to stop
    """
    logger.info("Shutting down...")

    # Note: Orchestrator handles its own shutdown, no separate stop needed

    # Shutdown health monitoring
    await health_check.stop()
    logger.info("Health monitoring shutdown complete")

    # Shutdown monitoring system
    await shutdown_monitoring()
    logger.info("Monitoring system shutdown complete")

    # Close substrate client
    await close_substrate_client()

    # Close database pool
    await close_db_pool()
    logger.info("Database pool closed")

    # Exit after a short delay to allow cleanup
    await asyncio.sleep(1)
    sys.exit(0)


if __name__ == "__main__":
    # Get validator account ID from command line args if provided
    validator_id = None
    if len(sys.argv) > 1:
        validator_id = sys.argv[1]

    # Run the main function
    asyncio.run(main(validator_id))
