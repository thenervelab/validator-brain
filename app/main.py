"""FastAPI application entry point."""

import asyncio
import logging
import sys
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.validator import validator_main_loop, shutdown_event
from app.db.connection import init_db_pool, close_db_pool
from app.services.substrate_client import (
    init_substrate_client,
    close_substrate_client,
)
from app.utils.http import close_http_client
from app.utils.logging import configure_logging, logger


# Create a wrapper function that starts the validator main loop
async def run_validator():
    """Run the validator main loop and handle exceptions properly."""
    try:
        await validator_main_loop()
    except Exception as e:
        # Log the error with the traceback but don't print it twice
        logger.critical(f"FATAL ERROR in validator loop: {e}", exc_info=True)
        # Print to stderr directly for immediate visibility
        print(f"\nFATAL ERROR: {e}", file=sys.stderr)
        # Small delay to ensure logs are flushed
        await asyncio.sleep(0.1)
        # Exit the process with a non-zero exit code to indicate failure
        os._exit(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan manager for startup and shutdown events.

    Args:
        app: FastAPI application instance
    """
    logger.info("Initializing IPFS Service Validator...")
    await init_db_pool()
    logger.info("Database pool initialized")

    # Initialize substrate client
    await init_substrate_client()
    logger.info("Substrate client initialized")
    
    # Start the validator in a background task
    app.state.validator_task = asyncio.create_task(run_validator())
    logger.info("Validator started in background task")
    
    yield

    logger.info("Shutting down IPFS Service Validator...")

    # Signal shutdown to validator loop
    shutdown_event.set()
    
    # Cancel the validator task
    if hasattr(app.state, "validator_task"):
        app.state.validator_task.cancel()
    
    # Close connections
    await close_substrate_client()
    await close_db_pool()
    await close_http_client()

    logger.info("Shutdown complete")


# Configure FastAPI app
app = FastAPI(
    title="Hippius Validator",
    description="Validator service for IPFS storage on Hippius network",
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

configure_logging(logging.INFO, "validator.log")


# Add a simple status endpoint
@app.get("/status")
async def status():
    """Check if the service is running."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
