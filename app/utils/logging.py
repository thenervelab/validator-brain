"""Logging utilities."""

import logging
import sys

logger = logging.getLogger("ipfs_validator")


def configure_logging(log_level=logging.INFO, log_file=None):
    """
    Configure logging for the application.

    Args:
        log_level: Logging level (default: INFO)
        log_file: Path to log file (default: None, logs to stdout only)
    """
    logger.setLevel(log_level)

    # Configure a formatter that includes the timestamp and log level
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Remove existing handlers to avoid duplicates on reconfiguration
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Use a single console handler for simplicity and to avoid duplicate messages
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Prevent logging from propagating to the root logger
    logger.propagate = False
