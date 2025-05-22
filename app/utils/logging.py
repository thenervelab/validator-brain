"""Logging utilities."""
import logging
import sys

# Create logger
logger = logging.getLogger("ipfs_validator")


def configure_logging(log_level=logging.INFO, log_file=None):
    """
    Configure logging for the application.
    
    Args:
        log_level: Logging level (default: INFO)
        log_file: Path to log file (default: None, logs to stdout only)
    """
    logger.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Add stdout handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    # Prevent logging from propagating to the root logger
    logger.propagate = False