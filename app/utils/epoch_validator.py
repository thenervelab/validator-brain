#!/usr/bin/env python3
"""
Epoch Validator Utilities

This module provides functions to check if we are the current epoch validator
and manage epoch-related operations.
"""

import logging
import os
from typing import Optional, Tuple
from substrateinterface import SubstrateInterface
from app.utils.config import NODE_URL

logger = logging.getLogger(__name__)


def get_current_epoch_info(substrate: SubstrateInterface) -> Tuple[int, int]:
    """
    Get current epoch and block information.
    
    Args:
        substrate: Connected substrate interface
        
    Returns:
        Tuple of (current_epoch, current_block)
    """
    current_block = substrate.get_block_number(None)
    current_epoch = current_block // 100  # 100 blocks per epoch
    return current_epoch, current_block


def get_epoch_block_position(current_block: int) -> int:
    """
    Get the position within the current epoch (0-99).
    
    Args:
        current_block: Current block number
        
    Returns:
        Block position within epoch (0-99)
    """
    return current_block % 100


def get_epoch_start_block(epoch: int) -> int:
    """
    Get the starting block number for a given epoch.
    
    Args:
        epoch: Epoch number
        
    Returns:
        Starting block number for the epoch
    """
    return epoch * 100


def is_epoch_validator(substrate: SubstrateInterface, our_validator_account: str) -> Tuple[bool, Optional[str], Optional[int]]:
    """
    Check if we are the current epoch validator.
    
    Args:
        substrate: Connected substrate interface
        our_validator_account: Our validator account ID from environment
        
    Returns:
        Tuple of (is_validator, current_validator_account, epoch_start_block)
    """
    try:
        # Query the current epoch validator from the chain
        result = substrate.query(
            module='IpfsPallet',
            storage_function='CurrentEpochValidator'
        )
        
        if result is None or result.value is None:
            logger.warning("No current epoch validator found on chain")
            return False, None, None
        
        # Extract validator account and epoch start block
        validator_account, epoch_start_block = result.value
        
        # Convert to string for comparison
        current_validator = str(validator_account)
        epoch_start = int(epoch_start_block)
        
        logger.info(f"Current epoch validator: {current_validator}")
        logger.info(f"Epoch start block: {epoch_start}")
        logger.info(f"Our validator account: {our_validator_account}")
        
        # Check if we are the validator
        is_validator = (current_validator == our_validator_account)
        
        if is_validator:
            logger.info("✅ We ARE the current epoch validator")
        else:
            logger.info("❌ We are NOT the current epoch validator")
        
        return is_validator, current_validator, epoch_start
        
    except Exception as e:
        logger.error(f"Error checking epoch validator: {e}")
        return False, None, None


def get_validator_account_from_env() -> str:
    """
    Get our validator account ID from environment variables.
    
    Returns:
        Validator account ID
        
    Raises:
        ValueError: If VALIDATOR_ACCOUNT_ID is not set
    """
    validator_account = os.getenv('VALIDATOR_ACCOUNT_ID')
    if not validator_account:
        raise ValueError("VALIDATOR_ACCOUNT_ID environment variable is required")
    
    return validator_account


def connect_substrate() -> SubstrateInterface:
    """
    Connect to the substrate chain.
    
    Returns:
        Connected substrate interface
    """
    logger.info(f"Connecting to substrate at {NODE_URL}")
    substrate = SubstrateInterface(url=NODE_URL)
    logger.info("Connected to substrate")
    return substrate 