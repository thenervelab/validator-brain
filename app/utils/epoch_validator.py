#!/usr/bin/env python3
"""
Epoch Validator Utilities

This module provides functions to check if we are the current epoch validator
and manage epoch-related operations.
"""

import logging
import os
import time
from typing import Optional, Tuple
from substrateinterface import SubstrateInterface
from app.utils.config import NODE_URL

logger = logging.getLogger(__name__)


def get_current_epoch_info(substrate: SubstrateInterface) -> Tuple[int, int, SubstrateInterface]:
    """
    Get current epoch and block information with retry logic.
    
    Args:
        substrate: Connected substrate interface
        
    Returns:
        Tuple of (current_epoch, current_block, updated_substrate)
    """
    max_retries = 3
    current_substrate = substrate
    
    for attempt in range(max_retries):
        try:
            current_block = current_substrate.get_block_number(None)
            current_epoch = calculate_epoch_from_block(current_block)
            return current_epoch, current_block, current_substrate
        except Exception as e:
            logger.warning(f"Failed to get epoch info (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait before retry
                # Try to reconnect
                try:
                    current_substrate.close()
                    current_substrate = connect_substrate()
                    logger.info("Reconnected substrate for epoch info")
                except Exception as reconnect_error:
                    logger.warning(f"Reconnection failed: {reconnect_error}")
            else:
                raise


def calculate_epoch_from_block(block_number: int) -> int:
    """
    Calculate epoch number from block number.
    Epochs start at blocks ending in 38 (e.g., 771538, 771638, 771738, etc.)
    
    Args:
        block_number: Current block number
        
    Returns:
        Epoch number
    """
    # Epochs start at blocks ending in 38, with 100 blocks per epoch
    # To find the epoch, we need to adjust for the 38 offset
    # Example: block 771538 is epoch start, block 771637 is epoch end
    
    # Adjust block number by subtracting 38 to align with 0-based epochs
    adjusted_block = block_number - 38
    
    # If we're before the first epoch (block < 38), we're in epoch 0
    if adjusted_block < 0:
        return 0
    
    # Calculate epoch from adjusted block
    epoch = adjusted_block // 100
    return epoch


def get_epoch_block_position(current_block: int) -> int:
    """
    Get the position within the current epoch (0-99).
    Epochs start at blocks ending in 38.
    
    Args:
        current_block: Current block number
        
    Returns:
        Block position within epoch (0-99)
    """
    # Adjust block number by subtracting 38 to align with 0-based epochs
    adjusted_block = current_block - 38
    
    # If we're before the first epoch (block < 38), position is the block number itself
    if adjusted_block < 0:
        return current_block
    
    # Calculate position within epoch
    position = adjusted_block % 100
    return position


def get_epoch_start_block(epoch: int) -> int:
    """
    Get the starting block number for a given epoch.
    Epochs start at blocks ending in 38.
    
    Args:
        epoch: Epoch number
        
    Returns:
        Starting block number for the epoch
    """
    # Epoch 0 starts at block 38, epoch 1 starts at block 138, etc.
    return (epoch * 100) + 38


def get_epoch_end_block(epoch: int) -> int:
    """
    Get the ending block number for a given epoch.
    
    Args:
        epoch: Epoch number
        
    Returns:
        Ending block number for the epoch (inclusive)
    """
    return get_epoch_start_block(epoch) + 99


def is_epoch_validator(substrate: SubstrateInterface, our_validator_account: str) -> Tuple[bool, Optional[str], Optional[int], SubstrateInterface]:
    """
    Check if we are the current epoch validator with retry logic.
    
    Args:
        substrate: Connected substrate interface
        our_validator_account: Our validator account ID from environment
        
    Returns:
        Tuple of (is_validator, current_validator_account, epoch_start_block, updated_substrate)
    """
    max_retries = 3
    current_substrate = substrate
    
    for attempt in range(max_retries):
        try:
            # Query the current epoch validator from the chain
            result = current_substrate.query(
                module='IpfsPallet',
                storage_function='CurrentEpochValidator'
            )
            
            if result is None or result.value is None:
                logger.warning("No current epoch validator found on chain")
                return False, None, None, current_substrate
            
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
            
            return is_validator, current_validator, epoch_start, current_substrate
            
        except Exception as e:
            logger.warning(f"Failed to check epoch validator (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait before retry
                # Try to reconnect
                try:
                    current_substrate.close()
                    current_substrate = connect_substrate()
                    logger.info("Reconnected substrate for validator check")
                except Exception as reconnect_error:
                    logger.warning(f"Reconnection failed: {reconnect_error}")
            else:
                logger.error(f"Error checking epoch validator after {max_retries} attempts: {e}")
                return False, None, None, current_substrate


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
    Connect to the substrate chain with retry logic.
    
    Returns:
        Connected substrate interface
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to substrate at {NODE_URL} (attempt {attempt + 1}/{max_retries})")
            substrate = SubstrateInterface(
                url=NODE_URL,
                use_remote_preset=True
            )
            logger.info("Connected to substrate")
            return substrate
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Wait before retry
            else:
                logger.error(f"Failed to connect to substrate after {max_retries} attempts")
                raise 