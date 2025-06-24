"""Epoch phase management and actions.

This module defines the different phases of an epoch and
the actions to be performed in each phase.
"""

from enum import Enum
from typing import Optional, Tuple

from pydantic import BaseModel

from app.utils.logging import logger
from app.utils.epoch_validator import get_epoch_block_position


class EpochPhase(str, Enum):
    """Epoch phase definitions."""

    EARLY = "early"  # Position 70-100 (last 30% of epoch)
    MID = "mid"  # Position 30-70 (middle 40% of epoch)
    LATE = "late"  # Position 0-30 (first 30% of epoch)


class PhaseActions(BaseModel):
    """Actions to perform in a specific epoch phase."""

    phase: EpochPhase
    storage_processing: bool = False
    health_checking: bool = False
    content_verification: bool = False
    profile_reconstruction: bool = False
    blockchain_submission: bool = False
    file_redistribution: bool = False


class EpochPhaseManager:
    """Manages epoch phases and their associated actions."""

    @staticmethod
    def determine_phase(block_number: int, epoch_length: int = 100) -> Tuple[EpochPhase, float]:
        """
        Determine the current phase in the epoch and position percentage.

        Args:
            block_number: Current block number
            epoch_length: Number of blocks in an epoch

        Returns:
            Tuple of (epoch_phase, position_percentage)
        """
        # Calculate current position within epoch (0 to epoch_length-1) using correct calculation
        position_in_epoch = get_epoch_block_position(block_number)

        # Convert to percentage (0 to 100)
        position_percentage = (position_in_epoch / epoch_length) * 100

        # Determine phase based on percentage
        if position_percentage >= 70:
            return EpochPhase.EARLY, position_percentage
        if position_percentage >= 30:
            return EpochPhase.MID, position_percentage
        return EpochPhase.LATE, position_percentage

    @staticmethod
    def get_phase_actions(phase: EpochPhase, block_number: Optional[int] = None) -> PhaseActions:
        """
        Get the actions to perform for a specific epoch phase.

        Args:
            phase: The epoch phase
            block_number: Optional block number for special block handling

        Returns:
            PhaseActions object with flags for which actions to perform
        """
        # Initialize with default phase actions
        actions = PhaseActions(phase=phase)

        if phase == EpochPhase.EARLY:
            # Early epoch: Process storage requests and assign to miners
            actions.storage_processing = True

        elif phase == EpochPhase.MID:
            # Mid epoch: Check miner health and content availability
            actions.health_checking = True
            actions.content_verification = True

        elif phase == EpochPhase.LATE:
            # Late epoch: Finalize validation and prepare blockchain submissions
            actions.blockchain_submission = True

            # Special handling for block 90
            if block_number and get_epoch_block_position(block_number) == 90:
                actions.profile_reconstruction = True
                actions.file_redistribution = True

        return actions

    @staticmethod
    def log_phase_info(phase: EpochPhase, block_number: int, epoch_end_block: int):
        """
        Log information about the current epoch phase.

        Args:
            phase: Current epoch phase
            block_number: Current block number
            epoch_end_block: Block number when epoch ends
        """
        position = epoch_end_block - block_number
        logger.info(
            f"Current epoch phase: {phase} (Block {block_number}, "
            f"Position {position} blocks from end, Epoch ends at {epoch_end_block})",
        )
