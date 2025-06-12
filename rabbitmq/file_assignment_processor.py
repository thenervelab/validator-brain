#!/usr/bin/env python3
"""
File Assignment Processor

This processor runs near the end of the epoch to assign files from the pending_assignment_file 
table to miners with proper capacity checking and network balancing.

The processor:
1. Fetches unassigned files from pending_assignment_file table
2. Fetches files with empty miner slots from file_assignments table
3. Gets online miners with capacity information
4. Uses advanced scoring algorithm to balance assignments
5. Considers miner registration dates to help new miners
6. Respects storage capacity and health scores
7. Queues assignment tasks to RabbitMQ for processing
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from aio_pika import Message
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.db.connection import init_db_pool, close_db_pool, get_db_pool
from app.utils.config import NODE_URL
from app.utils.epoch_validator import calculate_epoch_from_block

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FileAssignmentProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'file_assignment_processing'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.substrate = None
        self.db_pool = None
        
        # Assignment configuration
        self.replicas_per_file = int(os.getenv('REPLICAS_PER_FILE', '5'))
        self.max_files_per_batch = int(os.getenv('MAX_FILES_PER_BATCH', '100'))
        self.max_reassignments_per_batch = int(os.getenv('MAX_REASSIGNMENTS_PER_BATCH', '50'))
        self.min_miner_health_score = float(os.getenv('MIN_MINER_HEALTH_SCORE', '20.0'))
        self.new_miner_boost_days = int(os.getenv('NEW_MINER_BOOST_DAYS', '30'))
        self.new_miner_boost_factor = float(os.getenv('NEW_MINER_BOOST_FACTOR', '1.5'))
        
        # Load balancing configuration
        self.assignment_penalty_factor = float(os.getenv('ASSIGNMENT_PENALTY_FACTOR', '0.8'))  # Penalty per assignment
        self.max_assignments_per_miner = int(os.getenv('MAX_ASSIGNMENTS_PER_MINER', '10'))  # Per batch
        
        # Pin check failure detection configuration
        self.pin_check_failure_threshold = float(os.getenv('PIN_CHECK_FAILURE_THRESHOLD', '50.0'))  # 50% success rate
        self.recent_epochs_window = int(os.getenv('RECENT_EPOCHS_WINDOW', '1'))  # Look back 3 epochs
        self.max_failing_replacements_per_batch = int(os.getenv('MAX_FAILING_REPLACEMENTS_PER_BATCH', '50'))  # Per batch
        
        # Track assignments within current batch for load balancing
        self.batch_assignments = {}  # miner_id -> count
        
    def connect_substrate(self):
        """Connect to the substrate chain."""
        logger.info(f"Connecting to substrate at {NODE_URL}")
        self.substrate = SubstrateInterface(url=NODE_URL)
        logger.info("Connected to substrate")
        
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare the queue."""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Declare the queue
            await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    def get_current_epoch(self) -> int:
        """Get the current epoch from the blockchain."""
        try:
            if not self.substrate:
                self.connect_substrate()
            
            block_number = self.substrate.get_block_number(None)
            
            epoch = calculate_epoch_from_block(block_number)
            logger.info(f"Current block: {block_number}, epoch: {epoch}")
            return epoch
        except Exception as e:
            logger.error(f"Error getting current epoch: {e}")
            return 0
    
    async def get_pending_files(self) -> List[Dict[str, Any]]:
        """Get files that need assignment from pending_assignment_file table."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT paf.id, paf.cid, paf.owner, paf.filename, paf.file_size_bytes, paf.created_at
                FROM pending_assignment_file paf
                WHERE paf.status = 'processed' 
                  AND paf.file_size_bytes IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM file_assignments fa 
                      WHERE fa.cid = paf.cid
                  )
                ORDER BY paf.created_at ASC
                LIMIT $1
            """, self.max_files_per_batch)
            
            return [dict(row) for row in rows]
    
    async def get_files_needing_reassignment(self) -> List[Dict[str, Any]]:
        """
        Get files that have empty miner slots and need reassignment.
        
        ENHANCED: Prioritizes files with more NULL miners, especially completely empty ones.
        This catches files like the example that have NULL miners and ensures they get fixed.
        """
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    fa.cid,
                    fa.owner,
                    f.name as filename,
                    f.size as file_size_bytes,
                    fa.miner1,
                    fa.miner2,
                    fa.miner3,
                    fa.miner4,
                    fa.miner5,
                    fa.updated_at,
                    -- Count NULL miners for prioritization
                    (CASE WHEN fa.miner1 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner2 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner3 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner4 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner5 IS NULL THEN 1 ELSE 0 END) as null_miner_count
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL OR 
                       fa.miner4 IS NULL OR fa.miner5 IS NULL)
                  AND f.size IS NOT NULL
                ORDER BY 
                    null_miner_count DESC,  -- Prioritize files with more NULL miners (empty assignments first)
                    fa.updated_at ASC       -- Then by oldest first
                LIMIT $1
            """, self.max_reassignments_per_batch)
            
            files_with_nulls = [dict(row) for row in rows]
            
            if files_with_nulls:
                # Log prioritization for debugging
                completely_empty = [f for f in files_with_nulls if f['null_miner_count'] == 5]
                partially_empty = [f for f in files_with_nulls if f['null_miner_count'] < 5]
                
                logger.info(f"🔍 Found {len(files_with_nulls)} files needing reassignment:")
                logger.info(f"   - {len(completely_empty)} completely empty (0/5 miners)")
                logger.info(f"   - {len(partially_empty)} partially empty (1-4/5 miners)")
                
                if completely_empty:
                    logger.info(f"🎯 Prioritizing {len(completely_empty)} completely empty files:")
                    for file_info in completely_empty[:3]:  # Show first 3
                        logger.info(f"     {file_info['cid'][:20]}... (all NULL miners)")
            
            return files_with_nulls
    
    async def get_available_miners(self, current_epoch: int) -> List[Dict[str, Any]]:
        """Get available miners with their capacity and health information."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    r.node_id,
                    r.ipfs_peer_id,
                    r.registered_at,
                    r.owner_account,
                    COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                    COALESCE(nm.ipfs_repo_size, 0) as used_storage_bytes,
                    COALESCE(ms.total_files_pinned, 0) as total_files_pinned,
                    COALESCE(ms.total_files_size_bytes, 0) as total_files_size_bytes,
                    COALESCE(ms.health_score, 100) as health_score,
                    COALESCE(ms.last_online_block, 0) as last_online_block,
                    -- Calculate days since registration
                    EXTRACT(EPOCH FROM (NOW() - TO_TIMESTAMP(r.registered_at))) / 86400 as days_since_registration
                FROM registration r
                LEFT JOIN (
                    -- Get latest node metrics for each miner
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max, ipfs_repo_size
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) nm ON r.node_id = nm.miner_id
                LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                WHERE r.node_type = 'StorageMiner' 
                  AND r.status = 'active'
                  AND COALESCE(ms.health_score, 100) >= $1
                  AND (ms.last_online_block IS NULL OR ms.last_online_block >= $2 - 1000)
                ORDER BY r.node_id
            """, self.min_miner_health_score, current_epoch * 100)
            
            return [dict(row) for row in rows]
    
    def calculate_miner_score(self, miner: Dict[str, Any]) -> float:
        """
        Calculate a score for miner selection based on multiple factors.
        Higher score = better candidate for file assignment.
        """
        # Convert database decimal values to float to avoid decimal/float division errors
        storage_capacity = float(miner['storage_capacity_bytes'])
        used_storage_bytes = float(miner['used_storage_bytes'])
        total_files_size_bytes = float(miner['total_files_size_bytes'])
        total_files_pinned = float(miner['total_files_pinned'])
        health_score_value = float(miner['health_score'])
        
        # Use actual IPFS repo size as primary indicator of usage
        ipfs_repo_size = used_storage_bytes  # from node_metrics.ipfs_repo_size
        calculated_size = total_files_size_bytes  # from our miner_stats
        
        # Use the higher value as a safety measure, but prefer actual IPFS data
        if ipfs_repo_size > 0:
            # IPFS repo size is available and non-zero, use it as primary
            used_storage = ipfs_repo_size
            # But ensure it's at least as much as our calculated size
            if calculated_size > ipfs_repo_size:
                used_storage = calculated_size
        else:
            used_storage = calculated_size
        
        available_storage = max(0, storage_capacity - used_storage)
        
        if storage_capacity <= 0:
            return 0.0
        
        # Storage score (0-1): percentage of available storage
        storage_score = available_storage / storage_capacity
        
        # File count score (0-1): prefer miners with fewer files
        file_count = total_files_pinned
        # Normalize file count (assume 1000 files as "full")
        file_count_normalized = min(1.0, file_count / 1000.0)
        file_score = 1.0 - file_count_normalized
        
        # Health score (0-1)
        health_score = min(1.0, health_score_value / 100.0)
        
        # New miner boost: give preference to recently registered miners
        days_since_registration = float(miner.get('days_since_registration', 365))
        if days_since_registration <= self.new_miner_boost_days:
            # Linear boost from max factor to 1.0 over the boost period
            new_miner_boost = self.new_miner_boost_factor - (
                (self.new_miner_boost_factor - 1.0) * 
                (days_since_registration / self.new_miner_boost_days)
            )
        else:
            new_miner_boost = 1.0
        
        # Load balancing penalty: heavily penalize miners assigned many files in current batch
        miner_id = miner['node_id']
        batch_assignments = self.batch_assignments.get(miner_id, 0)
        
        # Exponential penalty for multiple assignments in same batch
        if batch_assignments > 0:
            load_penalty = self.assignment_penalty_factor ** batch_assignments
        else:
            load_penalty = 1.0
        
        # Hard limit: if miner has reached max assignments, score becomes very low
        if batch_assignments >= self.max_assignments_per_miner:
            load_penalty = 0.01  # Almost eliminate from selection
        
        # Combined score with weights:
        # - Storage availability: 40%
        # - File count balance: 20% 
        # - Health score: 15%
        # - Registration recency: 10%
        # - Load balancing: 15%
        base_score = (
            storage_score * 0.40 +
            file_score * 0.20 +
            health_score * 0.15 +
            min(1.0, days_since_registration / 365) * 0.10 +  # Older miners get slight preference for stability
            (1.0 - min(1.0, batch_assignments / 5.0)) * 0.15  # Prefer miners with fewer current assignments
        )
        
        # Apply new miner boost and load balancing penalty
        final_score = base_score * new_miner_boost * load_penalty
        
        logger.debug(f"Miner {miner['node_id']}: storage={storage_score:.3f}, files={file_score:.3f}, "
                    f"health={health_score:.3f}, days={days_since_registration:.1f}, "
                    f"batch_assignments={batch_assignments}, load_penalty={load_penalty:.3f}, "
                    f"boost={new_miner_boost:.3f}, final={final_score:.3f}")
        
        return final_score
    
    def select_miners_for_file(self, miners: List[Dict[str, Any]], file_size: int, exclude_miners: List[str] = None) -> List[str]:
        """
        Select the best miners for a file assignment using weighted random selection for better distribution.
        
        Args:
            miners: List of available miners with scores
            file_size: Size of the file to assign
            exclude_miners: List of miner IDs to exclude (already assigned to this file)
            
        Returns:
            List of selected miner node_ids
        """
        exclude_miners = exclude_miners or []
        
        # Add 20% safety margin for IPFS overhead, metadata, and growth
        safety_margin = int(file_size * 0.2)
        required_space = file_size + safety_margin
        
        # Filter miners that have enough storage for this file and are not excluded
        suitable_miners = []
        for miner in miners:
            if miner['node_id'] in exclude_miners:
                continue
            
            # Use actual IPFS repo size as primary indicator of usage
            ipfs_repo_size = miner['used_storage_bytes']  # from node_metrics.ipfs_repo_size
            calculated_size = miner['total_files_size_bytes']  # from our miner_stats
            
            # Use the higher value as a safety measure, but prefer actual IPFS data
            if ipfs_repo_size > 0:
                # IPFS repo size is available and non-zero, use it as primary
                used_storage = ipfs_repo_size
                # But ensure it's at least as much as our calculated size
                if calculated_size > ipfs_repo_size:
                    logger.debug(f"Miner {miner['node_id']}: Our calculated size ({calculated_size:,}) > IPFS repo size ({ipfs_repo_size:,}), using calculated")
                    used_storage = calculated_size
            else:
                # IPFS repo size not available, fall back to our calculated size
                used_storage = calculated_size
                logger.debug(f"Miner {miner['node_id']}: No IPFS repo size data, using calculated size ({calculated_size:,})")
            
            storage_capacity = miner['storage_capacity_bytes']
            available_storage = max(0, storage_capacity - used_storage)
            
            if available_storage >= required_space:
                suitable_miners.append(miner)
                logger.debug(f"Miner {miner['node_id']}: ✅ {available_storage:,} available >= {required_space:,} required")
            else:
                logger.debug(f"Miner {miner['node_id']}: ❌ {available_storage:,} available < {required_space:,} required")
        
        if len(suitable_miners) < self.replicas_per_file:
            logger.warning(f"Only {len(suitable_miners)} suitable miners found for file size {file_size:,} bytes (need {required_space:,} with safety margin), "
                          f"need {self.replicas_per_file}")
            # Use what we have
            return [m['node_id'] for m in suitable_miners]
        
        # Calculate scores for all suitable miners
        scored_miners = []
        for miner in suitable_miners:
            score = self.calculate_miner_score(miner)
            if score > 0:  # Only include miners with positive scores
                scored_miners.append({
                    'node_id': miner['node_id'],
                    'score': score,
                    'miner_data': miner
                })
        
        if len(scored_miners) < self.replicas_per_file:
            logger.warning(f"Only {len(scored_miners)} miners with positive scores, need {self.replicas_per_file}")
            return [m['node_id'] for m in scored_miners]
        
        # Use weighted random selection for better distribution
        selected_miners = self._weighted_random_selection(scored_miners, self.replicas_per_file)
        
        return selected_miners
    
    def _weighted_random_selection(self, scored_miners: List[Dict[str, Any]], count: int) -> List[str]:
        """
        Select miners using weighted random selection based on their scores.
        This promotes better distribution across the network.
        """
        import random
        
        if len(scored_miners) <= count:
            return [m['node_id'] for m in scored_miners]
        
        # Create probability distribution based on scores
        total_score = sum(m['score'] for m in scored_miners)
        if total_score <= 0:
            # Fallback to random selection if all scores are zero
            return [m['node_id'] for m in random.sample(scored_miners, count)]
        
        # Normalize scores to probabilities
        probabilities = [m['score'] / total_score for m in scored_miners]
        
        # Select miners without replacement using weighted probabilities
        selected_miners = []
        remaining_miners = scored_miners.copy()
        remaining_probs = probabilities.copy()
        
        for _ in range(count):
            if not remaining_miners:
                break
            
            # Weighted random selection
            selected_idx = random.choices(range(len(remaining_miners)), weights=remaining_probs)[0]
            selected_miner = remaining_miners.pop(selected_idx)
            remaining_probs.pop(selected_idx)
            
            selected_miners.append(selected_miner['node_id'])
            
            # Renormalize remaining probabilities
            if remaining_probs:
                total_remaining = sum(remaining_probs)
                if total_remaining > 0:
                    remaining_probs = [p / total_remaining for p in remaining_probs]
        
        logger.debug(f"Weighted selection chose: {', '.join(selected_miners)}")
        return selected_miners
    
    def select_miners_for_reassignment(self, miners: List[Dict[str, Any]], file_size: int, 
                                     current_miners: List[str], empty_slots: int) -> List[str]:
        """
        Select miners to fill empty slots in an existing file assignment using weighted selection.
        
        Args:
            miners: List of available miners
            file_size: Size of the file
            current_miners: List of currently assigned miners (excluding None)
            empty_slots: Number of empty slots to fill
            
        Returns:
            List of selected miner node_ids for the empty slots
        """
        # Exclude currently assigned miners
        exclude_miners = [m for m in current_miners if m is not None]
        
        # Add 20% safety margin for IPFS overhead, metadata, and growth
        safety_margin = int(file_size * 0.2)
        required_space = file_size + safety_margin
        
        # Filter suitable miners
        suitable_miners = []
        for miner in miners:
            if miner['node_id'] in exclude_miners:
                continue
            
            # Use actual IPFS repo size as primary indicator of usage
            ipfs_repo_size = miner['used_storage_bytes']  # from node_metrics.ipfs_repo_size
            calculated_size = miner['total_files_size_bytes']  # from our miner_stats
            
            # Use the higher value as a safety measure, but prefer actual IPFS data
            if ipfs_repo_size > 0:
                # IPFS repo size is available and non-zero, use it as primary
                used_storage = ipfs_repo_size
                # But ensure it's at least as much as our calculated size
                if calculated_size > ipfs_repo_size:
                    used_storage = calculated_size
            else:
                # IPFS repo size not available, fall back to our calculated size
                used_storage = calculated_size
            
            storage_capacity = miner['storage_capacity_bytes']
            available_storage = max(0, storage_capacity - used_storage)
            
            if available_storage >= required_space:
                suitable_miners.append(miner)
        
        if len(suitable_miners) < empty_slots:
            logger.warning(f"Only {len(suitable_miners)} suitable miners found for reassignment (need {required_space:,} bytes with safety margin), "
                          f"need {empty_slots}")
        
        # Calculate scores and use weighted selection
        scored_miners = []
        for miner in suitable_miners:
            score = self.calculate_miner_score(miner)
            if score > 0:
                scored_miners.append({
                    'node_id': miner['node_id'],
                    'score': score,
                    'miner_data': miner
                })
        
        if len(scored_miners) < empty_slots:
            return [m['node_id'] for m in scored_miners]
        
        # Use weighted random selection for better distribution
        selected_miners = self._weighted_random_selection(scored_miners, empty_slots)
        return selected_miners
    
    def update_miner_usage(self, miners: List[Dict[str, Any]], selected_miner_ids: List[str], file_size: int):
        """Update miner usage statistics after assignment and track batch assignments."""
        for miner in miners:
            if miner['node_id'] in selected_miner_ids:
                # Update usage for scoring future files
                miner['total_files_size_bytes'] += file_size
                miner['total_files_pinned'] += 1
                
                # Track batch assignments for load balancing
                miner_id = miner['node_id']
                self.batch_assignments[miner_id] = self.batch_assignments.get(miner_id, 0) + 1
                
                logger.debug(f"Updated miner {miner['node_id']}: +{file_size} bytes, +1 file, "
                           f"batch_assignments={self.batch_assignments[miner_id]}")
    
    async def queue_assignment_task(self, assignment_data: Dict[str, Any]) -> None:
        """Queue a file assignment task to RabbitMQ."""
        message_body = json.dumps(assignment_data).encode()
        
        await self.rabbitmq_channel.default_exchange.publish(
            Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=self.queue_name
        )
        
        logger.debug(f"Queued assignment for file {assignment_data['cid']}")
    
    async def process_new_file_assignments(self, available_miners: List[Dict[str, Any]], current_epoch: int) -> tuple[int, int]:
        """Process new files from pending_assignment_file table."""
        pending_files = await self.get_pending_files()
        if not pending_files:
            logger.info("No pending files found for assignment")
            return 0, 0

        logger.info(f"Found {len(pending_files)} files pending assignment")
        
        # CRITICAL: Get minimum required miners from environment
        min_required_miners = int(os.getenv('MIN_REQUIRED_MINERS', '5'))
        logger.info(f"🎯 Minimum required miners per file: {min_required_miners}")

        successful_assignments = 0
        failed_assignments = 0

        for file_info in pending_files:
            try:
                cid = file_info['cid']
                file_size = file_info['file_size_bytes'] or 0
                owner = file_info['owner']
                filename = file_info.get('filename', '')

                logger.info(f"Assigning new file {filename} ({cid[:16]}...) - Size: {file_size:,} bytes")

                # Select miners for this file
                selected_miners = self.select_miners_for_file(available_miners, file_size)

                # CRITICAL VALIDATION: Check if we have enough miners
                if len(selected_miners) < min_required_miners:
                    logger.error(f"❌ INSUFFICIENT MINERS for file {cid}: Found {len(selected_miners)}, need {min_required_miners}")
                    logger.error(f"   File: {filename} ({file_size:,} bytes)")
                    logger.error(f"   Available miners: {len(available_miners)}")
                    
                    # Mark as failed in pending_assignment_file
                    try:
                        async with self.db_pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE pending_assignment_file
                                SET status = 'failed', 
                                    error_message = $1,
                                    processed_at = CURRENT_TIMESTAMP
                                WHERE id = $2
                            """, f"Insufficient available miners: {len(selected_miners)}/{min_required_miners} required", file_info['id'])
                    except Exception as e:
                        logger.error(f"Error marking pending file as failed: {e}")
                    
                    failed_assignments += 1
                    continue

                if len(selected_miners) < self.replicas_per_file:
                    logger.warning(f"⚠️ Only assigned {len(selected_miners)} replicas for file {cid}, "
                                 f"target was {self.replicas_per_file} (minimum {min_required_miners} satisfied)")

                # Update miner usage for future assignments in this batch
                self.update_miner_usage(available_miners, selected_miners, file_size)

                # Prepare assignment data
                assignment_data = {
                    'type': 'new_assignment',
                    'cid': cid,
                    'owner': owner,
                    'filename': filename,
                    'file_size_bytes': file_size,
                    'assigned_miners': selected_miners,
                    'epoch': current_epoch,
                    'pending_file_id': file_info['id'],
                    'timestamp': datetime.utcnow().isoformat()
                }

                # Queue for processing
                await self.queue_assignment_task(assignment_data)
                successful_assignments += 1

                logger.info(f"✅ Successfully queued assignment for file {cid[:16]}... to {len(selected_miners)} miners: "
                           f"{', '.join(selected_miners[:3])}{'...' if len(selected_miners) > 3 else ''}")

            except Exception as e:
                logger.error(f"❌ Error processing file {file_info.get('cid', 'unknown')}: {e}")
                failed_assignments += 1
                continue

        logger.info(f"📊 Assignment Summary: {successful_assignments} successful, {failed_assignments} failed (insufficient miners)")
        return successful_assignments, failed_assignments
    
    async def process_reassignments(self, available_miners: List[Dict[str, Any]], current_epoch: int) -> tuple[int, int]:
        """Process files that need reassignment due to empty miner slots."""
        files_needing_reassignment = await self.get_files_needing_reassignment()
        if not files_needing_reassignment:
            logger.info("No files found needing reassignment")
            return 0, 0
        
        logger.info(f"Found {len(files_needing_reassignment)} files needing reassignment")
        
        successful_reassignments = 0
        failed_reassignments = 0
        
        for file_info in files_needing_reassignment:
            try:
                cid = file_info['cid']
                file_size = file_info['file_size_bytes'] or 0
                owner = file_info['owner']
                filename = file_info.get('filename', '')
                
                # Get current miner assignments
                current_miners = [
                    file_info['miner1'], file_info['miner2'], file_info['miner3'],
                    file_info['miner4'], file_info['miner5']
                ]
                
                # Count empty slots
                empty_slots = sum(1 for m in current_miners if m is None)
                assigned_miners = [m for m in current_miners if m is not None]
                
                logger.info(f"Reassigning file {filename} ({cid[:16]}...) - {empty_slots} empty slots, "
                           f"currently assigned to {len(assigned_miners)} miners")
                
                # Select miners for empty slots
                new_miners = self.select_miners_for_reassignment(
                    available_miners, file_size, assigned_miners, empty_slots
                )
                
                if not new_miners:
                    logger.warning(f"No suitable miners found for reassignment of file {cid}")
                    failed_reassignments += 1
                    continue
                
                # Update miner usage for future assignments in this batch
                self.update_miner_usage(available_miners, new_miners, file_size)
                
                # Prepare reassignment data
                assignment_data = {
                    'type': 'reassignment',
                    'cid': cid,
                    'owner': owner,
                    'filename': filename,
                    'file_size_bytes': file_size,
                    'current_miners': current_miners,
                    'new_miners': new_miners,
                    'epoch': current_epoch,
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                # Queue for processing
                await self.queue_assignment_task(assignment_data)
                successful_reassignments += 1
                
                logger.info(f"Successfully queued reassignment for file {cid[:16]}... - "
                           f"adding {len(new_miners)} miners: {', '.join(new_miners)}")
                
            except Exception as e:
                logger.error(f"Error processing reassignment for file {file_info.get('cid', 'unknown')}: {e}")
                failed_reassignments += 1
                continue
        
        return successful_reassignments, failed_reassignments
    
    async def process_failing_miner_replacements(self, available_miners: List[Dict[str, Any]], current_epoch: int) -> tuple[int, int]:
        """
        Process files assigned to miners with failing pin checks and replace failing miners.
        
        Args:
            available_miners: List of available healthy miners
            current_epoch: Current epoch number
            
        Returns:
            Tuple of (successful_replacements, failed_replacements)
        """
        files_with_failing_miners = await self.get_files_with_failing_miners(current_epoch)
        if not files_with_failing_miners:
            logger.info("No files found with failing miners")
            return 0, 0
        
        logger.info(f"Found {len(files_with_failing_miners)} files with failing miners")
        
        successful_replacements = 0
        failed_replacements = 0
        
        for file_info in files_with_failing_miners:
            try:
                cid = file_info['cid']
                file_size = file_info['file_size_bytes'] or 0
                owner = file_info['owner']
                filename = file_info.get('filename', '')
                failing_miners = file_info['failing_miners']
                failing_count = file_info['failing_miner_count']
                
                # Get current miner assignments
                current_miners = [
                    file_info['miner1'], file_info['miner2'], file_info['miner3'],
                    file_info['miner4'], file_info['miner5']
                ]
                
                # Get healthy miners (exclude failing ones)
                healthy_miners = [m for m in current_miners if m is not None and m not in failing_miners]
                
                logger.info(f"Replacing failing miners for file {filename} ({cid[:16]}...) - "
                           f"{failing_count} failing miners: {', '.join(failing_miners)}")
                
                # Select replacement miners (exclude current healthy miners and failing miners)
                exclude_miners = healthy_miners + failing_miners
                replacement_miners = self.select_miners_for_reassignment(
                    available_miners, file_size, exclude_miners, failing_count
                )
                
                if not replacement_miners:
                    logger.warning(f"No suitable replacement miners found for file {cid}")
                    failed_replacements += 1
                    continue
                
                if len(replacement_miners) < failing_count:
                    logger.warning(f"Only found {len(replacement_miners)} replacement miners for {failing_count} failing miners")
                
                # Update miner usage for future assignments in this batch
                self.update_miner_usage(available_miners, replacement_miners, file_size)
                
                # Create new miner assignment by replacing failing miners
                new_miners = current_miners.copy()
                replacement_idx = 0
                
                for i, miner in enumerate(new_miners):
                    if miner in failing_miners and replacement_idx < len(replacement_miners):
                        new_miners[i] = replacement_miners[replacement_idx]
                        replacement_idx += 1
                
                # Prepare replacement data
                assignment_data = {
                    'type': 'failing_miner_replacement',
                    'cid': cid,
                    'owner': owner,
                    'filename': filename,
                    'file_size_bytes': file_size,
                    'old_miners': current_miners,
                    'new_miners': new_miners,
                    'failing_miners': failing_miners,
                    'replacement_miners': replacement_miners,
                    'epoch': current_epoch,
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                # Queue for processing
                await self.queue_assignment_task(assignment_data)
                successful_replacements += 1
                
                logger.info(f"Successfully queued replacement for file {cid[:16]}... - "
                           f"replacing {len(failing_miners)} failing miners with: {', '.join(replacement_miners)}")
                
            except Exception as e:
                logger.error(f"Error processing failing miner replacement for file {file_info.get('cid', 'unknown')}: {e}")
                failed_replacements += 1
                continue
        
        return successful_replacements, failed_replacements
    
    async def process_file_assignments(self) -> None:
        """Main processing function to assign files to miners."""
        try:
            # Reset batch assignment tracking
            self.batch_assignments = {}
            logger.info("Reset batch assignment tracking for new processing cycle")
            
            # Get current epoch
            current_epoch = self.get_current_epoch()
            
            # Get available miners
            available_miners = await self.get_available_miners(current_epoch)
            if not available_miners:
                logger.error("No available miners found for assignment")
                return
            
            logger.info(f"Found {len(available_miners)} available miners")
            
            # PRIORITY 1: Fix files with NULL miners in file_assignments table (ALWAYS FIRST)
            logger.info("🎯 PRIORITY 1: Scanning file_assignments table for NULL miners...")
            reassign_successful, reassign_failed = await self.process_reassignments(available_miners, current_epoch)
            
            # PRIORITY 2: Process new file assignments from pending table
            logger.info("📋 PRIORITY 2: Processing new files from pending_assignment_file...")
            new_successful, new_failed = await self.process_new_file_assignments(available_miners, current_epoch)
            
            # PRIORITY 3: Process failing miner replacements
            logger.info("🔄 PRIORITY 3: Processing failing miner replacements...")
            replace_successful, replace_failed = await self.process_failing_miner_replacements(available_miners, current_epoch)
            
            # Summary
            total_successful = new_successful + reassign_successful + replace_successful
            total_failed = new_failed + reassign_failed + replace_failed
            
            logger.info(f"📊 File assignment processing complete:")
            logger.info(f"  🎯 NULL miner fixes: {reassign_successful} successful, {reassign_failed} failed")
            logger.info(f"  📋 New assignments: {new_successful} successful, {new_failed} failed")
            logger.info(f"  🔄 Failing replacements: {replace_successful} successful, {replace_failed} failed")
            logger.info(f"  📈 Total: {total_successful} successful, {total_failed} failed")
            
            # CRITICAL: Check for remaining NULL assignments
            async with self.db_pool.acquire() as conn:
                remaining_nulls = await conn.fetchval("""
                    SELECT COUNT(*) FROM file_assignments 
                    WHERE miner1 IS NULL OR miner2 IS NULL OR miner3 IS NULL 
                      OR miner4 IS NULL OR miner5 IS NULL
                """)
                
                if remaining_nulls == 0:
                    logger.info("🎉 SUCCESS: All files now have complete miner assignments!")
                else:
                    logger.warning(f"⚠️ {remaining_nulls} files still have NULL miners - may need larger batch size or more miners")
            
            # Log distribution statistics
            self._log_distribution_stats()
            
        except Exception as e:
            logger.error(f"Error in file assignment processing: {e}")
            raise
    
    def _log_distribution_stats(self):
        """Log statistics about how files were distributed across miners."""
        if not self.batch_assignments:
            logger.info("No assignments made in this batch")
            return
        
        total_assignments = sum(self.batch_assignments.values())
        unique_miners = len(self.batch_assignments)
        avg_assignments = total_assignments / unique_miners if unique_miners > 0 else 0
        
        # Sort miners by assignment count
        sorted_assignments = sorted(self.batch_assignments.items(), key=lambda x: x[1], reverse=True)
        
        logger.info(f"📊 Distribution Statistics:")
        logger.info(f"  Total assignments: {total_assignments}")
        logger.info(f"  Unique miners used: {unique_miners}")
        logger.info(f"  Average assignments per miner: {avg_assignments:.1f}")
        
        # Show top 10 most assigned miners
        logger.info(f"  Top assigned miners:")
        for i, (miner_id, count) in enumerate(sorted_assignments[:10]):
            logger.info(f"    {i+1}. {miner_id}: {count} assignments")
        
        # Show distribution spread
        assignment_counts = list(self.batch_assignments.values())
        min_assignments = min(assignment_counts)
        max_assignments = max(assignment_counts)
        logger.info(f"  Assignment range: {min_assignments} - {max_assignments} per miner")
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.substrate:
            self.substrate.close()
            logger.info("Closed substrate connection")

    async def get_files_with_failing_miners(self, current_epoch: int) -> List[Dict[str, Any]]:
        """
        Get files assigned to miners with failing pin checks that need reassignment.
        
        Args:
            current_epoch: Current epoch number
            
        Returns:
            List of files with failing miners that need reassignment
        """
        async with self.db_pool.acquire() as conn:
            # Get files assigned to miners with poor pin check performance
            # We'll consider miners failing if:
            # 1. Their recent pin check success rate is below threshold (e.g., 50%)
            # 2. They have recent pin check failures in current/recent epochs
            # 3. Their overall health score is below minimum threshold
            
            # Calculate the epoch window start in Python to avoid SQL type ambiguity
            epoch_window_start = max(0, current_epoch - self.recent_epochs_window)
            
            rows = await conn.fetch("""
                WITH failing_miners AS (
                    -- Get miners with poor pin check performance in recent epochs
                    SELECT DISTINCT meh.node_id
                    FROM miner_epoch_health meh
                    WHERE meh.epoch >= $1  -- Recent epochs (calculated in Python)
                    AND (meh.pin_check_successes + meh.pin_check_failures) > 0  -- Has pin check activity
                    AND (
                        -- Poor recent pin check success rate
                        (meh.pin_check_successes * 100.0 / (meh.pin_check_successes + meh.pin_check_failures)) < $2
                        OR
                        -- Has recent failures and low success rate
                        (meh.pin_check_failures > 0 AND 
                         meh.pin_check_successes * 100.0 / (meh.pin_check_successes + meh.pin_check_failures) < 70)
                    )
                    
                    UNION
                    
                    -- Also include miners with overall poor health scores
                    SELECT ms.node_id
                    FROM miner_stats ms
                    WHERE ms.health_score < $3
                    AND ms.total_pin_checks > 5  -- Only consider miners with some history
                ),
                files_with_failing_miners AS (
                    SELECT 
                        fa.cid,
                        fa.owner,
                        f.name as filename,
                        f.size as file_size_bytes,
                        fa.miner1,
                        fa.miner2,
                        fa.miner3,
                        fa.miner4,
                        fa.miner5,
                        fa.updated_at,
                        -- Count how many assigned miners are failing
                        (CASE WHEN fa.miner1 IN (SELECT node_id FROM failing_miners) THEN 1 ELSE 0 END +
                         CASE WHEN fa.miner2 IN (SELECT node_id FROM failing_miners) THEN 1 ELSE 0 END +
                         CASE WHEN fa.miner3 IN (SELECT node_id FROM failing_miners) THEN 1 ELSE 0 END +
                         CASE WHEN fa.miner4 IN (SELECT node_id FROM failing_miners) THEN 1 ELSE 0 END +
                         CASE WHEN fa.miner5 IN (SELECT node_id FROM failing_miners) THEN 1 ELSE 0 END) as failing_miner_count,
                        -- List the failing miners
                        ARRAY_REMOVE(ARRAY[
                            CASE WHEN fa.miner1 IN (SELECT node_id FROM failing_miners) THEN fa.miner1 END,
                            CASE WHEN fa.miner2 IN (SELECT node_id FROM failing_miners) THEN fa.miner2 END,
                            CASE WHEN fa.miner3 IN (SELECT node_id FROM failing_miners) THEN fa.miner3 END,
                            CASE WHEN fa.miner4 IN (SELECT node_id FROM failing_miners) THEN fa.miner4 END,
                            CASE WHEN fa.miner5 IN (SELECT node_id FROM failing_miners) THEN fa.miner5 END
                        ], NULL) as failing_miners
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE (
                        fa.miner1 IN (SELECT node_id FROM failing_miners) OR
                        fa.miner2 IN (SELECT node_id FROM failing_miners) OR
                        fa.miner3 IN (SELECT node_id FROM failing_miners) OR
                        fa.miner4 IN (SELECT node_id FROM failing_miners) OR
                        fa.miner5 IN (SELECT node_id FROM failing_miners)
                    )
                )
                SELECT *
                FROM files_with_failing_miners
                WHERE failing_miner_count > 0
                ORDER BY failing_miner_count DESC, updated_at ASC  -- Prioritize files with more failing miners
                LIMIT $4
            """, 
            epoch_window_start,
            self.pin_check_failure_threshold, 
            self.min_miner_health_score,
            self.max_failing_replacements_per_batch)
            
            return [dict(row) for row in rows]


async def main():
    """Main entry point."""
    processor = FileAssignmentProcessor()
    
    try:
        # Initialize database pool
        await init_db_pool()
        processor.db_pool = get_db_pool()
        logger.info("Database connection pool initialized")
        
        # Connect to services
        processor.connect_substrate()
        await processor.connect_rabbitmq()
        
        # Process file assignments
        await processor.process_file_assignments()
        
        logger.info("Completed file assignment processing")
        
    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 