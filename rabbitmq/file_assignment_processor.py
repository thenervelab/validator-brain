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
        self.min_miner_health_score = float(os.getenv('MIN_MINER_HEALTH_SCORE', '70.0'))
        self.new_miner_boost_days = int(os.getenv('NEW_MINER_BOOST_DAYS', '30'))
        self.new_miner_boost_factor = float(os.getenv('NEW_MINER_BOOST_FACTOR', '1.5'))
        
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
            epoch = block_number // 100  # Assuming 100 blocks per epoch
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
        """Get files that have empty miner slots and need reassignment."""
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
                    fa.updated_at
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL OR 
                       fa.miner4 IS NULL OR fa.miner5 IS NULL)
                ORDER BY fa.updated_at ASC
                LIMIT $1
            """, self.max_reassignments_per_batch)
            
            return [dict(row) for row in rows]
    
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
        Calculate a score for miner selection based on capacity, health, and registration date.
        
        Higher score = better candidate for assignment
        """
        # Basic capacity scoring
        storage_capacity = miner['storage_capacity_bytes']
        used_storage = max(miner['used_storage_bytes'], miner['total_files_size_bytes'])
        available_storage = max(0, storage_capacity - used_storage)
        
        if storage_capacity <= 0:
            return 0.0
        
        # Storage score (0-1): percentage of available storage
        storage_score = available_storage / storage_capacity
        
        # File count score (0-1): prefer miners with fewer files
        file_count = miner['total_files_pinned']
        # Normalize file count (assume 1000 files as "full")
        file_count_normalized = min(1.0, file_count / 1000.0)
        file_score = 1.0 - file_count_normalized
        
        # Health score (0-1)
        health_score = min(1.0, miner['health_score'] / 100.0)
        
        # New miner boost: give preference to recently registered miners
        days_since_registration = miner.get('days_since_registration', 365)
        if days_since_registration <= self.new_miner_boost_days:
            # Linear boost from max factor to 1.0 over the boost period
            new_miner_boost = self.new_miner_boost_factor - (
                (self.new_miner_boost_factor - 1.0) * 
                (days_since_registration / self.new_miner_boost_days)
            )
        else:
            new_miner_boost = 1.0
        
        # Combined score with weights:
        # - Storage availability: 50%
        # - File count balance: 25% 
        # - Health score: 15%
        # - Registration recency: 10%
        base_score = (
            storage_score * 0.50 +
            file_score * 0.25 +
            health_score * 0.15 +
            min(1.0, days_since_registration / 365) * 0.10  # Older miners get slight preference for stability
        )
        
        # Apply new miner boost
        final_score = base_score * new_miner_boost
        
        logger.debug(f"Miner {miner['node_id']}: storage={storage_score:.3f}, files={file_score:.3f}, "
                    f"health={health_score:.3f}, days={days_since_registration:.1f}, "
                    f"boost={new_miner_boost:.3f}, final={final_score:.3f}")
        
        return final_score
    
    def select_miners_for_file(self, miners: List[Dict[str, Any]], file_size: int, exclude_miners: List[str] = None) -> List[str]:
        """
        Select the best miners for a file assignment.
        
        Args:
            miners: List of available miners with scores
            file_size: Size of the file to assign
            exclude_miners: List of miner IDs to exclude (already assigned to this file)
            
        Returns:
            List of selected miner node_ids
        """
        exclude_miners = exclude_miners or []
        
        # Filter miners that have enough storage for this file and are not excluded
        suitable_miners = []
        for miner in miners:
            if miner['node_id'] in exclude_miners:
                continue
                
            available_storage = max(0, 
                miner['storage_capacity_bytes'] - 
                max(miner['used_storage_bytes'], miner['total_files_size_bytes'])
            )
            if available_storage >= file_size:
                suitable_miners.append(miner)
        
        if len(suitable_miners) < self.replicas_per_file:
            logger.warning(f"Only {len(suitable_miners)} suitable miners found for file size {file_size}, "
                          f"need {self.replicas_per_file}")
            # Use what we have
            return [m['node_id'] for m in suitable_miners]
        
        # Calculate scores for all suitable miners
        scored_miners = []
        for miner in suitable_miners:
            score = self.calculate_miner_score(miner)
            scored_miners.append({
                'node_id': miner['node_id'],
                'score': score,
                'miner_data': miner
            })
        
        # Sort by score (highest first)
        scored_miners.sort(key=lambda x: x['score'], reverse=True)
        
        # Select top miners with some randomization to distribute load
        # Take top 15 candidates and randomly select required number
        top_candidates = scored_miners[:min(15, len(scored_miners))]
        
        import random
        selected_miners = random.sample(top_candidates, min(self.replicas_per_file, len(top_candidates)))
        
        return [m['node_id'] for m in selected_miners]
    
    def select_miners_for_reassignment(self, miners: List[Dict[str, Any]], file_size: int, 
                                     current_miners: List[str], empty_slots: int) -> List[str]:
        """
        Select miners to fill empty slots in an existing file assignment.
        
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
        
        # Filter suitable miners
        suitable_miners = []
        for miner in miners:
            if miner['node_id'] in exclude_miners:
                continue
                
            available_storage = max(0, 
                miner['storage_capacity_bytes'] - 
                max(miner['used_storage_bytes'], miner['total_files_size_bytes'])
            )
            if available_storage >= file_size:
                suitable_miners.append(miner)
        
        if len(suitable_miners) < empty_slots:
            logger.warning(f"Only {len(suitable_miners)} suitable miners found for reassignment, "
                          f"need {empty_slots}")
        
        # Calculate scores and select best miners
        scored_miners = []
        for miner in suitable_miners:
            score = self.calculate_miner_score(miner)
            scored_miners.append({
                'node_id': miner['node_id'],
                'score': score,
                'miner_data': miner
            })
        
        # Sort by score (highest first) and take the best ones
        scored_miners.sort(key=lambda x: x['score'], reverse=True)
        selected_count = min(empty_slots, len(scored_miners))
        
        return [scored_miners[i]['node_id'] for i in range(selected_count)]
    
    def update_miner_usage(self, miners: List[Dict[str, Any]], selected_miner_ids: List[str], file_size: int):
        """Update miner usage statistics after assignment."""
        for miner in miners:
            if miner['node_id'] in selected_miner_ids:
                # Update usage for scoring future files
                miner['total_files_size_bytes'] += file_size
                miner['total_files_pinned'] += 1
                logger.debug(f"Updated miner {miner['node_id']}: +{file_size} bytes, +1 file")
    
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
                
                if not selected_miners:
                    logger.error(f"No suitable miners found for file {cid}")
                    failed_assignments += 1
                    continue
                
                if len(selected_miners) < self.replicas_per_file:
                    logger.warning(f"Only assigned {len(selected_miners)} replicas for file {cid}, "
                                 f"target was {self.replicas_per_file}")
                
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
                
                logger.info(f"Successfully assigned file {cid[:16]}... to {len(selected_miners)} miners: "
                           f"{', '.join(selected_miners[:3])}{'...' if len(selected_miners) > 3 else ''}")
                
            except Exception as e:
                logger.error(f"Error processing file {file_info.get('cid', 'unknown')}: {e}")
                failed_assignments += 1
                continue
        
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
    
    async def process_file_assignments(self) -> None:
        """Main processing function to assign files to miners."""
        try:
            # Get current epoch
            current_epoch = self.get_current_epoch()
            
            # Get available miners
            available_miners = await self.get_available_miners(current_epoch)
            if not available_miners:
                logger.error("No available miners found for assignment")
                return
            
            logger.info(f"Found {len(available_miners)} available miners")
            
            # Process new file assignments
            new_successful, new_failed = await self.process_new_file_assignments(available_miners, current_epoch)
            
            # Process reassignments for files with empty slots
            reassign_successful, reassign_failed = await self.process_reassignments(available_miners, current_epoch)
            
            # Summary
            total_successful = new_successful + reassign_successful
            total_failed = new_failed + reassign_failed
            
            logger.info(f"File assignment processing complete:")
            logger.info(f"  New assignments: {new_successful} successful, {new_failed} failed")
            logger.info(f"  Reassignments: {reassign_successful} successful, {reassign_failed} failed")
            logger.info(f"  Total: {total_successful} successful, {total_failed} failed")
            
        except Exception as e:
            logger.error(f"Error in file assignment processing: {e}")
            raise
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.substrate:
            self.substrate.close()
            logger.info("Closed substrate connection")


async def main():
    """Main entry point."""
    processor = FileAssignmentProcessor()
    
    try:
        # Initialize database pool
        await init_db_pool()
        processor.db_pool = await get_db_pool()
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