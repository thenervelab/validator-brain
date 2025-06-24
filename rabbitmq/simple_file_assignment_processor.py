#!/usr/bin/env python3
"""
Simple File Assignment Processor

A simplified, reliable file assignment processor that:
1. Uses simple criteria: 1+ day old miners with capacity
2. Ensures broad network distribution 
3. Assigns per file (not complex batch scoring)
4. Avoids the complex scoring that causes failures
5. Integrates directly with the epoch orchestrator

This replaces the complex file_assignment_processor.py with reliable logic.
"""

import asyncio
import json
import logging
import os
import sys
import random
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


class SimpleFileAssignmentProcessor:
    def __init__(self):
        self.db_pool = None
        self.substrate = None
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        
        # Simple configuration
        self.replicas_per_file = int(os.getenv('REPLICAS_PER_FILE', 5))
        self.min_miner_age_days = 1  # Only use miners 1+ days old
        self.min_health_score = 50   # Lower threshold for reliability
        self.queue_name = 'file_assignment_processing'
        
        # Track assignments per miner in this session for distribution
        self.session_assignments = {}
        
    async def initialize(self):
        """Initialize database and RabbitMQ connections."""
        try:
            # Initialize database - always call init_db_pool first when running standalone
            try:
                self.db_pool = get_db_pool()
                if self.db_pool:
                    logger.info("✅ Using existing database pool")
                else:
                    raise Exception("No existing pool")
            except:
                # No existing pool, initialize new one
                logger.info("🔧 Initializing new database pool...")
                await init_db_pool()
                self.db_pool = get_db_pool()
                
                if not self.db_pool:
                    raise Exception("Failed to initialize database pool")
                
                logger.info("✅ Database pool initialized successfully")
            
            # Initialize Substrate connection
            node_url = NODE_URL or 'wss://rpc.hippius.network'
            self.substrate = SubstrateInterface(url=node_url)
            logger.info(f"✅ Connected to Substrate at {node_url}")
            
            # Initialize RabbitMQ
            rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://localhost')
            self.rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Declare queue
            await self.rabbitmq_channel.declare_queue(
                self.queue_name, 
                durable=True
            )
            logger.info(f"✅ RabbitMQ connected and queue '{self.queue_name}' declared")
            
            logger.info("✅ Simple file assignment processor initialized")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize processor: {e}")
            return False
    
    def get_current_epoch(self) -> int:
        """Get current epoch from blockchain."""
        try:
            if self.substrate:
                current_block = self.substrate.get_block_number()
                return calculate_epoch_from_block(current_block)
            return 0
        except Exception as e:
            logger.error(f"Error getting current epoch: {e}")
            return 0
    
    async def get_reliable_miners(self) -> List[Dict[str, Any]]:
        """Get reliable miners that are 1+ days old with capacity."""
        try:
            async with self.db_pool.acquire() as conn:
                cutoff_date = datetime.now() - timedelta(days=self.min_miner_age_days)
                
                miners = await conn.fetch("""
                    SELECT 
                        r.node_id,
                        r.ipfs_peer_id,
                        r.registered_at,
                        COALESCE(nm.ipfs_storage_max, 1000000000) as storage_max,
                        COALESCE(nm.ipfs_repo_size, 0) as storage_used,
                        COALESCE(ms.health_score, 100) as health_score,
                        COALESCE(ms.total_files_pinned, 0) as files_pinned
                    FROM registration r
                    LEFT JOIN (
                        SELECT DISTINCT ON (miner_id) 
                            miner_id, ipfs_storage_max, ipfs_repo_size
                        FROM node_metrics 
                        ORDER BY miner_id, block_number DESC
                    ) nm ON r.node_id = nm.miner_id
                    LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                    WHERE r.node_type = 'StorageMiner' 
                      AND r.status = 'active'
                      AND r.registered_at <= $1
                      AND COALESCE(ms.health_score, 100) >= $2
                    ORDER BY RANDOM()  -- Random order for better distribution
                """, cutoff_date, self.min_health_score)
                
                # Filter for capacity (simple check - just need some space)
                reliable_miners = []
                for miner in miners:
                    available_space = miner['storage_max'] - miner['storage_used']
                    # Require at least 10MB available space to be considered for assignments
                    # This prevents assigning to miners that are nearly full
                    if available_space > 10_000_000:  # At least 10MB available
                        reliable_miners.append({
                            'node_id': miner['node_id'],
                            'ipfs_peer_id': miner['ipfs_peer_id'],
                            'health_score': miner['health_score'],
                            'files_pinned': miner['files_pinned'],
                            'available_space': available_space,
                            'age_days': (datetime.now() - miner['registered_at']).days
                        })
                
                logger.info(f"✅ Found {len(reliable_miners)} reliable miners (1+ day old with capacity)")
                return reliable_miners
                
        except Exception as e:
            logger.error(f"❌ Error getting reliable miners: {e}")
            return []
    
    def select_miners_simple(self, miners: List[Dict[str, Any]], count: int = 5, exclude: List[str] = None, file_size: int = 0) -> List[str]:
        """Simple miner selection with network distribution and proper capacity checking."""
        exclude = exclude or []
        
        # Filter miners that have enough capacity for this specific file
        # Add 20% safety margin for IPFS overhead, metadata, and growth
        safety_margin = int(file_size * 0.2) if file_size > 0 else 0
        required_space = file_size + safety_margin
        
        suitable_miners = []
        for miner in miners:
            if miner['node_id'] in exclude:
                continue
                
            # Check if this miner has enough space for this specific file
            available_space = miner['available_space']
            if available_space >= required_space:
                suitable_miners.append(miner)
            else:
                logger.debug(f"Skipping miner {miner['node_id']}: needs {required_space:,} bytes, "
                           f"has {available_space:,} bytes available")
        
        if len(suitable_miners) < count:
            logger.warning(f"⚠️ Only {len(suitable_miners)} miners have enough space for file "
                          f"({required_space:,} bytes needed), requested {count}")
        
        if len(suitable_miners) <= count:
            return [m['node_id'] for m in suitable_miners]
        
        # Simple distribution strategy:
        # 1. Sort by current session assignments (fewer = better)
        # 2. Then by total files pinned (fewer = better)
        # 3. Add randomness for tie-breaking
        
        # Update session assignment counts
        for miner in suitable_miners:
            node_id = miner['node_id']
            miner['session_assignments'] = self.session_assignments.get(node_id, 0)
        
        # Sort by multiple criteria for fair distribution
        suitable_miners.sort(key=lambda m: (
            m['session_assignments'],    # Fewer assignments this session
            m['files_pinned'],          # Fewer total files
            random.random()             # Random factor for tie-breaking
        ))
        
        # Select the best distributed miners
        selected = suitable_miners[:count]
        selected_ids = [m['node_id'] for m in selected]
        
        # Update session assignment counts
        for node_id in selected_ids:
            self.session_assignments[node_id] = self.session_assignments.get(node_id, 0) + 1
        
        return selected_ids
    
    async def get_pending_files(self) -> List[Dict[str, Any]]:
        """Get files from pending_assignment_file table that need assignment."""
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT 
                        paf.id,
                        paf.cid,
                        paf.owner,
                        paf.filename,
                        paf.file_size_bytes,
                        paf.created_at
                    FROM pending_assignment_file paf
                    WHERE paf.status = 'pending'
                    ORDER BY paf.file_size_bytes ASC  -- Process smaller files first
                    LIMIT 100  -- Process in manageable batches
                """)
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"❌ Error getting pending files: {e}")
            return []
    
    async def get_files_needing_reassignment(self) -> List[Dict[str, Any]]:
        """Get files with empty miner slots that need reassignment."""
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT 
                        fa.cid,
                        fa.owner,
                        fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5,
                        f.name as filename,
                        f.size as file_size_bytes
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL 
                           OR fa.miner4 IS NULL OR fa.miner5 IS NULL)
                    ORDER BY f.size ASC
                    LIMIT 100  -- Process in manageable batches
                """)
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"❌ Error getting files needing reassignment: {e}")
            return []
    
    async def queue_assignment_task(self, assignment_data: Dict[str, Any]) -> None:
        """Queue a file assignment task to RabbitMQ."""
        try:
            message_body = json.dumps(assignment_data).encode()
            
            await self.rabbitmq_channel.default_exchange.publish(
                Message(
                    body=message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=self.queue_name
            )
            
            logger.debug(f"Queued assignment for file {assignment_data['cid'][:16]}...")
            
        except Exception as e:
            logger.error(f"❌ Error queuing assignment task: {e}")
    
    async def process_new_file_assignments(self, available_miners: List[Dict[str, Any]], current_epoch: int) -> tuple[int, int]:
        """Process new files from pending_assignment_file table."""
        pending_files = await self.get_pending_files()
        if not pending_files:
            logger.info("No pending files found for assignment")
            return 0, 0
        
        logger.info(f"📁 Processing {len(pending_files)} pending files for assignment")
        
        successful_assignments = 0
        failed_assignments = 0
        
        for file_info in pending_files:
            try:
                cid = file_info['cid']
                file_size = file_info['file_size_bytes'] or 0
                owner = file_info['owner']
                filename = file_info.get('filename', '')
                
                logger.info(f"Assigning new file {filename} ({cid[:16]}...) - Size: {file_size:,} bytes")
                
                # Select miners for this file using simple distribution
                selected_miners = self.select_miners_simple(available_miners, self.replicas_per_file, file_size=file_size)
                
                if not selected_miners:
                    logger.error(f"❌ No suitable miners found for file {cid}")
                    failed_assignments += 1
                    continue
                
                if len(selected_miners) < self.replicas_per_file:
                    logger.warning(f"⚠️ Only assigned {len(selected_miners)} replicas for file {cid}, "
                                   f"target was {self.replicas_per_file}")
                
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
                
                logger.info(f"✅ Assigned file {cid[:16]}... to {len(selected_miners)} miners: "
                           f"{', '.join(selected_miners[:3])}{'...' if len(selected_miners) > 3 else ''}")
                
            except Exception as e:
                logger.error(f"❌ Error processing file {file_info.get('cid', 'unknown')}: {e}")
                failed_assignments += 1
                continue
        
        return successful_assignments, failed_assignments
    
    async def process_reassignments(self, available_miners: List[Dict[str, Any]], current_epoch: int) -> tuple[int, int]:
        """Process files that need reassignment due to empty miner slots."""
        files_needing_reassignment = await self.get_files_needing_reassignment()
        if not files_needing_reassignment:
            logger.info("No files found needing reassignment")
            return 0, 0
        
        logger.info(f"🔄 Processing {len(files_needing_reassignment)} files needing reassignment")
        
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
                
                # Count empty slots and get assigned miners
                assigned_miners = [m for m in current_miners if m is not None]
                empty_slots = self.replicas_per_file - len(assigned_miners)
                
                if empty_slots <= 0:
                    continue  # No empty slots
                
                logger.info(f"Reassigning file {filename} ({cid[:16]}...) - {empty_slots} empty slots")
                
                # Select miners for empty slots (exclude currently assigned miners)
                new_miners = self.select_miners_simple(
                    available_miners, empty_slots, exclude=assigned_miners, file_size=file_size
                )
                
                if not new_miners:
                    logger.warning(f"⚠️ No suitable miners found for reassignment of file {cid}")
                    failed_reassignments += 1
                    continue
                
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
                
                logger.info(f"✅ Queued reassignment for file {cid[:16]}... - "
                           f"adding {len(new_miners)} miners: {', '.join(new_miners)}")
                
            except Exception as e:
                logger.error(f"❌ Error processing reassignment for file {file_info.get('cid', 'unknown')}: {e}")
                failed_reassignments += 1
                continue
        
        return successful_reassignments, failed_reassignments
    
    async def process_file_assignments(self) -> None:
        """Main processing function to assign files to miners."""
        try:
            # Reset session assignment tracking
            self.session_assignments = {}
            logger.info("🔧 Starting simple file assignment processing")
            
            # Get current epoch
            current_epoch = self.get_current_epoch()
            logger.info(f"Processing assignments for epoch {current_epoch}")
            
            # Get available miners
            available_miners = await self.get_reliable_miners()
            if not available_miners:
                logger.error("❌ No available miners found for assignment")
                return
            
            logger.info(f"✅ Found {len(available_miners)} reliable miners")
            
            # Process new file assignments
            new_successful, new_failed = await self.process_new_file_assignments(available_miners, current_epoch)
            
            # Process reassignments for files with empty slots
            reassign_successful, reassign_failed = await self.process_reassignments(available_miners, current_epoch)
            
            # Summary
            total_successful = new_successful + reassign_successful
            total_failed = new_failed + reassign_failed
            
            logger.info(f"📊 Simple file assignment processing complete:")
            logger.info(f"  New assignments: {new_successful} successful, {new_failed} failed")
            logger.info(f"  Reassignments: {reassign_successful} successful, {reassign_failed} failed")
            logger.info(f"  Total: {total_successful} successful, {total_failed} failed")
            
            # Log distribution statistics
            if self.session_assignments:
                logger.info(f"📈 Assignment distribution this session:")
                sorted_assignments = sorted(self.session_assignments.items(), key=lambda x: x[1], reverse=True)
                for i, (miner_id, count) in enumerate(sorted_assignments[:10]):
                    logger.info(f"  {i+1}. {miner_id}: {count} assignments")
                
                total_assignments = sum(self.session_assignments.values())
                avg_assignments = total_assignments / len(self.session_assignments)
                logger.info(f"  Average assignments per miner: {avg_assignments:.1f}")
            
        except Exception as e:
            logger.error(f"❌ Error during file assignment processing: {e}")
            logger.exception("Full traceback:")
    
    async def cleanup(self):
        """Clean up resources."""
        try:
            if hasattr(self, 'rabbitmq_connection') and self.rabbitmq_connection and not self.rabbitmq_connection.is_closed:
                await self.rabbitmq_connection.close()
                logger.info("✅ RabbitMQ connection closed")
        except Exception as e:
            logger.warning(f"⚠️ Error closing RabbitMQ connection: {e}")
        
        try:
            if hasattr(self, 'substrate') and self.substrate:
                self.substrate.close()
                logger.info("✅ Substrate connection closed")
        except Exception as e:
            logger.warning(f"⚠️ Error closing Substrate connection: {e}")
        
        try:
            # Only close database pool if we initialized it ourselves
            if hasattr(self, 'db_pool') and self.db_pool:
                # Don't close the pool if it was shared - let the main process handle it
                logger.info("✅ Database pool cleanup handled by main process")
        except Exception as e:
            logger.warning(f"⚠️ Error during database cleanup: {e}")
        
        logger.info("✅ Simple file assignment processor cleanup complete")


async def main():
    """Main entry point for the processor."""
    # Ensure environment variables are loaded
    load_dotenv()
    
    # Add additional logging for debugging
    logger.info(f"🔧 Starting Simple File Assignment Processor")
    logger.info(f"   Working directory: {os.getcwd()}")
    logger.info(f"   Python path: {sys.path[0]}")
    
    processor = SimpleFileAssignmentProcessor()
    
    try:
        # Initialize
        logger.info("🚀 Initializing processor...")
        success = await processor.initialize()
        if not success:
            logger.error("❌ Failed to initialize processor")
            return 1
        
        # Process file assignments
        logger.info("📋 Starting file assignment processing...")
        await processor.process_file_assignments()
        
        logger.info("✅ Simple file assignment processing completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Fatal error in simple file assignment processor: {e}")
        logger.exception("Full traceback:")
        return 1
    finally:
        try:
            await processor.cleanup()
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 