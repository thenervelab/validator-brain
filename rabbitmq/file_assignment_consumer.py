#!/usr/bin/env python3
"""
File Assignment Consumer

This consumer processes file assignment tasks from the queue and updates the database
with the assignments.

The consumer:
1. Reads assignment messages from the file_assignment_processing queue
2. Handles both new assignments and reassignments
3. Updates the file_assignments table with miner assignments (with race condition protection)
4. Updates the files table with file metadata
5. Marks pending_assignment_file records as assigned
6. Handles assignment failures gracefully
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from aio_pika import IncomingMessage
from dotenv import load_dotenv

from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FileAssignmentConsumer:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'file_assignment_processing'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ."""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Set prefetch count to process one message at a time
            await self.rabbitmq_channel.set_qos(prefetch_count=1)
            
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def process_new_assignment(self, assignment_data: Dict[str, Any]) -> bool:
        """
        Process a new file assignment.
        
        CRITICAL FIX: Reject assignments with insufficient miners and don't save NULL assignments.
        Only save complete assignments with at least minimum required miners.
        """
        try:
            cid = assignment_data['cid']
            owner = assignment_data['owner']
            filename = assignment_data.get('filename', '')
            file_size_bytes = assignment_data.get('file_size_bytes', 0)
            assigned_miners = assignment_data.get('assigned_miners', [])
            pending_file_id = assignment_data.get('pending_file_id')
            
            # Filter out None values from assigned miners
            valid_miners = [m for m in assigned_miners if m is not None and m.strip()]
            
            # CRITICAL VALIDATION: Require minimum miners
            min_required_miners = int(os.getenv('MIN_REQUIRED_MINERS', '3'))  # Default 3, can be configured
            
            if len(valid_miners) < min_required_miners:
                logger.error(f"❌ REJECTED assignment for file {cid}: Only {len(valid_miners)} valid miners, need minimum {min_required_miners}")
                logger.error(f"   File: {filename} ({file_size_bytes:,} bytes)")
                logger.error(f"   Valid miners: {valid_miners}")
                
                # Mark pending file as failed with specific error
                if pending_file_id:
                    try:
                        async with self.db_pool.acquire() as conn:
                            await conn.execute("""
                                UPDATE pending_assignment_file
                                SET status = 'failed', 
                                    error_message = $1,
                                    processed_at = CURRENT_TIMESTAMP
                                WHERE id = $2
                            """, f"Insufficient miners: {len(valid_miners)}/{min_required_miners} required", pending_file_id)
                    except Exception as e:
                        logger.error(f"Error marking pending file as failed: {e}")
                
                return False
            
            logger.info(f"✅ Processing assignment for file {cid}: {len(valid_miners)} valid miners (≥{min_required_miners} required)")
        
        except Exception as e:
            logger.error(f"Error validating assignment for file {cid}: {e}")
            return False
        
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Insert/update the file in the files table
                    await conn.execute("""
                        INSERT INTO files (cid, name, size, created_date)
                        VALUES ($1, $2, $3, NOW())
                        ON CONFLICT (cid) DO UPDATE SET
                            name = EXCLUDED.name,
                            size = EXCLUDED.size
                    """, cid, filename, file_size_bytes)
                    
                    # 2. Prepare miner assignments (pad ONLY valid miners to 5 slots)
                    miners_padded = (valid_miners + [None] * 5)[:5]
                    
                    # 3. Insert/update file assignments (only with valid miners)
                    await conn.execute("""
                        INSERT INTO file_assignments (cid, owner, miner1, miner2, miner3, miner4, miner5)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (cid) DO UPDATE SET
                            owner = EXCLUDED.owner,
                            miner1 = EXCLUDED.miner1,
                            miner2 = EXCLUDED.miner2,
                            miner3 = EXCLUDED.miner3,
                            miner4 = EXCLUDED.miner4,
                            miner5 = EXCLUDED.miner5,
                            updated_at = CURRENT_TIMESTAMP
                    """, cid, owner, miners_padded[0], miners_padded[1], 
                        miners_padded[2], miners_padded[3], miners_padded[4])
                    
                    # 4. Update pending_assignment_file status if we have the ID
                    if pending_file_id:
                        await conn.execute("""
                            UPDATE pending_assignment_file
                            SET status = 'assigned', processed_at = CURRENT_TIMESTAMP
                            WHERE id = $1
                        """, pending_file_id)
                    
                    # 5. Batch update miner stats for assigned miners
                    if valid_miners:
                        batch_data = [(miner_id, file_size_bytes) for miner_id in valid_miners]
                        await conn.executemany("""
                            INSERT INTO miner_stats (
                                node_id, total_files_pinned, total_files_size_bytes, updated_at
                            )
                            VALUES ($1, 1, $2, NOW())
                            ON CONFLICT (node_id) DO UPDATE SET
                                total_files_pinned = miner_stats.total_files_pinned + 1,
                                total_files_size_bytes = miner_stats.total_files_size_bytes + $2,
                                updated_at = NOW()
                        """, batch_data)
                    
                    logger.info(f"✅ Successfully assigned file {cid[:16]}... to {len(valid_miners)} miners: {', '.join(valid_miners)}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error processing new assignment for file {cid}: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def process_reassignment(self, assignment_data: Dict[str, Any]) -> bool:
        """
        Process a file reassignment task (filling empty slots).
        
        ENHANCED: Better handling of completely empty assignments and improved logging.
        
        Args:
            assignment_data: Reassignment data from the queue
            
        Returns:
            True if processed successfully, False otherwise
        """
        cid = assignment_data.get('cid')
        owner = assignment_data.get('owner')
        filename = assignment_data.get('filename', '')
        file_size_bytes = assignment_data.get('file_size_bytes', 0)
        current_miners = assignment_data.get('current_miners', [])
        new_miners = assignment_data.get('new_miners', [])
        null_miner_count = assignment_data.get('null_miner_count', 0)  # From enhanced processor
        
        if not cid or not owner or not new_miners:
            logger.error(f"Invalid reassignment data: missing cid, owner, or new_miners. Data: {assignment_data}")
            return False
        
        # Enhanced logging for NULL miner fixes
        if null_miner_count == 5:
            logger.info(f"🎯 Processing COMPLETELY EMPTY assignment for file {filename} ({cid[:16]}...) - filling all 5 slots")
        elif null_miner_count > 0:
            logger.info(f"📋 Processing PARTIAL reassignment for file {filename} ({cid[:16]}...) - filling {null_miner_count} empty slots")
        else:
            logger.info(f"🔄 Processing reassignment for file {filename} ({cid[:16]}...) - adding {len(new_miners)} miners")
        
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Get current assignment state (to handle race conditions)
                    current_assignment = await conn.fetchrow("""
                        SELECT miner1, miner2, miner3, miner4, miner5, updated_at
                        FROM file_assignments
                        WHERE cid = $1
                        FOR UPDATE
                    """, cid)
                    
                    if not current_assignment:
                        logger.error(f"File assignment not found for CID {cid}")
                        return False
                    
                    # 2. Build the updated miner list
                    current_list = [
                        current_assignment['miner1'], current_assignment['miner2'],
                        current_assignment['miner3'], current_assignment['miner4'],
                        current_assignment['miner5']
                    ]
                    
                    # Debug: Log current state
                    current_null_count = sum(1 for m in current_list if m is None)
                    logger.debug(f"Current assignment state: {current_null_count} NULL slots out of 5")
                    
                    # 3. Fill empty slots with new miners
                    new_miner_index = 0
                    updated_miners = []
                    
                    for i, current_miner in enumerate(current_list):
                        if current_miner is None and new_miner_index < len(new_miners):
                            # Fill empty slot with new miner
                            updated_miners.append(new_miners[new_miner_index])
                            logger.debug(f"   Slot {i+1}: NULL → {new_miners[new_miner_index]}")
                            new_miner_index += 1
                        else:
                            # Keep existing miner (or None if no new miners left)
                            updated_miners.append(current_miner)
                            if current_miner:
                                logger.debug(f"   Slot {i+1}: Keeping {current_miner}")
                    
                    # 4. Update file assignments with race condition protection
                    result = await conn.execute("""
                        UPDATE file_assignments
                        SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cid = $1
                        AND updated_at = $7
                    """, cid, updated_miners[0], updated_miners[1], updated_miners[2], 
                        updated_miners[3], updated_miners[4], current_assignment['updated_at'])
                    
                    # Check if update was successful (no race condition)
                    if result == "UPDATE 0":
                        logger.warning(f"Race condition detected for file {cid} - assignment was modified by another process")
                        return False
                    
                    # 5. Batch update miner stats for newly assigned miners
                    valid_new_miners = [miner_id for miner_id in new_miners if miner_id]
                    if valid_new_miners:
                        batch_data = [(miner_id, file_size_bytes) for miner_id in valid_new_miners]
                        await conn.executemany("""
                            INSERT INTO miner_stats (
                                node_id, total_files_pinned, total_files_size_bytes, updated_at
                            )
                            VALUES ($1, 1, $2, NOW())
                            ON CONFLICT (node_id) DO UPDATE SET
                                total_files_pinned = miner_stats.total_files_pinned + 1,
                                total_files_size_bytes = miner_stats.total_files_size_bytes + $2,
                                updated_at = NOW()
                        """, batch_data)
                    
                    # Enhanced success logging
                    final_null_count = sum(1 for m in updated_miners if m is None)
                    if final_null_count == 0:
                        logger.info(f"✅ Successfully COMPLETED assignment for file {cid[:16]}... - ALL 5 slots now filled!")
                    else:
                        logger.info(f"✅ Successfully reassigned file {cid[:16]}... - {5-final_null_count}/5 slots filled ({final_null_count} still empty)")
                    
                    logger.info(f"   Added miners: {', '.join(new_miners)}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error processing reassignment for file {cid}: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def process_failing_miner_replacement(self, assignment_data: Dict[str, Any]) -> bool:
        """
        Process a failing miner replacement task.
        
        Args:
            assignment_data: Replacement data from the queue
            
        Returns:
            True if processed successfully, False otherwise
        """
        cid = assignment_data.get('cid')
        owner = assignment_data.get('owner')
        filename = assignment_data.get('filename', '')
        file_size_bytes = assignment_data.get('file_size_bytes', 0)
        old_miners = assignment_data.get('old_miners', [])
        new_miners = assignment_data.get('new_miners', [])
        failing_miners = assignment_data.get('failing_miners', [])
        replacement_miners = assignment_data.get('replacement_miners', [])
        
        if not cid or not owner or not new_miners:
            logger.error(f"Invalid failing miner replacement data: missing cid, owner, or new_miners. Data: {assignment_data}")
            return False
        
        logger.info(f"Processing failing miner replacement for file {filename} ({cid[:16]}...) - "
                   f"replacing {len(failing_miners)} failing miners with {len(replacement_miners)} healthy miners")
        
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Get current assignment state (to handle race conditions)
                    current_assignment = await conn.fetchrow("""
                        SELECT miner1, miner2, miner3, miner4, miner5, updated_at
                        FROM file_assignments
                        WHERE cid = $1
                        FOR UPDATE
                    """, cid)
                    
                    if not current_assignment:
                        logger.error(f"File assignment not found for CID {cid}")
                        return False
                    
                    # 2. Update file assignments with new miners (this will update updated_at timestamp)
                    # The new_miners list already has the replacements in the correct positions
                    result = await conn.execute("""
                        UPDATE file_assignments
                        SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cid = $1
                        AND updated_at = $7
                    """, cid, new_miners[0], new_miners[1], new_miners[2], 
                        new_miners[3], new_miners[4], current_assignment['updated_at'])
                    
                    # Check if update was successful (no race condition)
                    if result == "UPDATE 0":
                        logger.warning(f"Race condition detected for file {cid} - assignment was modified by another process")
                        return False
                    
                    # 3. Update miner stats for newly assigned miners (add)
                    for miner_id in replacement_miners:
                        if miner_id:  # Skip None values
                            await conn.execute("""
                                INSERT INTO miner_stats (
                                    node_id, total_files_pinned, total_files_size_bytes, updated_at
                                )
                                VALUES ($1, 1, $2, NOW())
                                ON CONFLICT (node_id) DO UPDATE SET
                                    total_files_pinned = miner_stats.total_files_pinned + 1,
                                    total_files_size_bytes = miner_stats.total_files_size_bytes + $2,
                                    updated_at = NOW()
                            """, miner_id, file_size_bytes)
                    
                    # 4. Update miner stats for removed failing miners (subtract)
                    for miner_id in failing_miners:
                        if miner_id:  # Skip None values
                            await conn.execute("""
                                UPDATE miner_stats
                                SET total_files_pinned = GREATEST(0, total_files_pinned - 1),
                                    total_files_size_bytes = GREATEST(0, total_files_size_bytes - $2),
                                    updated_at = NOW()
                                WHERE node_id = $1
                            """, miner_id, file_size_bytes)
                    
                    logger.info(f"Successfully replaced failing miners for file {cid[:16]}... - "
                               f"removed: {', '.join(failing_miners)}, added: {', '.join(replacement_miners)}")
                    
                    # The updated_at timestamp change will automatically trigger user profile reconstruction
                    # in the next user_profile_reconstruction_processor run
                    logger.info(f"File assignment updated - user profile for {owner} will be reconstructed in next cycle")
                    
                    return True
                    
        except Exception as e:
            logger.error(f"Error processing failing miner replacement for file {cid}: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def process_rebalancing(self, assignment_data: Dict[str, Any]) -> bool:
        """
        Process a file rebalancing task (moving a file from one miner to another).
        
        Args:
            assignment_data: Rebalancing data from the queue
            
        Returns:
            True if processed successfully, False otherwise
        """
        cid = assignment_data.get('cid')
        filename = assignment_data.get('filename', '')
        file_size_bytes = assignment_data.get('file_size_bytes', 0)
        from_miner = assignment_data.get('from_miner')
        to_miner = assignment_data.get('to_miner')
        reason = assignment_data.get('reason', 'Network rebalancing')
        
        if not cid or not from_miner or not to_miner:
            logger.error(f"Invalid rebalancing data: missing cid, from_miner, or to_miner. Data: {assignment_data}")
            return False
        
        logger.info(f"🔄 Processing rebalancing for file {filename} ({cid[:16]}...) - "
                   f"moving from {from_miner[:12]}... to {to_miner[:12]}... ({reason})")
        
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Get current assignment state (to handle race conditions)
                    current_assignment = await conn.fetchrow("""
                        SELECT miner1, miner2, miner3, miner4, miner5, updated_at
                        FROM file_assignments
                        WHERE cid = $1
                        FOR UPDATE
                    """, cid)
                    
                    if not current_assignment:
                        logger.error(f"File assignment not found for CID {cid}")
                        return False
                    
                    # 2. Build the current miner list
                    current_list = [
                        current_assignment['miner1'], current_assignment['miner2'],
                        current_assignment['miner3'], current_assignment['miner4'],
                        current_assignment['miner5']
                    ]
                    
                    # 3. Find and replace the from_miner with to_miner
                    updated_miners = []
                    found_miner = False
                    
                    for current_miner in current_list:
                        if current_miner == from_miner and not found_miner:
                            # Replace the first occurrence of from_miner with to_miner
                            updated_miners.append(to_miner)
                            found_miner = True
                            logger.debug(f"   Replaced {from_miner[:12]}... with {to_miner[:12]}...")
                        else:
                            # Keep existing miner
                            updated_miners.append(current_miner)
                    
                    # 4. Verify the replacement was made
                    if not found_miner:
                        logger.error(f"Miner {from_miner} not found in current assignment for file {cid}")
                        logger.error(f"Current miners: {current_list}")
                        return False
                    
                    # 5. Verify to_miner is not already assigned (prevent duplicates)
                    original_to_count = current_list.count(to_miner)
                    if original_to_count > 0:
                        logger.warning(f"Target miner {to_miner} is already assigned to file {cid} ({original_to_count} times)")
                        # Allow it but log the warning
                    
                    # 6. Update file assignments
                    result = await conn.execute("""
                        UPDATE file_assignments
                        SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cid = $1
                        AND updated_at = $7
                    """, cid, updated_miners[0], updated_miners[1], updated_miners[2], 
                        updated_miners[3], updated_miners[4], current_assignment['updated_at'])
                    
                    # Check if update was successful (no race condition)
                    if result == "UPDATE 0":
                        logger.warning(f"Race condition detected for file {cid} - assignment was modified by another process")
                        return False
                    
                    # 7. Update miner stats for the new miner (add)
                    await conn.execute("""
                        INSERT INTO miner_stats (
                            node_id, total_files_pinned, total_files_size_bytes, updated_at
                        )
                        VALUES ($1, 1, $2, NOW())
                        ON CONFLICT (node_id) DO UPDATE SET
                            total_files_pinned = miner_stats.total_files_pinned + 1,
                            total_files_size_bytes = miner_stats.total_files_size_bytes + $2,
                            updated_at = NOW()
                    """, to_miner, file_size_bytes)
                    
                    # 8. Update miner stats for the old miner (subtract)
                    await conn.execute("""
                        UPDATE miner_stats
                        SET total_files_pinned = GREATEST(0, total_files_pinned - 1),
                            total_files_size_bytes = GREATEST(0, total_files_size_bytes - $2),
                            updated_at = NOW()
                        WHERE node_id = $1
                    """, from_miner, file_size_bytes)
                    
                    logger.info(f"✅ Successfully rebalanced file {cid[:16]}... - "
                               f"moved from {from_miner[:12]}... to {to_miner[:12]}...")
                    
                    return True
                    
        except Exception as e:
            logger.error(f"Error processing rebalancing for file {cid}: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def process_assignment(self, assignment_data: Dict[str, Any]) -> bool:
        """
        Process a file assignment task (new, reassignment, failing miner replacement, or rebalancing).
        
        Args:
            assignment_data: Assignment data from the queue
            
        Returns:
            True if processed successfully, False otherwise
        """
        assignment_type = assignment_data.get('type', 'new_assignment')
        
        if assignment_type == 'new_assignment':
            return await self.process_new_assignment(assignment_data)
        elif assignment_type == 'reassignment':
            return await self.process_reassignment(assignment_data)
        elif assignment_type == 'failing_miner_replacement':
            return await self.process_failing_miner_replacement(assignment_data)
        elif assignment_type == 'rebalancing':
            return await self.process_rebalancing(assignment_data)
        else:
            logger.error(f"Unknown assignment type: {assignment_type}")
            return False
    
    async def message_handler(self, message: IncomingMessage):
        """Handle incoming assignment messages."""
        async with message.process():
            try:
                # Parse message
                assignment_data = json.loads(message.body.decode())
                
                # Process the assignment
                success = await self.process_assignment(assignment_data)
                
                if success:
                    logger.debug(f"Successfully processed {assignment_data.get('type', 'assignment')} for {assignment_data.get('cid', 'unknown')}")
                else:
                    logger.error(f"Failed to process {assignment_data.get('type', 'assignment')} for {assignment_data.get('cid', 'unknown')}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
            except Exception as e:
                logger.error(f"Error in message handler: {e}")
                logger.exception("Full traceback:")
    
    async def start_consuming(self):
        """Start consuming messages from the queue."""
        try:
            # Declare the queue (in case it doesn't exist)
            queue = await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Starting to consume from queue '{self.queue_name}'")
            
            # Start consuming
            await queue.consume(self.message_handler)
            
            # Keep the consumer running
            logger.info("File assignment consumer is running. Press Ctrl+C to stop.")
            try:
                await asyncio.Future()  # Run forever
            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
            
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
            raise
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")


async def main():
    """Main entry point."""
    consumer = FileAssignmentConsumer()
    
    try:
        # Initialize database pool
        await init_db_pool()
        consumer.db_pool = get_db_pool()
        logger.info("Database connection pool initialized")
        
        # Connect to RabbitMQ
        await consumer.connect_rabbitmq()
        
        # Start consuming
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Shutting down file assignment consumer...")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        raise
    finally:
        await consumer.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 