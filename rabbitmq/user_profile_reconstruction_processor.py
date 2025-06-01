#!/usr/bin/env python3
"""
Processor that fetches user profiles from the database and queues them for reconstruction.
This processor reads user data from file_assignments table, groups files by owner,
and sends them to a queue for reconstruction and publishing to IPFS.
"""

import asyncio
import json
import logging
import os
from typing import Dict, List, Any

import aio_pika
import asyncpg
from aio_pika import Message
from substrateinterface import SubstrateInterface

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UserProfileReconstructionProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/substrate_fetcher')
        self.node_url = os.getenv('NODE_URL', 'wss://rpc.hippius.network')
        self.queue_name = 'user_profile_reconstruction'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.current_block = None
    
    async def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=10
            )
            logger.info("Connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare queue"""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Declare queue
            await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def fetch_current_block(self):
        """Fetch the current block number from the substrate chain"""
        try:
            substrate = SubstrateInterface(url=self.node_url)
            block_hash = substrate.get_chain_head()
            block_number = substrate.get_block_number(block_hash)
            self.current_block = block_number
            logger.info(f"Current block number: {self.current_block}")
            substrate.close()
        except Exception as e:
            logger.error(f"Failed to fetch current block: {e}")
            # Use a default block number if we can't fetch it
            self.current_block = 0
    
    async def assign_fallback_miners(self, owner: str, unassigned_files: List[Dict[str, Any]], conn) -> None:
        """
        Assign miners to unassigned files as a fallback during profile reconstruction.
        This ensures no files are lost if the main file assignment process missed them.
        Uses load balancing to distribute files across available miners.
        """
        try:
            # Get available miners with capacity
            available_miners = await conn.fetch("""
                SELECT 
                    r.node_id,
                    r.ipfs_peer_id,
                    COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                    COALESCE(nm.ipfs_repo_size, 0) as used_storage_bytes,
                    COALESCE(ms.total_files_pinned, 0) as total_files_pinned,
                    COALESCE(ms.total_files_size_bytes, 0) as total_files_size_bytes,
                    COALESCE(ms.health_score, 100) as health_score
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
                  AND COALESCE(ms.health_score, 100) >= 70
                ORDER BY COALESCE(ms.health_score, 100) DESC, r.node_id
                LIMIT 50
            """)
            
            if not available_miners:
                logger.error(f"No available miners found for fallback assignment for user {owner}")
                return
            
            # Track assignments for load balancing within this fallback session
            fallback_assignments = {}
            replicas_per_file = 5
            
            for file_data in unassigned_files:
                cid = file_data['cid']
                file_size = file_data['size']
                filename = file_data['name']
                
                # Add 20% safety margin for IPFS overhead, metadata, and growth
                safety_margin = int(file_size * 0.2)
                required_space = file_size + safety_margin
                
                # Score miners based on capacity and current fallback assignments
                scored_miners = []
                for miner in available_miners:
                    # Use actual IPFS repo size as primary indicator of usage
                    ipfs_repo_size = miner['used_storage_bytes']  # from node_metrics.ipfs_repo_size
                    calculated_size = miner['total_files_size_bytes']  # from our miner_stats
                    
                    # Use the higher value as a safety measure, but prefer actual IPFS data
                    if ipfs_repo_size > 0:
                        used_storage = ipfs_repo_size
                        if calculated_size > ipfs_repo_size:
                            used_storage = calculated_size
                    else:
                        used_storage = calculated_size
                    
                    storage_capacity = miner['storage_capacity_bytes']
                    available_storage = max(0, storage_capacity - used_storage)
                    
                    # Check if miner has enough space
                    if available_storage < required_space:
                        continue
                    
                    # Calculate score based on available storage and current assignments
                    storage_score = available_storage / storage_capacity if storage_capacity > 0 else 0
                    health_score = min(1.0, miner['health_score'] / 100.0)
                    
                    # Penalize miners that have been assigned files in this fallback session
                    fallback_count = fallback_assignments.get(miner['node_id'], 0)
                    load_penalty = 0.8 ** fallback_count  # Exponential penalty
                    
                    # Combined score
                    final_score = (storage_score * 0.6 + health_score * 0.4) * load_penalty
                    
                    scored_miners.append({
                        'node_id': miner['node_id'],
                        'score': final_score,
                        'available_storage': available_storage
                    })
                
                if not scored_miners:
                    logger.error(f"No miners with sufficient capacity for file {cid} (size: {file_size:,}, need: {required_space:,} with safety margin)")
                    # Use first available miners as last resort
                    selected_miners = [m['node_id'] for m in available_miners[:min(3, len(available_miners))]]
                    logger.warning(f"Using {len(selected_miners)} miners as last resort for file {cid}")
                else:
                    # Use weighted random selection for better distribution
                    selected_miners = self._fallback_weighted_selection(scored_miners, replicas_per_file)
                
                # Update fallback assignment tracking
                for miner_id in selected_miners:
                    fallback_assignments[miner_id] = fallback_assignments.get(miner_id, 0) + 1
                
                # Update the file_data with assigned miners
                file_data['miner_ids'] = selected_miners
                file_data['total_replicas'] = len(selected_miners)
                
                # Insert into file_assignments table
                miners_padded = (selected_miners + [None] * 5)[:5]
                
                await conn.execute("""
                    INSERT INTO files (cid, name, size, created_date)
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (cid) DO UPDATE SET
                        name = EXCLUDED.name,
                        size = EXCLUDED.size
                """, cid, filename, file_size)
                
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
                
                # Mark as assigned in pending_assignment_file
                await conn.execute("""
                    UPDATE pending_assignment_file
                    SET status = 'assigned', processed_at = CURRENT_TIMESTAMP
                    WHERE cid = $1 AND owner = $2
                """, cid, owner)
                
                logger.info(f"Fallback assigned file {filename} ({cid[:16]}...) to {len(selected_miners)} miners: {', '.join(selected_miners[:3])}{'...' if len(selected_miners) > 3 else ''}")
            
            # Log fallback distribution stats
            if fallback_assignments:
                total_fallback = sum(fallback_assignments.values())
                unique_miners = len(fallback_assignments)
                logger.info(f"Fallback assignment stats for {owner}: {total_fallback} assignments across {unique_miners} miners")
                
        except Exception as e:
            logger.error(f"Error in fallback miner assignment for user {owner}: {e}")
    
    def _fallback_weighted_selection(self, scored_miners: List[Dict[str, Any]], count: int) -> List[str]:
        """Weighted random selection for fallback assignments."""
        import random
        
        if len(scored_miners) <= count:
            return [m['node_id'] for m in scored_miners]
        
        # Create probability distribution based on scores
        total_score = sum(m['score'] for m in scored_miners)
        if total_score <= 0:
            # Fallback to random selection if all scores are zero
            return [m['node_id'] for m in random.sample(scored_miners, count)]
        
        # Select miners without replacement using weighted probabilities
        selected_miners = []
        remaining_miners = scored_miners.copy()
        
        for _ in range(count):
            if not remaining_miners:
                break
            
            # Calculate current probabilities
            current_total = sum(m['score'] for m in remaining_miners)
            if current_total <= 0:
                # Random selection for remaining
                selected_miner = random.choice(remaining_miners)
            else:
                probabilities = [m['score'] / current_total for m in remaining_miners]
                selected_idx = random.choices(range(len(remaining_miners)), weights=probabilities)[0]
                selected_miner = remaining_miners[selected_idx]
            
            selected_miners.append(selected_miner['node_id'])
            remaining_miners.remove(selected_miner)
        
        return selected_miners

    async def fetch_user_profiles_to_reconstruct(self) -> List[Dict[str, Any]]:
        """Fetch user profiles that need to be reconstructed from file_assignments and pending files"""
        # Get batch size from environment variable (0 means no limit)
        batch_size = int(os.getenv('USER_PROFILE_BATCH_SIZE', '100'))
        
        async with self.db_pool.acquire() as conn:
            # Get users who either:
            # 1. Don't have any published profile yet, OR
            # 2. Have new files since their last profile was published (from either table)
            query = """
            WITH user_file_stats AS (
                -- Files from file_assignments table (already assigned)
                SELECT 
                    fa.owner,
                    COUNT(*) as assigned_file_count,
                    SUM(COALESCE(f.size, 0)) as assigned_total_size,
                    MAX(fa.updated_at) as latest_assigned_update
                FROM file_assignments fa
                LEFT JOIN files f ON f.cid = fa.cid
                WHERE fa.owner IS NOT NULL
                GROUP BY fa.owner
            ),
            user_pending_stats AS (
                -- Files from pending_assignment_file table (new from storage requests)
                SELECT 
                    paf.owner,
                    COUNT(*) as pending_file_count,
                    SUM(COALESCE(paf.file_size_bytes, 0)) as pending_total_size,
                    MAX(paf.processed_at) as latest_pending_update
                FROM pending_assignment_file paf
                WHERE paf.owner IS NOT NULL
                AND paf.status = 'processed'
                AND paf.file_size_bytes IS NOT NULL
                -- Only count files that are NOT already in file_assignments
                AND NOT EXISTS (
                    SELECT 1 FROM file_assignments fa 
                    WHERE fa.cid = paf.cid
                )
                GROUP BY paf.owner
            ),
            combined_user_stats AS (
                -- Combine stats from both tables
                SELECT 
                    COALESCE(ufs.owner, ups.owner) as owner,
                    COALESCE(ufs.assigned_file_count, 0) + COALESCE(ups.pending_file_count, 0) as current_file_count,
                    COALESCE(ufs.assigned_total_size, 0) + COALESCE(ups.pending_total_size, 0) as current_total_size,
                    GREATEST(
                        COALESCE(ufs.latest_assigned_update, '1970-01-01'::timestamp),
                        COALESCE(ups.latest_pending_update, '1970-01-01'::timestamp)
                    ) as latest_file_update,
                    COALESCE(ups.pending_file_count, 0) as new_pending_files
                FROM user_file_stats ufs
                FULL OUTER JOIN user_pending_stats ups ON ufs.owner = ups.owner
                WHERE COALESCE(ufs.assigned_file_count, 0) + COALESCE(ups.pending_file_count, 0) > 0
            ),
            latest_profiles AS (
                SELECT DISTINCT ON (owner) 
                    owner,
                    files_count,
                    files_size,
                    created_at as profile_created_at
                FROM pending_user_profile 
                WHERE status = 'published'
                ORDER BY owner, created_at DESC
            )
            SELECT DISTINCT
                cus.owner,
                cus.current_file_count,
                cus.current_total_size,
                cus.latest_file_update,
                cus.new_pending_files,
                COALESCE(lp.files_count, 0) as last_profile_file_count,
                COALESCE(lp.files_size, 0) as last_profile_total_size,
                lp.profile_created_at
            FROM combined_user_stats cus
            LEFT JOIN latest_profiles lp ON lp.owner = cus.owner
            WHERE 
                -- No published profile yet
                lp.owner IS NULL
                OR 
                -- File count changed
                cus.current_file_count != COALESCE(lp.files_count, 0)
                OR 
                -- Total size changed
                cus.current_total_size != COALESCE(lp.files_size, 0)
                OR
                -- New files added since last profile (if we have timestamps)
                (lp.profile_created_at IS NOT NULL AND cus.latest_file_update > lp.profile_created_at)
                OR
                -- Has new pending files from storage requests
                cus.new_pending_files > 0
            ORDER BY cus.owner
            """
            
            if batch_size > 0:
                query += f" LIMIT {batch_size}"
            
            users_rows = await conn.fetch(query)
            
            # Log what we found for debugging
            for row in users_rows:
                if row['last_profile_file_count'] > 0:
                    logger.info(f"User {row['owner']} needs profile update: "
                              f"files {row['last_profile_file_count']} -> {row['current_file_count']}, "
                              f"size {row['last_profile_total_size']} -> {row['current_total_size']}")
                    if row['new_pending_files'] > 0:
                        logger.info(f"  - Including {row['new_pending_files']} NEW files from storage requests")
                else:
                    logger.info(f"User {row['owner']} needs initial profile: "
                              f"{row['current_file_count']} files, {row['current_total_size']} bytes")
                    if row['new_pending_files'] > 0:
                        logger.info(f"  - Including {row['new_pending_files']} NEW files from storage requests")
            
            return [{'owner': row['owner']} for row in users_rows]
    
    async def fetch_user_profile_files(self, owner: str) -> List[Dict[str, Any]]:
        """Fetch all files owned by a specific user and assign miners to any unassigned files"""
        async with self.db_pool.acquire() as conn:
            # Get files from file_assignments table (already assigned files)
            assigned_rows = await conn.fetch("""
                SELECT DISTINCT
                    f.cid,
                    f.name,
                    f.size,
                    f.created_date,
                    fa.miner1,
                    fa.miner2,
                    fa.miner3,
                    fa.miner4,
                    fa.miner5,
                    fa.updated_at,
                    'assigned' as source
                FROM files f
                JOIN file_assignments fa ON f.cid = fa.cid
                WHERE fa.owner = $1
                ORDER BY f.created_date ASC
            """, owner)
            
            # Get files from pending_assignment_file table (new files from storage requests)
            pending_rows = await conn.fetch("""
                SELECT DISTINCT
                    paf.cid,
                    paf.filename as name,
                    paf.file_size_bytes as size,
                    paf.created_at as created_date,
                    NULL as miner1,
                    NULL as miner2,
                    NULL as miner3,
                    NULL as miner4,
                    NULL as miner5,
                    paf.processed_at as updated_at,
                    'pending' as source
                FROM pending_assignment_file paf
                WHERE paf.owner = $1
                AND paf.status = 'processed'
                AND paf.file_size_bytes IS NOT NULL
                -- Only include files that are NOT already in file_assignments
                AND NOT EXISTS (
                    SELECT 1 FROM file_assignments fa 
                    WHERE fa.cid = paf.cid
                )
                ORDER BY paf.created_at ASC
            """, owner)
            
            # Combine both sets of files
            all_rows = list(assigned_rows) + list(pending_rows)
            
            # Convert to proper format and handle datetime serialization
            files = []
            unassigned_files = []
            
            for row in all_rows:
                # Collect assigned miners (filter out None values)
                miner_ids = [
                    miner for miner in [
                        row['miner1'], row['miner2'], row['miner3'], 
                        row['miner4'], row['miner5']
                    ] if miner is not None
                ]
                
                file_data = {
                    'cid': row['cid'],
                    'name': row['name'] or 'unknown',
                    'size': row['size'] or 0,
                    'miner_ids': miner_ids,
                    'total_replicas': len(miner_ids),
                    'source': row['source']
                }
                
                # Convert datetime to string if present
                if row['created_date']:
                    file_data['created_date'] = row['created_date'].isoformat()
                if row['updated_at']:
                    file_data['last_charged_at'] = row['updated_at'].isoformat()
                
                # Track files that need miner assignment
                if len(miner_ids) == 0:
                    unassigned_files.append(file_data)
                
                files.append(file_data)
            
            # Assign miners to unassigned files as fallback
            if unassigned_files:
                logger.warning(f"User {owner}: Found {len(unassigned_files)} files without miners - assigning fallback miners")
                await self.assign_fallback_miners(owner, unassigned_files, conn)
            
            # Log what we found for debugging
            assigned_count = len(assigned_rows)
            pending_count = len(pending_rows)
            logger.info(f"User {owner}: Found {assigned_count} assigned files + {pending_count} pending files = {len(files)} total files")
            
            if pending_count > 0:
                logger.info(f"Including {pending_count} files from storage requests in profile for {owner}")
                # Log sample pending files
                for row in pending_rows[:3]:  # Show first 3
                    logger.info(f"  - NEW: {row['name']} ({row['cid'][:16]}...) - {row['size']:,} bytes")
            
            return files
    
    async def send_to_queue(self, profile_data: Dict[str, Any]) -> None:
        """Send profile data to RabbitMQ queue"""
        message_body = json.dumps(profile_data)
        message = Message(
            body=message_body.encode(),
            delivery_mode=2  # Make message persistent
        )
        
        await self.rabbitmq_channel.default_exchange.publish(
            message,
            routing_key=self.queue_name
        )
        
        logger.debug(f"Sent profile to queue: {profile_data['owner']} -> {profile_data['cid']}")
    
    async def process_profiles(self):
        """Main processing loop"""
        profiles = await self.fetch_user_profiles_to_reconstruct()
        
        if not profiles:
            logger.info("No user profiles to reconstruct")
            return
        
        logger.info(f"Found {len(profiles)} user profiles to reconstruct")
        
        for profile in profiles:
            try:
                owner = profile['owner']
                
                # Fetch files for this user
                files = await self.fetch_user_profile_files(owner)
                
                # Calculate file count and total size
                file_count = len(files)
                total_size = sum(file_data.get('size', 0) for file_data in files)
                
                # Skip users with no files
                if file_count == 0:
                    logger.info(f"Skipping user {owner} - no files assigned")
                    continue
                
                # Generate a synthetic CID for the profile (we'll use the owner as base)
                profile_cid = f"user_profile_{owner}"
                
                # Prepare message data
                message_data = {
                    'cid': profile_cid,
                    'owner': owner,
                    'file_count': file_count,
                    'files': files,
                    'total_size': total_size,
                    'block_number': self.current_block
                }
                
                # Send to queue
                await self.send_to_queue(message_data)
                
                logger.info(f"Queued profile for user {owner}: {file_count} files, {total_size} bytes")
                
            except Exception as e:
                logger.error(f"Error processing profile for user {profile['owner']}: {e}")
                continue
        
        logger.info(f"Successfully queued {len(profiles)} profiles for reconstruction")
    
    async def close(self):
        """Close connections"""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Closed database connection")


async def main():
    processor = UserProfileReconstructionProcessor()
    
    try:
        # Connect to services
        await processor.connect_database()
        await processor.connect_rabbitmq()
        
        # Fetch current block number
        await processor.fetch_current_block()
        
        # Process profiles
        await processor.process_profiles()
        
    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main()) 