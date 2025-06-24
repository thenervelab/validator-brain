#!/usr/bin/env python3
"""
Network Rebalancing Processor

This processor analyzes the distribution of files across miners and performs
rebalancing operations to ensure fair distribution by both file count and storage size.

Features:
1. File count rebalancing - prevents miners from having too many/few files
2. Size-based rebalancing - ensures fair storage utilization distribution  
3. Capacity-aware moves - respects miner storage limits
4. Health-aware selection - prioritizes healthy miners for reassignments
5. Gradual rebalancing - moves files incrementally to avoid disruption
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

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


@dataclass
class MinerLoad:
    """Represents the current load for a miner."""
    miner_id: str
    file_count: int
    total_size_bytes: int
    storage_capacity_bytes: int
    used_storage_bytes: int  # From IPFS repo
    available_storage_bytes: int
    health_score: float
    days_since_registration: float
    
    @property
    def storage_utilization(self) -> float:
        """Calculate storage utilization percentage (0-1)."""
        if self.storage_capacity_bytes <= 0:
            return 1.0  # Assume full if no capacity data
        return min(1.0, self.used_storage_bytes / self.storage_capacity_bytes)
    
    @property
    def assigned_storage_utilization(self) -> float:
        """Calculate utilization based on assigned files (0-1)."""
        if self.storage_capacity_bytes <= 0:
            return 1.0
        return min(1.0, self.total_size_bytes / self.storage_capacity_bytes)


@dataclass
class RebalanceCandidate:
    """Represents a file that could be moved for rebalancing."""
    cid: str
    filename: str
    size_bytes: int
    owner: str
    current_miners: List[str]
    overloaded_miner: str  # The miner to move FROM
    reason: str  # Why this file should be moved
    priority: float  # Higher = more important to move


@dataclass  
class RebalanceAction:
    """Represents a specific rebalancing action."""
    cid: str
    filename: str
    size_bytes: int
    from_miner: str
    to_miner: str
    reason: str
    file_count_improvement: int  # Net file count improvement
    size_improvement_bytes: int  # Net size distribution improvement


class NetworkRebalancingProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'file_assignment_processing'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.substrate = None
        self.db_pool = None
        
        # Rebalancing configuration
        self.max_files_per_batch = int(os.getenv('REBALANCE_MAX_FILES_PER_BATCH', '20'))
        self.file_count_threshold = float(os.getenv('REBALANCE_FILE_COUNT_THRESHOLD', '2.0'))  # Standard deviations
        self.size_threshold = float(os.getenv('REBALANCE_SIZE_THRESHOLD', '1.5'))  # Standard deviations  
        self.min_miner_health_score = float(os.getenv('MIN_MINER_HEALTH_SCORE', '70.0'))
        self.max_utilization_target = float(os.getenv('REBALANCE_MAX_UTILIZATION', '0.85'))  # 85% max
        self.min_available_space_mb = int(os.getenv('REBALANCE_MIN_AVAILABLE_MB', '1000'))  # 1GB minimum
        
        # Safety limits
        self.max_moves_per_miner = int(os.getenv('REBALANCE_MAX_MOVES_PER_MINER', '3'))
        self.rebalance_interval_hours = int(os.getenv('REBALANCE_INTERVAL_HOURS', '6'))
        
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
            
            # Declare the queue (reuse file assignment queue)
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
    
    async def get_miner_loads(self) -> List[MinerLoad]:
        """Get current load information for all miners."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    r.node_id,
                    -- File assignments and sizes
                    COUNT(CASE WHEN fa.miner1 = r.node_id THEN 1 END) +
                    COUNT(CASE WHEN fa.miner2 = r.node_id THEN 1 END) +
                    COUNT(CASE WHEN fa.miner3 = r.node_id THEN 1 END) +
                    COUNT(CASE WHEN fa.miner4 = r.node_id THEN 1 END) +
                    COUNT(CASE WHEN fa.miner5 = r.node_id THEN 1 END) as file_count,
                    
                    COALESCE(SUM(CASE WHEN fa.miner1 = r.node_id THEN f.size END), 0) +
                    COALESCE(SUM(CASE WHEN fa.miner2 = r.node_id THEN f.size END), 0) +
                    COALESCE(SUM(CASE WHEN fa.miner3 = r.node_id THEN f.size END), 0) +
                    COALESCE(SUM(CASE WHEN fa.miner4 = r.node_id THEN f.size END), 0) +
                    COALESCE(SUM(CASE WHEN fa.miner5 = r.node_id THEN f.size END), 0) as total_size_bytes,
                    
                    -- Miner capacity and health
                    COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                    COALESCE(nm.ipfs_repo_size, 0) as used_storage_bytes,
                    COALESCE(ms.health_score, 100) as health_score,
                    EXTRACT(EPOCH FROM (NOW() - TO_TIMESTAMP(r.registered_at))) / 86400 as days_since_registration
                    
                FROM registration r
                LEFT JOIN file_assignments fa ON (fa.miner1 = r.node_id OR fa.miner2 = r.node_id OR 
                                                 fa.miner3 = r.node_id OR fa.miner4 = r.node_id OR fa.miner5 = r.node_id)
                LEFT JOIN files f ON fa.cid = f.cid
                LEFT JOIN (
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max, ipfs_repo_size
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) nm ON r.node_id = nm.miner_id
                LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                WHERE r.node_type = 'StorageMiner' 
                  AND r.status = 'active'
                  AND COALESCE(ms.health_score, 100) >= $1
                GROUP BY r.node_id, nm.ipfs_storage_max, nm.ipfs_repo_size, ms.health_score, r.registered_at
                ORDER BY file_count DESC, total_size_bytes DESC
            """, self.min_miner_health_score)
            
            miner_loads = []
            for row in rows:
                storage_capacity = float(row['storage_capacity_bytes'])
                used_storage = float(row['used_storage_bytes'])
                available_storage = max(0, storage_capacity - used_storage)
                
                load = MinerLoad(
                    miner_id=row['node_id'],
                    file_count=int(row['file_count']),
                    total_size_bytes=int(row['total_size_bytes']),
                    storage_capacity_bytes=int(storage_capacity),
                    used_storage_bytes=int(used_storage),
                    available_storage_bytes=int(available_storage),
                    health_score=float(row['health_score']),
                    days_since_registration=float(row['days_since_registration'] or 365)
                )
                miner_loads.append(load)
            
            return miner_loads
    
    def analyze_distribution_imbalance(self, miner_loads: List[MinerLoad]) -> Tuple[List[str], List[str], Dict[str, Any]]:
        """
        Analyze file count and size distribution to identify imbalanced miners.
        
        Returns:
            Tuple of (overloaded_miners, underutilized_miners, stats)
        """
        if not miner_loads:
            return [], [], {}
        
        # Calculate statistics
        file_counts = [load.file_count for load in miner_loads if load.file_count > 0]
        size_totals = [load.total_size_bytes for load in miner_loads if load.total_size_bytes > 0]
        
        if not file_counts:
            return [], [], {}
        
        # File count statistics
        avg_files = sum(file_counts) / len(file_counts)
        file_variance = sum((x - avg_files) ** 2 for x in file_counts) / len(file_counts)
        file_stddev = file_variance ** 0.5
        
        # Size statistics  
        avg_size = sum(size_totals) / len(size_totals) if size_totals else 0
        size_variance = sum((x - avg_size) ** 2 for x in size_totals) / len(size_totals) if size_totals else 0
        size_stddev = size_variance ** 0.5
        
        # Identify imbalanced miners
        overloaded_miners = []
        underutilized_miners = []
        
        for load in miner_loads:
            # File count imbalance
            file_z_score = (load.file_count - avg_files) / file_stddev if file_stddev > 0 else 0
            
            # Size imbalance
            size_z_score = (load.total_size_bytes - avg_size) / size_stddev if size_stddev > 0 else 0
            
            # Storage utilization check
            storage_overloaded = load.storage_utilization > self.max_utilization_target
            
            # Determine if overloaded (any condition triggers)
            is_overloaded = (
                file_z_score > self.file_count_threshold or 
                size_z_score > self.size_threshold or
                storage_overloaded
            )
            
            # Determine if underutilized (all conditions must be met)
            is_underutilized = (
                file_z_score < -self.file_count_threshold and
                size_z_score < -self.size_threshold and
                load.storage_utilization < 0.5 and  # Less than 50% used
                load.available_storage_bytes > self.min_available_space_mb * 1024 * 1024  # Has space
            )
            
            if is_overloaded and load.file_count > 0:
                overloaded_miners.append(load.miner_id)
            elif is_underutilized:
                underutilized_miners.append(load.miner_id)
        
        stats = {
            'total_miners': len(miner_loads),
            'miners_with_files': len([l for l in miner_loads if l.file_count > 0]),
            'avg_files_per_miner': avg_files,
            'file_stddev': file_stddev,
            'avg_size_per_miner': avg_size,
            'size_stddev': size_stddev,
            'overloaded_count': len(overloaded_miners),
            'underutilized_count': len(underutilized_miners),
            'max_files': max(file_counts),
            'min_files': min(file_counts),
            'max_size_mb': max(size_totals) / (1024 * 1024) if size_totals else 0,
            'min_size_mb': min(size_totals) / (1024 * 1024) if size_totals else 0
        }
        
        return overloaded_miners, underutilized_miners, stats
    
    async def get_offline_miners_with_files(self) -> List[str]:
        """
        Get miners that have files assigned but are currently offline or unhealthy.
        This is the targeted approach - only rebalance files from miners that are actually offline.
        """
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT miner_id FROM (
                    SELECT fa.miner1 as miner_id FROM file_assignments fa
                    WHERE fa.miner1 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner1 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                    UNION
                    SELECT fa.miner2 as miner_id FROM file_assignments fa
                    WHERE fa.miner2 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner2 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                    UNION
                    SELECT fa.miner3 as miner_id FROM file_assignments fa
                    WHERE fa.miner3 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner3 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                    UNION
                    SELECT fa.miner4 as miner_id FROM file_assignments fa
                    WHERE fa.miner4 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner4 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                    UNION
                    SELECT fa.miner5 as miner_id FROM file_assignments fa
                    WHERE fa.miner5 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner5 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                ) offline_miners
            """)
            
            offline_miners = [row['miner_id'] for row in rows]
            
            if offline_miners:
                logger.info(f"🚨 Found {len(offline_miners)} offline miners with file assignments:")
                for miner_id in offline_miners[:5]:  # Show first 5
                    logger.info(f"   - {miner_id} (offline/unhealthy)")
                
                if len(offline_miners) > 5:
                    logger.info(f"   ... and {len(offline_miners) - 5} more")
            else:
                logger.info("✅ No offline miners found with file assignments")
            
            return offline_miners

    async def get_files_from_offline_miners(self, offline_miners: List[str]) -> List[RebalanceCandidate]:
        """Get all files assigned to offline miners for rebalancing."""
        if not offline_miners:
            return []
        
        # Create placeholders for the query
        placeholders = ','.join(f'${i+1}' for i in range(len(offline_miners)))
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT 
                    fa.cid,
                    f.name as filename,
                    f.size as size_bytes,
                    fa.owner,
                    fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5,
                    -- Priority based on file size - larger files get higher priority for faster network recovery
                    CASE 
                        WHEN f.size > 100000000 THEN 10.0  -- Large files (>100MB) - critical for storage balance
                        WHEN f.size > 10000000 THEN 8.0    -- Medium files (>10MB) - important
                        WHEN f.size > 1000000 THEN 5.0     -- Small files (>1MB) - normal priority
                        ELSE 3.0                           -- Tiny files - lower priority but still move
                    END as priority
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE (fa.miner1 IN ({placeholders}) OR fa.miner2 IN ({placeholders}) OR 
                       fa.miner3 IN ({placeholders}) OR fa.miner4 IN ({placeholders}) OR 
                       fa.miner5 IN ({placeholders}))
                  AND f.size IS NOT NULL
                  AND f.size > 0
                ORDER BY priority DESC, f.size DESC
                LIMIT $100
            """, *offline_miners, self.max_files_per_batch)
            
            candidates = []
            for row in rows:
                current_miners = [
                    row['miner1'], row['miner2'], row['miner3'], 
                    row['miner4'], row['miner5']
                ]
                current_miners = [m for m in current_miners if m is not None]
                
                # Find which offline miner has this file
                offline_miner = None
                for miner in current_miners:
                    if miner in offline_miners:
                        offline_miner = miner
                        break
                
                if offline_miner:
                    size_mb = row['size_bytes'] / (1024 * 1024)
                    reason = f"Offline miner {offline_miner[:12]}... ({size_mb:.1f}MB file)"
                    
                    candidate = RebalanceCandidate(
                        cid=row['cid'],
                        filename=row['filename'] or 'Unknown',
                        size_bytes=row['size_bytes'],
                        owner=row['owner'],
                        current_miners=current_miners,
                        overloaded_miner=offline_miner,  # Using this field for offline miner
                        reason=reason,
                        priority=row['priority']
                    )
                    candidates.append(candidate)
            
            # Sort by priority (highest first) - prioritize larger files for faster recovery
            candidates.sort(key=lambda c: c.priority, reverse=True)
            logger.info(f"📁 Found {len(candidates)} files to move from offline miners")
            
            if candidates:
                # Log some examples
                for candidate in candidates[:3]:
                    size_mb = candidate.size_bytes / (1024 * 1024)
                    logger.info(f"   Priority {candidate.priority}: {candidate.filename} ({size_mb:.1f}MB) from {candidate.overloaded_miner[:12]}...")
            
            return candidates
    
    def select_target_miners(self, candidate: RebalanceCandidate, miner_loads: List[MinerLoad], 
                           underutilized_miners: List[str]) -> List[str]:
        """Select the best target miners for a file rebalancing."""
        # Filter suitable miners
        suitable_miners = []
        required_space = candidate.size_bytes + (candidate.size_bytes * 0.2)  # 20% safety margin
        
        for load in miner_loads:
            # Skip if already assigned to this file
            if load.miner_id in candidate.current_miners:
                continue
            
            # Must have enough space
            if load.available_storage_bytes < required_space:
                continue
            
            # Must be healthy
            if load.health_score < self.min_miner_health_score:
                continue
            
            # Prefer underutilized miners
            priority_boost = 2.0 if load.miner_id in underutilized_miners else 1.0
            
            # Calculate selection score
            space_score = min(1.0, load.available_storage_bytes / (required_space * 5))  # Prefer miners with plenty of space
            health_score = load.health_score / 100.0
            utilization_score = 1.0 - load.storage_utilization  # Prefer less utilized
            
            # Boost newer miners slightly
            newness_boost = 1.2 if load.days_since_registration <= 30 else 1.0
            
            total_score = (space_score * 0.4 + health_score * 0.3 + utilization_score * 0.3) * priority_boost * newness_boost
            
            suitable_miners.append((load.miner_id, total_score))
        
        # Sort by score and return top candidate
        suitable_miners.sort(key=lambda x: x[1], reverse=True)
        return [miner_id for miner_id, _ in suitable_miners[:1]]  # Return only the best one
    
    async def plan_rebalancing_actions(self, candidates: List[RebalanceCandidate], 
                                     miner_loads: List[MinerLoad], 
                                     underutilized_miners: List[str]) -> List[RebalanceAction]:
        """Plan specific rebalancing actions."""
        actions = []
        moves_per_miner = {}  # Track moves per miner to respect limits
        
        for candidate in candidates:
            # Respect per-miner move limits
            from_miner = candidate.overloaded_miner
            if moves_per_miner.get(from_miner, 0) >= self.max_moves_per_miner:
                continue
            
            # Find target miners
            target_miners = self.select_target_miners(candidate, miner_loads, underutilized_miners)
            
            if not target_miners:
                logger.debug(f"No suitable target miners for file {candidate.cid}")
                continue
            
            target_miner = target_miners[0]
            
            # Calculate improvements
            file_count_improvement = 1  # Moving 1 file away from overloaded miner
            size_improvement = candidate.size_bytes  # Bytes moved away from overloaded miner
            
            action = RebalanceAction(
                cid=candidate.cid,
                filename=candidate.filename,
                size_bytes=candidate.size_bytes,
                from_miner=from_miner,
                to_miner=target_miner,
                reason=candidate.reason,
                file_count_improvement=file_count_improvement,
                size_improvement_bytes=size_improvement
            )
            
            actions.append(action)
            moves_per_miner[from_miner] = moves_per_miner.get(from_miner, 0) + 1
            
            # Update miner loads for next iteration
            for load in miner_loads:
                if load.miner_id == from_miner:
                    load.file_count -= 1
                    load.total_size_bytes -= candidate.size_bytes
                elif load.miner_id == target_miner:
                    load.file_count += 1
                    load.total_size_bytes += candidate.size_bytes
                    load.available_storage_bytes -= candidate.size_bytes
        
        return actions
    
    async def queue_rebalancing_task(self, action: RebalanceAction, current_epoch: int) -> None:
        """Queue a file rebalancing task to RabbitMQ."""
        # Create new miner assignment by replacing the from_miner with to_miner
        assignment_data = {
            'type': 'rebalancing',
            'cid': action.cid,
            'filename': action.filename,
            'size_bytes': action.size_bytes,
            'from_miner': action.from_miner,
            'to_miner': action.to_miner,
            'reason': action.reason,
            'epoch': current_epoch,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        message_body = json.dumps(assignment_data).encode()
        
        await self.rabbitmq_channel.default_exchange.publish(
            Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=self.queue_name
        )
        
        logger.debug(f"Queued rebalancing task for file {action.cid} from {action.from_miner} to {action.to_miner}")
    
    async def should_run_rebalancing(self) -> bool:
        """Check if enough time has passed since last rebalancing."""
        try:
            async with self.db_pool.acquire() as conn:
                last_run = await conn.fetchval("""
                    SELECT MAX(created_at) FROM system_events 
                    WHERE event_type = 'network_rebalancing' 
                    AND created_at > NOW() - INTERVAL '24 hours'
                """)
                
                if last_run is None:
                    return True  # Never run before
                
                hours_since = (datetime.utcnow() - last_run).total_seconds() / 3600
                return hours_since >= self.rebalance_interval_hours
        except Exception:
            return True  # Default to allowing rebalancing if check fails
    
    async def record_rebalancing_run(self, stats: Dict[str, Any], actions_count: int) -> None:
        """Record that rebalancing was performed."""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO system_events (event_type, event_data, created_at)
                    VALUES ('network_rebalancing', $1, NOW())
                """, json.dumps({
                    'actions_planned': actions_count,
                    'stats': stats,
                    'timestamp': datetime.utcnow().isoformat()
                }))
        except Exception as e:
            logger.warning(f"Failed to record rebalancing run: {e}")
    
    async def perform_network_rebalancing(self) -> None:
        """
        TARGETED rebalancing function - only moves files from offline/unhealthy miners.
        This ensures rebalancing only happens when actually needed, not on every validator cycle.
        """
        try:
            logger.info("🔍 Starting TARGETED network rebalancing (offline miners only)...")
            
            # STEP 1: Check for offline miners with files
            offline_miners = await self.get_offline_miners_with_files()
            
            if not offline_miners:
                logger.info("✅ No offline miners found with file assignments - no rebalancing needed")
                return
            
            # STEP 2: Check if rebalancing should run (cooldown period)
            if not await self.should_run_rebalancing():
                logger.info(f"⏳ Skipping rebalancing - cooldown period not reached ({self.rebalance_interval_hours}h)")
                logger.info(f"   Found {len(offline_miners)} offline miners but waiting for cooldown")
                return
            
            logger.warning(f"🚨 OFFLINE MINERS DETECTED: {len(offline_miners)} miners need file redistribution")
            
            # Get current epoch
            current_epoch = self.get_current_epoch()
            
            # Get all miner loads for target selection
            miner_loads = await self.get_miner_loads()
            if not miner_loads:
                logger.error("❌ No miner loads found - cannot perform rebalancing")
                return
            
            # Filter to get healthy miners for targets
            healthy_miners = [load for load in miner_loads if load.health_score >= self.min_miner_health_score]
            
            logger.info(f"📊 Rebalancing Context:")
            logger.info(f"   Offline miners with files: {len(offline_miners)}")
            logger.info(f"   Available healthy miners: {len(healthy_miners)}")
            logger.info(f"   Target health threshold: {self.min_miner_health_score}")
            
            if len(healthy_miners) < 5:  # Need at least 5 healthy miners for file assignments
                logger.error(f"❌ Insufficient healthy miners ({len(healthy_miners)}) for safe rebalancing")
                logger.error("   Need at least 5 healthy miners to maintain file redundancy")
                return
            
            # STEP 3: Get files from offline miners that need redistribution
            candidates = await self.get_files_from_offline_miners(offline_miners)
            
            if not candidates:
                logger.info("📁 No files found on offline miners - rebalancing complete")
                await self.record_rebalancing_run({
                    'offline_miners': len(offline_miners),
                    'healthy_miners': len(healthy_miners),
                    'files_found': 0
                }, 0)
                return
            
            logger.warning(f"📁 Found {len(candidates)} files that need redistribution from offline miners")
            
            # STEP 4: Plan rebalancing actions (target healthy miners)
            underutilized_miners = [load.miner_id for load in healthy_miners 
                                  if load.storage_utilization < 0.5 and 
                                     load.available_storage_bytes > self.min_available_space_mb * 1024 * 1024]
            
            logger.info(f"🎯 Target miners for redistribution: {len(underutilized_miners)} underutilized, {len(healthy_miners)} total healthy")
            
            actions = await self.plan_rebalancing_actions(candidates, miner_loads, underutilized_miners)
            
            if not actions:
                logger.warning("⚠️ No viable rebalancing actions planned - may need manual intervention")
                logger.warning("   Check if healthy miners have sufficient storage capacity")
                await self.record_rebalancing_run({
                    'offline_miners': len(offline_miners),
                    'healthy_miners': len(healthy_miners),
                    'files_found': len(candidates),
                    'viable_actions': 0
                }, 0)
                return
            
            # STEP 5: Execute rebalancing actions
            logger.warning(f"🔄 EXECUTING {len(actions)} targeted rebalancing actions for offline miners:")
            
            total_files_moved = 0
            total_size_moved = 0
            miners_helped = set()
            
            # Queue rebalancing tasks
            for action in actions:
                size_mb = action.size_bytes / (1024 * 1024)
                logger.warning(f"   📁 {action.filename} ({size_mb:.1f}MB): OFFLINE {action.from_miner[:12]}... → HEALTHY {action.to_miner[:12]}...")
                
                await self.queue_rebalancing_task(action, current_epoch)
                total_files_moved += 1
                total_size_moved += action.size_bytes
                miners_helped.add(action.from_miner)
            
            logger.warning(f"🎉 TARGETED Rebalancing Summary:")
            logger.warning(f"   Files redistributed from offline miners: {total_files_moved}")
            logger.warning(f"   Total size redistributed: {total_size_moved/1024/1024:.1f} MB")
            logger.warning(f"   Offline miners assisted: {len(miners_helped)}")
            logger.warning(f"   Estimated processing time: 5-10 minutes")
            logger.warning(f"   🎯 This was TARGETED rebalancing - only for offline miners!")
            
            # Record the targeted rebalancing run
            await self.record_rebalancing_run({
                'type': 'targeted_offline_miners',
                'offline_miners': len(offline_miners),
                'healthy_miners': len(healthy_miners),
                'files_found': len(candidates),
                'files_moved': total_files_moved,
                'size_moved_mb': total_size_moved/1024/1024,
                'miners_helped': len(miners_helped)
            }, len(actions))
            
        except Exception as e:
            logger.error(f"❌ Error during TARGETED network rebalancing: {e}")
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
    processor = NetworkRebalancingProcessor()
    
    try:
        # Initialize database pool
        await init_db_pool()
        processor.db_pool = get_db_pool()
        logger.info("Database connection pool initialized")
        
        # Connect to services
        processor.connect_substrate()
        await processor.connect_rabbitmq()
        
        # Perform network rebalancing
        await processor.perform_network_rebalancing()
        
        logger.info("Completed network rebalancing processing")
        
    except Exception as e:
        logger.error(f"Error in rebalancing processor: {e}")
        raise
    finally:
        await processor.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 