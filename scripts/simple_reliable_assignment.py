#!/usr/bin/env python3
"""
Simple Reliable File Assignment v2.0 - Fair Distribution

A straightforward assignment system that:
1. Assigns per file (not batch)
2. Ensures broad network distribution 
3. Uses simple criteria: 1+ day old miners with capacity
4. Avoids complex scoring that causes issues
5. FAIR DISTRIBUTION: Prevents "rich get richer" bias

Enhanced Space Checking v2.0:
- Realistic space thresholds (100MB minimum vs previous 100KB)
- File-size specific capacity checking with buffer space
- Space checking used as QUALIFICATION FILTER (not preference bias)
- Fair distribution among qualified miners (no storage favoritism)
- Comprehensive space logging and verification
- Fills ANY NULL miner columns (not just completely empty files)

Fair Distribution Principles:
- Miners must have adequate space for the file (qualification filter)
- Among qualified miners, prefer fair distribution over storage abundance
- Balances assignments across the network to prevent centralization
- Uses health data, assignment counts, and randomness for selection
"""

import asyncio
import logging
import os
import sys
import random
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleFileAssigner:
    def __init__(self, db_pool=None):
        self.db_pool = db_pool  # Accept external db_pool
        self.replicas_per_file = 5
        self.min_miner_age_days = 1
        self.assignment_count = {}  # Track assignments per miner in this session
        
    async def initialize(self):
        """Initialize database connection if not provided."""
        try:
            if self.db_pool is None:
                # Only initialize if not provided externally
                from app.db.connection import init_db_pool, get_db_pool
                await init_db_pool()
                self.db_pool = await get_db_pool()
                logger.info("✅ Database connection initialized")
            else:
                logger.info("✅ Using provided database pool")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize database: {e}")
            return False
    
    async def validate_health_data(self):
        """Validate that we have fresh health data before assignment."""
        try:
            async with self.db_pool.acquire() as conn:
                # Check for recent health data (within last 2 hours)
                recent_health = await conn.fetchval("""
                    SELECT COUNT(*) FROM miner_epoch_health 
                    WHERE last_activity_at >= NOW() - INTERVAL '2 hours'
                """)
                
                if recent_health == 0:
                    # Check for fallback health data (within last 8 hours)
                    fallback_health = await conn.fetchval("""
                        SELECT COUNT(*) FROM miner_epoch_health 
                        WHERE last_activity_at >= NOW() - INTERVAL '8 hours'
                    """)
                    
                    if fallback_health == 0:
                        logger.error("🚨 CRITICAL: No health data found (recent or fallback)!")
                        logger.error("   Assignments require some health data to determine miner availability")
                        return False
                    else:
                        logger.warning(f"⚠️ No recent health data, using {fallback_health} miners from fallback data (up to 8 hours old)")
                        logger.warning("   Assignment quality may be reduced but will proceed")
                        return True
                else:
                    logger.info(f"✅ Found health data for {recent_health} miners (within 2 hours)")
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Error validating health data: {e}")
            return False
    
    async def get_reliable_miners(self):
        """Get reliable miners that are 1+ days old with capacity and fresh health data."""
        try:
            async with self.db_pool.acquire() as conn:
                # Calculate cutoff block number (1 day = ~14,400 blocks at 6 seconds per block)
                blocks_per_day = 14400  # 24 * 60 * 60 / 6 seconds per block
                cutoff_blocks = self.min_miner_age_days * blocks_per_day
                
                # Get current block number to calculate cutoff
                try:
                    from app.utils.epoch_validator import connect_substrate
                    substrate = connect_substrate()
                    current_block = substrate.get_block_number()
                    cutoff_block = current_block - cutoff_blocks
                    substrate.close()
                    logger.debug(f"Current block: {current_block}, cutoff block: {cutoff_block} (miners must be registered before block {cutoff_block})")
                except Exception as e:
                    logger.warning(f"Could not get current block number: {e}, using fallback")
                    # Fallback: use a reasonable cutoff block (assume we're around block 800000)
                    cutoff_block = 800000 - cutoff_blocks
                
                # Enhanced query that prefers miners with recent health data
                miners = await conn.fetch("""
                    SELECT 
                        r.node_id,
                        r.registered_at,
                        COALESCE(nm.ipfs_storage_max, 1000000000) as storage_max,
                        COALESCE(nm.ipfs_repo_size, 0) as storage_used,
                        COALESCE(ms.health_score, 100) as health_score,
                        COALESCE(ms.total_files_pinned, 0) as files_pinned,
                        meh.last_activity_at as health_updated,
                        -- Prefer miners with recent health checks (more tolerant thresholds)
                        CASE 
                            WHEN meh.last_activity_at >= NOW() - INTERVAL '1 hour' THEN 100
                            WHEN meh.last_activity_at >= NOW() - INTERVAL '4 hours' THEN 75
                            WHEN meh.last_activity_at >= NOW() - INTERVAL '8 hours' THEN 50
                            WHEN meh.last_activity_at >= NOW() - INTERVAL '1 day' THEN 25
                            WHEN meh.last_activity_at IS NULL THEN 10  -- Miners with no health data get low priority
                            ELSE 5
                        END as health_freshness_score
                    FROM registration r
                    LEFT JOIN (
                        SELECT DISTINCT ON (miner_id) 
                            miner_id, ipfs_storage_max, ipfs_repo_size
                        FROM node_metrics 
                        ORDER BY miner_id, block_number DESC
                    ) nm ON r.node_id = nm.miner_id
                    LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                    LEFT JOIN (
                        -- Get most recent health data for each miner (even if old)
                        SELECT DISTINCT ON (node_id) 
                            node_id, last_activity_at
                        FROM miner_epoch_health 
                        ORDER BY node_id, last_activity_at DESC
                    ) meh ON r.node_id = meh.node_id
                    WHERE r.node_type = 'StorageMiner' 
                      AND r.status = 'active'
                      AND r.registered_at <= $1
                      AND COALESCE(ms.health_score, 100) >= $2
                    ORDER BY 
                        health_freshness_score DESC,  -- Prefer fresh health data
                        RANDOM()  -- Random order for better distribution
                """, cutoff_block, 50)
                
                # Enhanced capacity filtering with realistic thresholds
                reliable_miners = []
                fresh_health_count = 0
                fallback_health_count = 0
                no_health_count = 0
                insufficient_space_count = 0
                
                # Calculate minimum space requirements (more realistic thresholds)
                min_free_space_mb = 100  # Minimum 100MB free space
                min_free_space_bytes = min_free_space_mb * 1024 * 1024
                
                for miner in miners:
                    available_space = miner['storage_max'] - miner['storage_used']
                    available_space_mb = available_space / (1024 * 1024)
                    
                    # Enhanced space checking with realistic thresholds
                    if available_space > min_free_space_bytes:  # At least 100MB available
                        # Calculate age in blocks instead of days
                        try:
                            age_blocks = current_block - miner['registered_at']
                            age_days = age_blocks / blocks_per_day
                        except:
                            age_days = 999  # Fallback for very old miners
                        
                        reliable_miners.append({
                            'node_id': miner['node_id'],
                            'health_score': miner['health_score'],
                            'health_freshness_score': miner['health_freshness_score'],
                            'files_pinned': miner['files_pinned'],
                            'available_space': available_space,
                            'available_space_mb': available_space_mb,
                            'age_days': age_days,
                            'health_updated': miner['health_updated']
                        })
                        
                        # Count miners by health data quality
                        if miner['health_freshness_score'] >= 75:  # Within 4 hours
                            fresh_health_count += 1
                        elif miner['health_freshness_score'] >= 25:  # Within 1 day
                            fallback_health_count += 1
                        else:
                            no_health_count += 1
                    else:
                        insufficient_space_count += 1

                logger.info(f"✅ Found {len(reliable_miners)} reliable miners (1+ day old with capacity)")
                logger.info(f"   💾 Space requirement: minimum {min_free_space_mb}MB available")
                logger.info(f"   {fresh_health_count} miners have fresh health data (within 4 hours)")
                logger.info(f"   {fallback_health_count} miners have fallback health data (within 1 day)")
                logger.info(f"   {no_health_count} miners have no recent health data")
                
                if insufficient_space_count > 0:
                    logger.info(f"   ⚠️ {insufficient_space_count} miners excluded due to insufficient space (<{min_free_space_mb}MB)")
                
                # Health data quality warnings
                if fresh_health_count == 0 and fallback_health_count == 0:
                    logger.warning(f"⚠️ No miners with recent health data - using miners without health checks")
                    logger.warning(f"   Assignment quality will be reduced but network will continue functioning")
                elif fresh_health_count < len(reliable_miners) * 0.3:
                    logger.warning(f"⚠️ Only {fresh_health_count}/{len(reliable_miners)} miners have fresh health data")
                    logger.warning(f"   Using fallback health data for remaining miners")
                
                # Log storage capacity summary
                if reliable_miners:
                    total_available = sum(m['available_space'] for m in reliable_miners)
                    avg_available = total_available / len(reliable_miners) / (1024 * 1024 * 1024)  # GB
                    min_available = min(m['available_space_mb'] for m in reliable_miners)
                    max_available = max(m['available_space_mb'] for m in reliable_miners)
                    
                    logger.info(f"   📊 Storage capacity: avg {avg_available:.1f}GB, range {min_available:.0f}MB - {max_available:.0f}MB available")
                
                return reliable_miners
                
        except Exception as e:
            logger.error(f"❌ Error getting reliable miners: {e}")
            return []
    
    def select_miners_simple(self, miners, count=5):
        """
        Fair miner selection with network distribution priority.
        
        Selection Criteria (in order):
        1. Health data freshness (prefer recent health checks)
        2. Assignment balance (fewer assignments this session)
        3. Load balance (fewer total files pinned)
        4. Random distribution (prevents bias and "rich get richer")
        
        Space checking is done as a qualification filter (before this method),
        NOT as a preference factor to ensure fair network distribution.
        """
        if len(miners) <= count:
            return [m['node_id'] for m in miners]
        
        # Fair distribution strategy (prevents "rich get richer"):
        # 1. Prefer miners with fresh health data (network reliability)
        # 2. Balance assignments across miners (fair distribution)
        # 3. Distribute load evenly (network efficiency)
        # 4. Add randomness to prevent systematic bias
        
        # Update assignment counts
        for miner in miners:
            node_id = miner['node_id']
            miner['session_assignments'] = self.assignment_count.get(node_id, 0)
        
        # Sort by health freshness, assignment count, files pinned, then random (FAIR DISTRIBUTION)
        miners_sorted = sorted(miners, key=lambda m: (
            -m['health_freshness_score'],     # Higher health freshness first
            m['session_assignments'],         # Fewer assignments this session (FAIR DISTRIBUTION)
            m['files_pinned'],               # Fewer total files (FAIR DISTRIBUTION)
            random.random()                  # Random factor for distribution (PREVENTS "RICH GET RICHER")
        ))
        
        # Select the best distributed miners
        selected = miners_sorted[:count]
        selected_ids = [m['node_id'] for m in selected]
        
        # Update assignment counts
        for node_id in selected_ids:
            self.assignment_count[node_id] = self.assignment_count.get(node_id, 0) + 1
        
        # Log health data quality for selected miners
        fresh_count = sum(1 for m in selected if m['health_freshness_score'] >= 75)
        if fresh_count < len(selected):
            logger.info(f"   Selected {fresh_count}/{len(selected)} miners with fresh health data")
        
        return selected_ids
    
    async def assign_unassigned_files(self):
        """
        Public method for epoch orchestrator to fill incomplete file assignments.
        Fills any NULL miner slots in file_assignments table.
        Returns True if successful, False otherwise.
        """
        try:
            # Validate health data first
            health_valid = await self.validate_health_data()
            if not health_valid:
                logger.warning("⚠️ Proceeding with assignment despite stale health data")
            
            # Use the existing fix_empty_assignments method
            success, count = await self.fix_empty_assignments()
            
            if success:
                logger.info(f"✅ Successfully filled incomplete assignments for {count} files")
                return True
            else:
                logger.error(f"❌ Assignment partially failed - fixed {count} files")
                return count > 0  # Return True if we fixed at least some files
                
        except Exception as e:
            logger.error(f"❌ Error in assign_unassigned_files: {e}")
            return False
    
    async def assign_miners_to_file(self, cid, file_size, owner, filename=""):
        """Assign miners to fill NULL slots in a file assignment (preserves existing miners)."""
        try:
            logger.info(f"🔧 Filling missing miners for file: {filename or cid[:16]}...")
            
            # First, get current assignment to see what we already have
            async with self.db_pool.acquire() as conn:
                current_assignment = await conn.fetchrow("""
                    SELECT miner1, miner2, miner3, miner4, miner5
                    FROM file_assignments 
                    WHERE cid = $1
                """, cid)
                
                if not current_assignment:
                    logger.error(f"❌ File not found in assignments table: {cid}")
                    return False
                
                # Get current miners (non-NULL)
                current_miners = [
                    m for m in [current_assignment['miner1'], current_assignment['miner2'], 
                               current_assignment['miner3'], current_assignment['miner4'], 
                               current_assignment['miner5']]
                    if m is not None
                ]
                
                missing_count = 5 - len(current_miners)
                
                if missing_count == 0:
                    logger.info(f"✅ File already has complete assignment (5/5 miners)")
                    return True
                
                logger.info(f"📋 Current assignment: {len(current_miners)}/5 miners, need {missing_count} more")
                for i, miner in enumerate(current_miners):
                    logger.info(f"   Existing {i+1}. {miner}")
                
                # Get reliable miners
                miners = await self.get_reliable_miners()
                if len(miners) == 0:
                    logger.error("❌ No reliable miners available!")
                    return False
                
                # Filter miners to ensure they have enough space for this specific file
                file_size_mb = file_size / (1024 * 1024) if file_size else 0
                buffer_space_mb = 50  # Extra 50MB buffer space
                required_space_bytes = file_size + (buffer_space_mb * 1024 * 1024)
                
                space_sufficient_miners = []
                insufficient_space_miners = 0
                
                for miner in miners:
                    if miner['available_space'] >= required_space_bytes:
                        space_sufficient_miners.append(miner)
                    else:
                        insufficient_space_miners += 1
                
                logger.info(f"📊 File size: {file_size_mb:.1f}MB (+ {buffer_space_mb}MB buffer)")
                logger.info(f"📊 Miners with sufficient space: {len(space_sufficient_miners)}/{len(miners)}")
                if insufficient_space_miners > 0:
                    logger.info(f"   ⚠️ {insufficient_space_miners} miners excluded due to insufficient space for this file")
                
                # Filter out miners already assigned to this file
                available_miners = [m for m in space_sufficient_miners if m['node_id'] not in current_miners]
                
                logger.info(f"📊 Available miners: {len(available_miners)} (after excluding already assigned)")
                
                if len(available_miners) < missing_count:
                    logger.warning(f"⚠️ Only {len(available_miners)} available miners, need {missing_count}")
                
                # Select new miners using simple distribution
                new_miners = self.select_miners_simple(available_miners, missing_count)
                
                if new_miners:
                    logger.info(f"📋 Selected {len(new_miners)} new miners with space verification:")
                    for i, miner_id in enumerate(new_miners):
                        # Find the miner data to show space info
                        miner_data = next((m for m in available_miners if m['node_id'] == miner_id), None)
                        assignments = self.assignment_count.get(miner_id, 1)
                        
                        if miner_data:
                            space_gb = miner_data['available_space_mb'] / 1024
                            logger.info(f"   New {i+1}. {miner_id} (space: {space_gb:.1f}GB, assignments: {assignments})")
                        else:
                            logger.info(f"   New {i+1}. {miner_id} (assignments: {assignments})")
                
                # Build final assignment preserving existing miners and adding new ones
                final_miners = list(current_assignment)  # Start with current state
                next_slot = 0
                
                # Fill NULL slots with new miners
                for new_miner in new_miners:
                    # Find next NULL slot
                    while next_slot < 5 and final_miners[next_slot] is not None:
                        next_slot += 1
                    
                    if next_slot < 5:
                        final_miners[next_slot] = new_miner
                        next_slot += 1
                
                # Update database with preserved + new assignments
                async with conn.transaction():
                    result = await conn.execute("""
                        UPDATE file_assignments
                        SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cid = $1
                    """, cid, final_miners[0], final_miners[1], final_miners[2], 
                        final_miners[3], final_miners[4])
                    
                    if result == "UPDATE 0":
                        logger.error(f"❌ Failed to update file assignment: {cid}")
                        return False
                    
                    # Update miner stats for NEW miners only
                    for miner_id in new_miners:
                        if miner_id:
                            await conn.execute("""
                                INSERT INTO miner_stats (
                                    node_id, total_files_pinned, total_files_size_bytes, updated_at
                                )
                                VALUES ($1, 1, $2, NOW())
                                ON CONFLICT (node_id) DO UPDATE SET
                                    total_files_pinned = miner_stats.total_files_pinned + 1,
                                    total_files_size_bytes = miner_stats.total_files_size_bytes + $2,
                                    updated_at = NOW()
                            """, miner_id, file_size)
                
                # Final count
                final_count = sum(1 for m in final_miners if m is not None)
                logger.info(f"✅ Assignment updated: {len(current_miners)}/5 → {final_count}/5 miners")
                return True
            
        except Exception as e:
            logger.error(f"❌ Error assigning miners to file: {e}")
            return False
    
    async def fix_empty_assignments(self):
        """Fix all files with incomplete miner assignments (any NULL miner columns)."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get files with ANY missing miner assignments (not just completely empty)
                incomplete_files = await conn.fetch("""
                    SELECT 
                        fa.cid, fa.owner, f.name, f.size,
                        fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL 
                      OR fa.miner4 IS NULL OR fa.miner5 IS NULL
                    ORDER BY f.size ASC  -- Process smaller files first
                """)
                
                if not incomplete_files:
                    logger.info("✅ No files with missing miner assignments found")
                    return True, 0
                
                logger.info(f"📋 Found {len(incomplete_files)} files with incomplete assignments")
                
                success_count = 0
                for i, file_info in enumerate(incomplete_files, 1):
                    # Count how many miners are already assigned
                    current_miners = [
                        m for m in [file_info['miner1'], file_info['miner2'], 
                                   file_info['miner3'], file_info['miner4'], file_info['miner5']]
                        if m is not None
                    ]
                    
                    missing_miners = 5 - len(current_miners)
                    
                    logger.info(f"\n{'='*50}")
                    logger.info(f"Processing {i}/{len(incomplete_files)}: {file_info['name']}")
                    logger.info(f"   Current miners: {len(current_miners)}/5, need {missing_miners} more")
                    
                    success = await self.assign_miners_to_file(
                        file_info['cid'], 
                        file_info['size'], 
                        file_info['owner'],
                        file_info['name']
                    )
                    
                    if success:
                        success_count += 1
                    
                    # Brief pause to avoid overwhelming the system
                    if i % 10 == 0:
                        await asyncio.sleep(1)
                
                logger.info(f"\n{'='*50}")
                logger.info(f"✅ Assignment complete: {success_count}/{len(incomplete_files)} files fixed")
                
                # Show final summary
                remaining_incomplete = await conn.fetchval("""
                    SELECT COUNT(*) FROM file_assignments 
                    WHERE miner1 IS NULL OR miner2 IS NULL OR miner3 IS NULL 
                      OR miner4 IS NULL OR miner5 IS NULL
                """)
                
                if remaining_incomplete == 0:
                    logger.info("🎉 ALL files now have complete 5-miner assignments!")
                else:
                    logger.warning(f"⚠️ {remaining_incomplete} files still need miner assignments")
                
                return success_count == len(incomplete_files), success_count
                
        except Exception as e:
            logger.error(f"❌ Error fixing incomplete assignments: {e}")
            return False, 0
    
    async def fix_specific_file(self, cid):
        """Fix assignment for a specific file."""
        try:
            async with self.db_pool.acquire() as conn:
                file_info = await conn.fetchrow("""
                    SELECT 
                        f.cid, f.name, f.size,
                        fa.owner, fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5
                    FROM files f
                    JOIN file_assignments fa ON f.cid = fa.cid
                    WHERE f.cid = $1
                """, cid)
                
                if not file_info:
                    logger.error(f"❌ File not found: {cid}")
                    return False
                
                # Check current assignment
                assigned_miners = [
                    m for m in [file_info['miner1'], file_info['miner2'], 
                               file_info['miner3'], file_info['miner4'], file_info['miner5']]
                    if m is not None
                ]
                
                logger.info(f"📁 File: {file_info['name']}")
                logger.info(f"   Current miners: {len(assigned_miners)}/5")
                
                if len(assigned_miners) >= 5:
                    logger.info("✅ File already has full assignment")
                    return True
                
                # Assign miners
                success = await self.assign_miners_to_file(
                    cid, file_info['size'], file_info['owner'], file_info['name']
                )
                
                return success
                
        except Exception as e:
            logger.error(f"❌ Error fixing specific file: {e}")
            return False
    
    async def check_assignment_distribution(self):
        """Check how assignments are distributed across miners and analyze fairness."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get assignment distribution
                distribution = await conn.fetch("""
                    WITH miner_assignments AS (
                        SELECT node_id, COUNT(*) as assignment_count
                        FROM (
                            SELECT miner1 as node_id FROM file_assignments WHERE miner1 IS NOT NULL
                            UNION ALL
                            SELECT miner2 as node_id FROM file_assignments WHERE miner2 IS NOT NULL
                            UNION ALL
                            SELECT miner3 as node_id FROM file_assignments WHERE miner3 IS NOT NULL
                            UNION ALL
                            SELECT miner4 as node_id FROM file_assignments WHERE miner4 IS NOT NULL
                            UNION ALL
                            SELECT miner5 as node_id FROM file_assignments WHERE miner5 IS NOT NULL
                        ) assignments
                        GROUP BY node_id
                        ORDER BY assignment_count DESC
                    )
                    SELECT 
                        COUNT(*) as total_miners,
                        AVG(assignment_count) as avg_assignments,
                        MIN(assignment_count) as min_assignments,
                        MAX(assignment_count) as max_assignments,
                        STDDEV(assignment_count) as stddev_assignments
                    FROM miner_assignments
                """)
                
                if distribution:
                    stats = distribution[0]
                    avg_assignments = float(stats['avg_assignments'] or 0)
                    stddev_assignments = float(stats['stddev_assignments'] or 0)
                    max_assignments = int(stats['max_assignments'] or 0)
                    min_assignments = int(stats['min_assignments'] or 0)
                    
                    logger.info("📊 Assignment Distribution:")
                    logger.info(f"   Total miners with assignments: {stats['total_miners']}")
                    logger.info(f"   Average assignments per miner: {avg_assignments:.1f}")
                    logger.info(f"   Min assignments: {min_assignments}")
                    logger.info(f"   Max assignments: {max_assignments}")
                    logger.info(f"   Standard deviation: {stddev_assignments:.1f}")
                    
                    # Fairness analysis
                    if stats['total_miners'] > 0:
                        assignment_range = max_assignments - min_assignments
                        fairness_ratio = max_assignments / max(avg_assignments, 1)
                        
                        logger.info("\n📈 Fairness Analysis:")
                        logger.info(f"   Assignment range: {assignment_range} (max - min)")
                        logger.info(f"   Fairness ratio: {fairness_ratio:.2f} (max / avg)")
                        
                        # Fairness assessment
                        if fairness_ratio <= 1.5:
                            logger.info("   ✅ EXCELLENT fairness - well distributed assignments")
                        elif fairness_ratio <= 2.0:
                            logger.info("   ✅ GOOD fairness - reasonable distribution")
                        elif fairness_ratio <= 3.0:
                            logger.info("   ⚠️ MODERATE fairness - some concentration detected")
                        else:
                            logger.warning("   🚨 POOR fairness - significant assignment concentration!")
                            logger.warning("   This may indicate 'rich get richer' bias - check selection logic")
                
                # Show top and bottom miners with storage info
                top_miners = await conn.fetch("""
                    WITH miner_assignments AS (
                        SELECT node_id, COUNT(*) as assignment_count
                        FROM (
                            SELECT miner1 as node_id FROM file_assignments WHERE miner1 IS NOT NULL
                            UNION ALL
                            SELECT miner2 as node_id FROM file_assignments WHERE miner2 IS NOT NULL
                            UNION ALL
                            SELECT miner3 as node_id FROM file_assignments WHERE miner3 IS NOT NULL
                            UNION ALL
                            SELECT miner4 as node_id FROM file_assignments WHERE miner4 IS NOT NULL
                            UNION ALL
                            SELECT miner5 as node_id FROM file_assignments WHERE miner5 IS NOT NULL
                        ) assignments
                        GROUP BY node_id
                    ),
                    miner_storage AS (
                        SELECT DISTINCT ON (miner_id) 
                            miner_id, 
                            (ipfs_storage_max - ipfs_repo_size) / (1024*1024*1024) as available_gb
                        FROM node_metrics 
                        ORDER BY miner_id, block_number DESC
                    )
                    SELECT 
                        ma.node_id, 
                        ma.assignment_count,
                        COALESCE(ms.available_gb, 0) as available_gb
                    FROM miner_assignments ma
                    LEFT JOIN miner_storage ms ON ma.node_id = ms.miner_id
                    ORDER BY ma.assignment_count DESC
                    LIMIT 10
                """)
                
                logger.info("\n📈 Top 10 miners by assignments (with storage info):")
                for i, miner in enumerate(top_miners, 1):
                    available_gb = float(miner['available_gb'] or 0)
                    logger.info(f"   {i}. {miner['node_id']}: {miner['assignment_count']} assignments, {available_gb:.1f}GB available")
                
        except Exception as e:
            logger.error(f"❌ Error checking distribution: {e}")
    
    async def cleanup(self):
        """Clean up resources."""
        try:
            if self.db_pool:
                from app.db.connection import close_db_pool
                await close_db_pool()
                logger.info("✅ Database connection closed")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")


async def main():
    """Main entry point."""
    logger.info("🔧 Simple Reliable File Assignment Tool")
    logger.info("=" * 60)
    
    assigner = SimpleFileAssigner()
    
    try:
        # Initialize
        success = await assigner.initialize()
        if not success:
            logger.error("Failed to initialize")
            return 1
        
        # Parse command line arguments
        if len(sys.argv) > 1:
            arg = sys.argv[1]
            
            if arg == "--fix-all":
                # Fix all incomplete assignments
                success, count = await assigner.fix_empty_assignments()
                logger.info(f"\n{'='*60}")
                if success:
                    logger.info(f"✅ Successfully filled incomplete assignments for {count} files")
                else:
                    logger.error(f"❌ Fixed {count} files but some failed")
                
            elif arg == "--distribution":
                # Check assignment distribution
                await assigner.check_assignment_distribution()
                
            else:
                # Fix specific file
                cid = arg
                success = await assigner.fix_specific_file(cid)
                if success:
                    logger.info(f"✅ Successfully fixed file: {cid}")
                else:
                    logger.error(f"❌ Failed to fix file: {cid}")
        else:
            # Interactive mode
            print("\nOptions:")
            print("1. Fill all incomplete assignments (any NULL miner columns)")
            print("2. Check assignment distribution")
            print("3. Fix specific file")
            
            choice = input("Choose option (1-3): ").strip()
            
            if choice == "1":
                success, count = await assigner.fix_empty_assignments()
                if success:
                    logger.info(f"✅ Successfully filled incomplete assignments for {count} files")
                else:
                    logger.error(f"❌ Fixed {count} files but some failed")
                    
            elif choice == "2":
                await assigner.check_assignment_distribution()
                
            elif choice == "3":
                cid = input("Enter file CID: ").strip()
                success = await assigner.fix_specific_file(cid)
                if success:
                    logger.info(f"✅ Successfully fixed file: {cid}")
                else:
                    logger.error(f"❌ Failed to fix file: {cid}")
            else:
                logger.error("Invalid choice")
                return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        logger.exception("Full traceback:")
        return 1
    finally:
        await assigner.cleanup()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 