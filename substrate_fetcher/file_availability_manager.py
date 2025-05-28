"""
File Availability Manager

This module handles:
1. Tracking file availability failures
2. Managing miner availability scores
3. Reassigning replicas based on availability rules
4. Updating file assignments automatically
"""

import asyncio
import asyncpg
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AvailabilityRules:
    """Configuration for availability and reassignment rules."""
    min_replicas: int = 5  # Minimum number of replicas per file
    max_replicas: int = 5  # Maximum number of replicas per file
    min_availability_score: float = 0.7  # Minimum score to be considered reliable
    max_consecutive_failures: int = 3  # Max failures before marking miner as unreliable
    failure_window_hours: int = 24  # Time window to consider for failure tracking
    reassignment_cooldown_hours: int = 6  # Cooldown before reassigning same file


class FileAvailabilityManager:
    """Manages file availability, failures, and replica reassignment."""
    
    def __init__(self, db_pool: asyncpg.Pool, rules: AvailabilityRules = None):
        self.db_pool = db_pool
        self.rules = rules or AvailabilityRules()
    
    async def record_failure(
        self, 
        cid: str, 
        miner_id: str, 
        epoch: int, 
        failure_type: str, 
        failure_reason: str = None
    ) -> None:
        """Record a file availability failure."""
        async with self.db_pool.acquire() as conn:
            try:
                # Record the failure
                await conn.execute("""
                    INSERT INTO file_failures (cid, miner_id, epoch, failure_type, failure_reason)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (cid, miner_id, epoch, failure_type) 
                    DO UPDATE SET 
                        retry_count = file_failures.retry_count + 1,
                        detected_at = NOW(),
                        failure_reason = EXCLUDED.failure_reason
                """, cid, miner_id, epoch, failure_type, failure_reason)
                
                # Update miner availability
                await self.update_miner_availability(miner_id, success=False)
                
                logger.warning(f"Recorded failure for {cid} on miner {miner_id}: {failure_type}")
                
            except Exception as e:
                logger.error(f"Error recording failure for {cid} on miner {miner_id}: {e}")
    
    async def record_success(self, cid: str, miner_id: str, epoch: int) -> None:
        """Record a successful file availability check."""
        async with self.db_pool.acquire() as conn:
            try:
                # Mark any existing failures as resolved
                await conn.execute("""
                    UPDATE file_failures 
                    SET resolved_at = NOW() 
                    WHERE cid = $1 AND miner_id = $2 AND resolved_at IS NULL
                """, cid, miner_id)
                
                # Update miner availability
                await self.update_miner_availability(miner_id, success=True)
                
                logger.debug(f"Recorded success for {cid} on miner {miner_id}")
                
            except Exception as e:
                logger.error(f"Error recording success for {cid} on miner {miner_id}: {e}")
    
    async def update_miner_availability(self, miner_id: str, success: bool) -> None:
        """Update miner availability score and statistics."""
        async with self.db_pool.acquire() as conn:
            try:
                if success:
                    await conn.execute("""
                        INSERT INTO miner_availability (miner_id, successful_checks, last_successful_check, consecutive_failures)
                        VALUES ($1, 1, NOW(), 0)
                        ON CONFLICT (miner_id) DO UPDATE SET
                            successful_checks = miner_availability.successful_checks + 1,
                            last_successful_check = NOW(),
                            consecutive_failures = 0,
                            availability_score = LEAST(1.0, 
                                CAST(miner_availability.successful_checks + 1 AS DECIMAL) / 
                                CAST(miner_availability.successful_checks + miner_availability.failed_checks + 1 AS DECIMAL)
                            )
                    """, miner_id)
                else:
                    await conn.execute("""
                        INSERT INTO miner_availability (miner_id, failed_checks, last_failed_check, consecutive_failures)
                        VALUES ($1, 1, NOW(), 1)
                        ON CONFLICT (miner_id) DO UPDATE SET
                            failed_checks = miner_availability.failed_checks + 1,
                            last_failed_check = NOW(),
                            consecutive_failures = miner_availability.consecutive_failures + 1,
                            availability_score = CAST(miner_availability.successful_checks AS DECIMAL) / 
                                CAST(miner_availability.successful_checks + miner_availability.failed_checks + 1 AS DECIMAL),
                            is_active = CASE 
                                WHEN miner_availability.consecutive_failures + 1 >= $2 THEN FALSE 
                                ELSE miner_availability.is_active 
                            END
                    """, miner_id, self.rules.max_consecutive_failures)
                
            except Exception as e:
                logger.error(f"Error updating miner availability for {miner_id}: {e}")
    
    async def get_reliable_miners(self, exclude_miners: List[str] = None) -> List[str]:
        """Get list of reliable miners for reassignment."""
        exclude_miners = exclude_miners or []
        
        async with self.db_pool.acquire() as conn:
            # Get miners with good availability scores and active status
            rows = await conn.fetch("""
                SELECT miner_id, availability_score
                FROM miner_availability
                WHERE is_active = TRUE 
                  AND availability_score >= $1
                  AND miner_id != ALL($2)
                ORDER BY availability_score DESC, successful_checks DESC
            """, self.rules.min_availability_score, exclude_miners)
            
            return [row['miner_id'] for row in rows]
    
    async def get_files_needing_reassignment(self) -> List[Dict[str, Any]]:
        """Get files that need replica reassignment due to failures."""
        async with self.db_pool.acquire() as conn:
            # Find files with recent unresolved failures
            cutoff_time = datetime.utcnow() - timedelta(hours=self.rules.failure_window_hours)
            
            rows = await conn.fetch("""
                SELECT 
                    fa.cid,
                    fa.owner,
                    COUNT(CASE WHEN fa.miner1 IS NOT NULL THEN 1 END) +
                    COUNT(CASE WHEN fa.miner2 IS NOT NULL THEN 1 END) +
                    COUNT(CASE WHEN fa.miner3 IS NOT NULL THEN 1 END) +
                    COUNT(CASE WHEN fa.miner4 IS NOT NULL THEN 1 END) +
                    COUNT(CASE WHEN fa.miner5 IS NOT NULL THEN 1 END) as current_replicas,
                    
                    -- Count failed miners
                    COUNT(DISTINCT ff.miner_id) as failed_miners_count,
                    
                    -- Get failed miner IDs
                    ARRAY_AGG(DISTINCT ff.miner_id) as failed_miners
                    
                FROM file_assignments fa
                LEFT JOIN file_failures ff ON fa.cid = ff.cid 
                    AND ff.detected_at > $1 
                    AND ff.resolved_at IS NULL
                WHERE ff.cid IS NOT NULL  -- Only files with failures
                GROUP BY fa.cid, fa.owner
                HAVING COUNT(DISTINCT ff.miner_id) > 0  -- Has at least one failure
                ORDER BY failed_miners_count DESC, current_replicas ASC
            """, cutoff_time)
            
            return [dict(row) for row in rows]
    
    async def reassign_file_replicas(self, cid: str, failed_miners: List[str]) -> bool:
        """Reassign replicas for a file by replacing failed miners."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get current assignment
                assignment = await conn.fetchrow("""
                    SELECT cid, owner, miner1, miner2, miner3, miner4, miner5
                    FROM file_assignments
                    WHERE cid = $1
                """, cid)
                
                if not assignment:
                    logger.warning(f"No assignment found for CID {cid}")
                    return False
                
                # Current miners
                current_miners = [
                    assignment['miner1'], assignment['miner2'], assignment['miner3'],
                    assignment['miner4'], assignment['miner5']
                ]
                
                # Remove None values and failed miners
                active_miners = [m for m in current_miners if m and m not in failed_miners]
                
                # Check if we need more replicas
                needed_replicas = self.rules.min_replicas - len(active_miners)
                
                if needed_replicas <= 0:
                    logger.info(f"File {cid} has sufficient replicas ({len(active_miners)})")
                    return True
                
                # Get reliable miners for replacement
                exclude_miners = current_miners + failed_miners
                reliable_miners = await self.get_reliable_miners(exclude_miners)
                
                if len(reliable_miners) < needed_replicas:
                    logger.warning(f"Not enough reliable miners available for {cid}. Need {needed_replicas}, found {len(reliable_miners)}")
                    # Proceed with what we have
                
                # Select new miners
                new_miners = reliable_miners[:needed_replicas]
                
                # Build new assignment
                all_miners = active_miners + new_miners
                
                # Pad with None to ensure we have 5 slots
                while len(all_miners) < 5:
                    all_miners.append(None)
                
                # Update assignment
                await conn.execute("""
                    UPDATE file_assignments
                    SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6,
                        updated_at = NOW()
                    WHERE cid = $1
                """, cid, all_miners[0], all_miners[1], all_miners[2], all_miners[3], all_miners[4])
                
                # Mark failures as resolved for replaced miners
                await conn.execute("""
                    UPDATE file_failures
                    SET resolved_at = NOW()
                    WHERE cid = $1 AND miner_id = ANY($2) AND resolved_at IS NULL
                """, cid, failed_miners)
                
                logger.info(f"Reassigned replicas for {cid}: removed {failed_miners}, added {new_miners}")
                return True
                
        except Exception as e:
            logger.error(f"Error reassigning replicas for {cid}: {e}")
            return False
    
    async def process_reassignments(self) -> Dict[str, int]:
        """Process all files needing reassignment."""
        stats = {
            'files_checked': 0,
            'files_reassigned': 0,
            'reassignment_failures': 0
        }
        
        try:
            files_needing_reassignment = await self.get_files_needing_reassignment()
            stats['files_checked'] = len(files_needing_reassignment)
            
            logger.info(f"Found {len(files_needing_reassignment)} files needing reassignment")
            
            for file_info in files_needing_reassignment:
                cid = file_info['cid']
                failed_miners = file_info['failed_miners']
                
                logger.info(f"Processing reassignment for {cid} (failed miners: {failed_miners})")
                
                success = await self.reassign_file_replicas(cid, failed_miners)
                
                if success:
                    stats['files_reassigned'] += 1
                else:
                    stats['reassignment_failures'] += 1
            
            logger.info(f"Reassignment complete: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error processing reassignments: {e}")
            return stats
    
    async def get_availability_report(self) -> Dict[str, Any]:
        """Generate availability report."""
        async with self.db_pool.acquire() as conn:
            # Overall stats
            overall_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_miners,
                    COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_miners,
                    AVG(availability_score) as avg_availability_score,
                    COUNT(CASE WHEN availability_score >= $1 THEN 1 END) as reliable_miners
                FROM miner_availability
            """, self.rules.min_availability_score)
            
            # Recent failures
            cutoff_time = datetime.utcnow() - timedelta(hours=self.rules.failure_window_hours)
            failure_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(DISTINCT cid) as files_with_failures,
                    COUNT(DISTINCT miner_id) as miners_with_failures,
                    COUNT(*) as total_failures
                FROM file_failures
                WHERE detected_at > $1 AND resolved_at IS NULL
            """, cutoff_time)
            
            # Top failing miners
            top_failing_miners = await conn.fetch("""
                SELECT 
                    miner_id,
                    COUNT(*) as failure_count,
                    availability_score,
                    consecutive_failures,
                    is_active
                FROM file_failures ff
                JOIN miner_availability ma ON ff.miner_id = ma.miner_id
                WHERE ff.detected_at > $1 AND ff.resolved_at IS NULL
                GROUP BY miner_id, availability_score, consecutive_failures, is_active
                ORDER BY failure_count DESC
                LIMIT 10
            """, cutoff_time)
            
            return {
                'overall_stats': dict(overall_stats) if overall_stats else {},
                'failure_stats': dict(failure_stats) if failure_stats else {},
                'top_failing_miners': [dict(row) for row in top_failing_miners],
                'rules': {
                    'min_replicas': self.rules.min_replicas,
                    'max_replicas': self.rules.max_replicas,
                    'min_availability_score': self.rules.min_availability_score,
                    'max_consecutive_failures': self.rules.max_consecutive_failures,
                    'failure_window_hours': self.rules.failure_window_hours
                }
            } 