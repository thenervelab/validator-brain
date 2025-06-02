#!/usr/bin/env python3
"""
Simple Reliable File Assignment

A straightforward assignment system that:
1. Assigns per file (not batch)
2. Ensures broad network distribution 
3. Uses simple criteria: 1+ day old miners with capacity
4. Avoids complex scoring that causes issues
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
    def __init__(self):
        self.db_pool = None
        self.replicas_per_file = 5
        self.min_miner_age_days = 1
        self.assignment_count = {}  # Track assignments per miner in this session
        
    async def initialize(self):
        """Initialize database connection."""
        try:
            from app.db.connection import init_db_pool, get_db_pool
            await init_db_pool()
            self.db_pool = await get_db_pool()
            logger.info("✅ Database connection initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize database: {e}")
            return False
    
    async def get_reliable_miners(self):
        """Get reliable miners that are 1+ days old with capacity."""
        try:
            async with self.db_pool.acquire() as conn:
                cutoff_date = datetime.now() - timedelta(days=self.min_miner_age_days)
                
                miners = await conn.fetch("""
                    SELECT 
                        r.node_id,
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
                      AND COALESCE(ms.health_score, 100) >= 50
                    ORDER BY RANDOM()  -- Random order for better distribution
                """, cutoff_date)
                
                # Filter for capacity (keep it simple - just check they have some space)
                reliable_miners = []
                for miner in miners:
                    available_space = miner['storage_max'] - miner['storage_used']
                    if available_space > 100000:  # At least 100KB available (very low bar)
                        reliable_miners.append({
                            'node_id': miner['node_id'],
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
    
    def select_miners_simple(self, miners, count=5):
        """Simple miner selection with network distribution."""
        if len(miners) <= count:
            return [m['node_id'] for m in miners]
        
        # Simple distribution strategy:
        # 1. Sort by current assignment count (fewer = better)
        # 2. Add some randomness to avoid always picking the same ones
        
        # Update assignment counts
        for miner in miners:
            node_id = miner['node_id']
            miner['session_assignments'] = self.assignment_count.get(node_id, 0)
        
        # Sort by assignment count, then by files pinned, then random
        miners_sorted = sorted(miners, key=lambda m: (
            m['session_assignments'],          # Fewer assignments this session
            m['files_pinned'],                # Fewer total files
            random.random()                   # Random factor for distribution
        ))
        
        # Select the best distributed miners
        selected = miners_sorted[:count]
        selected_ids = [m['node_id'] for m in selected]
        
        # Update assignment counts
        for node_id in selected_ids:
            self.assignment_count[node_id] = self.assignment_count.get(node_id, 0) + 1
        
        return selected_ids
    
    async def assign_miners_to_file(self, cid, file_size, owner, filename=""):
        """Assign miners to a single file."""
        try:
            logger.info(f"🔧 Assigning miners to file: {filename or cid[:16]}...")
            
            # Get reliable miners
            miners = await self.get_reliable_miners()
            if len(miners) < self.replicas_per_file:
                logger.warning(f"⚠️ Only {len(miners)} reliable miners available, need {self.replicas_per_file}")
            
            if len(miners) == 0:
                logger.error("❌ No reliable miners available!")
                return False
            
            # Select miners using simple distribution
            selected_miners = self.select_miners_simple(miners, self.replicas_per_file)
            
            logger.info(f"📋 Selected {len(selected_miners)} miners:")
            for i, miner_id in enumerate(selected_miners):
                assignments = self.assignment_count.get(miner_id, 1)
                logger.info(f"   {i+1}. {miner_id} (session assignments: {assignments})")
            
            # Pad to 5 miners
            miners_padded = (selected_miners + [None] * 5)[:5]
            
            # Update database
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # Update file assignment
                    result = await conn.execute("""
                        UPDATE file_assignments
                        SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cid = $1
                    """, cid, miners_padded[0], miners_padded[1], miners_padded[2], 
                        miners_padded[3], miners_padded[4])
                    
                    if result == "UPDATE 0":
                        logger.error(f"❌ File not found in assignments table: {cid}")
                        return False
                    
                    # Update miner stats (simple increment)
                    for miner_id in selected_miners:
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
            
            logger.info(f"✅ Successfully assigned {len(selected_miners)} miners to file")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error assigning miners to file: {e}")
            return False
    
    async def fix_empty_assignments(self):
        """Fix all files with empty miner assignments."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get files with empty assignments
                empty_files = await conn.fetch("""
                    SELECT 
                        fa.cid, fa.owner, f.name, f.size
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
                      AND fa.miner4 IS NULL AND fa.miner5 IS NULL
                    ORDER BY f.size ASC  -- Process smaller files first
                """)
                
                if not empty_files:
                    logger.info("✅ No files with empty assignments found")
                    return True, 0
                
                logger.info(f"📋 Found {len(empty_files)} files with empty assignments")
                
                success_count = 0
                for i, file_info in enumerate(empty_files, 1):
                    logger.info(f"\n{'='*50}")
                    logger.info(f"Processing {i}/{len(empty_files)}: {file_info['name']}")
                    
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
                logger.info(f"✅ Assignment complete: {success_count}/{len(empty_files)} files fixed")
                
                return success_count == len(empty_files), success_count
                
        except Exception as e:
            logger.error(f"❌ Error fixing empty assignments: {e}")
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
        """Check how assignments are distributed across miners."""
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
                    logger.info("📊 Assignment Distribution:")
                    logger.info(f"   Total miners with assignments: {stats['total_miners']}")
                    logger.info(f"   Average assignments per miner: {stats['avg_assignments']:.1f}")
                    logger.info(f"   Min assignments: {stats['min_assignments']}")
                    logger.info(f"   Max assignments: {stats['max_assignments']}")
                    logger.info(f"   Standard deviation: {stats['stddev_assignments']:.1f}")
                
                # Show top and bottom miners
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
                    )
                    SELECT node_id, assignment_count
                    FROM miner_assignments
                    ORDER BY assignment_count DESC
                    LIMIT 10
                """)
                
                logger.info("\n📈 Top 10 miners by assignments:")
                for i, miner in enumerate(top_miners, 1):
                    logger.info(f"   {i}. {miner['node_id']}: {miner['assignment_count']} assignments")
                
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
                # Fix all empty assignments
                success, count = await assigner.fix_empty_assignments()
                logger.info(f"\n{'='*60}")
                if success:
                    logger.info(f"✅ Successfully fixed {count} empty assignments")
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
            print("1. Fix all empty assignments")
            print("2. Check assignment distribution")
            print("3. Fix specific file")
            
            choice = input("Choose option (1-3): ").strip()
            
            if choice == "1":
                success, count = await assigner.fix_empty_assignments()
                if success:
                    logger.info(f"✅ Successfully fixed {count} empty assignments")
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