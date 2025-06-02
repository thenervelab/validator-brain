#!/usr/bin/env python3
"""
Emergency Manual Assignment

Quick script to manually assign all empty files to available miners.
This bypasses the broken self-healing automation.
"""

import asyncio
import logging
import os
import sys
import random

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


async def manual_assignment_fix():
    """Manually assign all empty files to available miners."""
    try:
        from app.db.connection import init_db_pool, get_db_pool
        
        # Initialize database
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Get all completely empty files
            empty_files = await conn.fetch("""
                SELECT cid, owner
                FROM file_assignments 
                WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                  AND miner4 IS NULL AND miner5 IS NULL
                ORDER BY cid
            """)
            
            if not empty_files:
                logger.info("✅ No empty files found - all files have miners assigned!")
                return
            
            logger.info(f"🔧 Found {len(empty_files)} completely empty files")
            
            # Get available miners
            available_miners = await conn.fetch("""
                SELECT r.node_id 
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
                  AND COALESCE(ms.health_score, 100) >= 50 
                  AND (COALESCE(nm.ipfs_storage_max, 1000000000) - COALESCE(nm.ipfs_repo_size, 0)) > 10000000
                ORDER BY RANDOM()
            """)
            
            if len(available_miners) < 5:
                logger.error(f"❌ Only {len(available_miners)} miners available, need at least 5")
                return
            
            logger.info(f"✅ Found {len(available_miners)} available miners")
            
            # Assign each empty file to 5 random miners
            fixed_count = 0
            
            for file_info in empty_files:
                cid = file_info['cid']
                
                # Select 5 random miners for this file
                selected_miners = random.sample(available_miners, 5)
                miner_ids = [m['node_id'] for m in selected_miners]
                
                # Update the assignment
                await conn.execute("""
                    UPDATE file_assignments 
                    SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6
                    WHERE cid = $1
                """, cid, miner_ids[0], miner_ids[1], miner_ids[2], miner_ids[3], miner_ids[4])
                
                fixed_count += 1
                logger.info(f"✅ Fixed file {fixed_count}/{len(empty_files)}: {cid[:16]}...")
            
            # Verify the fix
            remaining_empty = await conn.fetchval("""
                SELECT COUNT(*) 
                FROM file_assignments 
                WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                  AND miner4 IS NULL AND miner5 IS NULL
            """)
            
            logger.info(f"🎉 EMERGENCY FIX COMPLETE!")
            logger.info(f"   Fixed: {fixed_count} files")
            logger.info(f"   Remaining empty: {remaining_empty}")
            
            if remaining_empty == 0:
                logger.info("✅ ALL FILES NOW HAVE MINERS ASSIGNED!")
            else:
                logger.warning(f"⚠️ {remaining_empty} files still empty - may need manual review")
        
        # Close database
        from app.db.connection import close_db_pool
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"❌ Error during manual assignment: {e}")
        logger.exception("Full traceback:")


async def main():
    """Main entry point."""
    logger.info("🚨 EMERGENCY MANUAL ASSIGNMENT - Fixing empty file assignments")
    logger.info("=" * 60)
    
    await manual_assignment_fix()
    
    logger.info("✅ Emergency manual assignment completed!")


if __name__ == "__main__":
    asyncio.run(main()) 