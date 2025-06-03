#!/usr/bin/env python3
"""
Diagnose Specific File Assignment Issues

Check why a specific file has NULL miners and debug the assignment logic.
"""

import asyncio
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def diagnose_file(file_cid: str):
    """Diagnose why a specific file has assignment issues."""
    try:
        await init_db_pool()
        db_pool = await get_db_pool()
        
        logger.info(f"🔍 Diagnosing file: {file_cid}")
        logger.info("=" * 80)
        
        async with db_pool.acquire() as conn:
            # 1. Check if file exists in files table
            file_info = await conn.fetchrow("""
                SELECT cid, name, size, created_date
                FROM files
                WHERE cid = $1
            """, file_cid)
            
            if not file_info:
                logger.error(f"❌ File {file_cid} not found in files table!")
                return
            
            logger.info(f"✅ File found in files table:")
            logger.info(f"   Name: {file_info['name']}")
            logger.info(f"   Size: {file_info['size']:,} bytes" if file_info['size'] else "   Size: NULL")
            logger.info(f"   Created: {file_info['created_date']}")
            
            # 2. Check file assignments
            assignment = await conn.fetchrow("""
                SELECT cid, owner, miner1, miner2, miner3, miner4, miner5, created_at, updated_at
                FROM file_assignments
                WHERE cid = $1
            """, file_cid)
            
            if not assignment:
                logger.error(f"❌ No assignment entry found for {file_cid}!")
                logger.info("   This file should be in pending_assignment_file for processing")
            else:
                miners = [assignment['miner1'], assignment['miner2'], assignment['miner3'], 
                         assignment['miner4'], assignment['miner5']]
                filled_count = sum(1 for m in miners if m is not None)
                empty_count = sum(1 for m in miners if m is None)
                
                logger.info(f"✅ Assignment entry found:")
                logger.info(f"   Owner: {assignment['owner']}")
                logger.info(f"   Miners filled: {filled_count}/5")
                logger.info(f"   Miners empty: {empty_count}/5")
                logger.info(f"   Created: {assignment['created_at']}")
                logger.info(f"   Updated: {assignment['updated_at']}")
                
                for i, miner in enumerate(miners, 1):
                    status = miner if miner else "NULL"
                    logger.info(f"     Slot {i}: {status}")
                
                if empty_count > 0:
                    logger.warning(f"⚠️ File has {empty_count} empty miner slots - should be caught by reassignment logic")
            
            # 3. Check pending assignment status
            pending = await conn.fetchrow("""
                SELECT id, cid, owner, filename, status, error_message, created_at, processed_at
                FROM pending_assignment_file
                WHERE cid = $1
            """, file_cid)
            
            if pending:
                logger.info(f"📋 Found in pending_assignment_file:")
                logger.info(f"   Status: {pending['status']}")
                logger.info(f"   Filename: {pending['filename']}")
                if pending['error_message']:
                    logger.info(f"   Error: {pending['error_message']}")
                logger.info(f"   Created: {pending['created_at']}")
                logger.info(f"   Processed: {pending['processed_at']}")
            else:
                logger.info("📋 Not found in pending_assignment_file")
            
            # 4. Check storage capacity requirements vs available miners
            if file_info['size']:
                file_size = file_info['size']
                safety_margin = int(file_size * 0.2)
                required_space = file_size + safety_margin
                
                logger.info(f"\n💾 Storage Requirements:")
                logger.info(f"   File size: {file_size:,} bytes")
                logger.info(f"   Safety margin (20%): {safety_margin:,} bytes")
                logger.info(f"   Required space per miner: {required_space:,} bytes")
                
                # Check how many miners have enough space
                suitable_miners = await conn.fetch("""
                    SELECT 
                        r.node_id,
                        COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                        COALESCE(nm.ipfs_repo_size, 0) as used_storage_bytes,
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
                      AND COALESCE(ms.health_score, 100) >= 30.0
                """)
                
                suitable_count = 0
                for miner in suitable_miners:
                    available = miner['storage_capacity_bytes'] - miner['used_storage_bytes']
                    if available >= required_space:
                        suitable_count += 1
                
                logger.info(f"   Miners with sufficient space: {suitable_count}/{len(suitable_miners)}")
                
                if suitable_count < 5:
                    logger.error(f"🚨 ISSUE FOUND: Only {suitable_count} miners have enough space!")
                    logger.error(f"   Need 5 miners with {required_space:,} bytes available")
                    logger.error(f"   This explains why assignment failed or has NULL miners")
                else:
                    logger.info(f"✅ Sufficient miners available ({suitable_count} >= 5)")
            
            # 5. Check configuration
            min_required = int(os.getenv('MIN_REQUIRED_MINERS', '3'))
            min_health = float(os.getenv('MIN_MINER_HEALTH_SCORE', '70.0'))
            logger.info(f"\n⚙️ Configuration:")
            logger.info(f"   MIN_REQUIRED_MINERS: {min_required}")
            logger.info(f"   MIN_MINER_HEALTH_SCORE: {min_health}%")
            
            if min_required != 5:
                logger.warning(f"⚠️ Configuration mismatch: MIN_REQUIRED_MINERS={min_required} but trying to assign 5 replicas")
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ File diagnosis completed")
        
    except Exception as e:
        logger.error(f"❌ Error during file diagnosis: {e}")
    finally:
        await close_db_pool()


async def main():
    """Main entry point."""
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python scripts/diagnose_specific_file.py <file_cid>")
        print("Example: python scripts/diagnose_specific_file.py bafkreicejeasjo5dpjgv2lctxjnbxedeelr64a3hgwdngr2lpuaks46ldq")
        sys.exit(1)
    
    file_cid = sys.argv[1]
    await diagnose_file(file_cid)


if __name__ == "__main__":
    asyncio.run(main()) 