#!/usr/bin/env python3
"""
Test Reassignment Selection

Test what files the assignment processor would select for reassignment.
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


async def test_reassignment_selection():
    """Test what files would be selected for reassignment."""
    try:
        await init_db_pool()
        db_pool = await get_db_pool()
        
        logger.info("🧪 Testing Reassignment Selection Logic")
        logger.info("=" * 60)
        
        async with db_pool.acquire() as conn:
            # Use the same query as the processor
            max_reassignments_per_batch = int(os.getenv('MAX_REASSIGNMENTS_PER_BATCH', '50'))
            
            files_needing_reassignment = await conn.fetch("""
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
                    fa.updated_at,
                    -- Count NULL miners for prioritization
                    (CASE WHEN fa.miner1 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner2 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner3 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner4 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner5 IS NULL THEN 1 ELSE 0 END) as null_miner_count
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL OR 
                       fa.miner4 IS NULL OR fa.miner5 IS NULL)
                  AND f.size IS NOT NULL
                ORDER BY 
                    null_miner_count DESC,  -- Prioritize files with more NULL miners (empty assignments first)
                    fa.updated_at ASC       -- Then by oldest first
                LIMIT $1
            """, max_reassignments_per_batch)
            
            if not files_needing_reassignment:
                logger.info("✅ No files found needing reassignment")
                return
            
            logger.info(f"Found {len(files_needing_reassignment)} files needing reassignment")
            
            # Group by null count
            completely_empty = [f for f in files_needing_reassignment if f['null_miner_count'] == 5]
            partially_empty = [f for f in files_needing_reassignment if f['null_miner_count'] < 5]
            
            logger.info(f"\n📊 Breakdown:")
            logger.info(f"   - {len(completely_empty)} completely empty (0/5 miners)")
            logger.info(f"   - {len(partially_empty)} partially empty (1-4/5 miners)")
            
            # Show top 10 completely empty files
            if completely_empty:
                logger.info(f"\n🎯 Top {min(10, len(completely_empty))} completely empty files:")
                for i, file_info in enumerate(completely_empty[:10]):
                    cid = file_info['cid']
                    filename = file_info['filename'] or "unnamed"
                    size = file_info['file_size_bytes'] or 0
                    updated = file_info['updated_at']
                    
                    logger.info(f"   {i+1}. {cid[:20]}... ({filename}, {size:,} bytes, updated: {updated})")
                    
                    # Check if this is our specific file
                    if cid == "bafkreicejeasjo5dpjgv2lctxjnbxedeelr64a3hgwdngr2lpuaks46ldq":
                        logger.info(f"      🎯 THIS IS THE FILE WE'RE LOOKING FOR!")
            
            # Show top 5 partially empty files
            if partially_empty:
                logger.info(f"\n📋 Top 5 partially empty files:")
                for i, file_info in enumerate(partially_empty[:5]):
                    cid = file_info['cid']
                    filename = file_info['filename'] or "unnamed"
                    null_count = file_info['null_miner_count']
                    filled_count = 5 - null_count
                    
                    logger.info(f"   {i+1}. {cid[:20]}... ({filename}, {filled_count}/5 filled)")
            
            # Check configuration
            logger.info(f"\n⚙️ Configuration:")
            logger.info(f"   MAX_REASSIGNMENTS_PER_BATCH: {max_reassignments_per_batch}")
            logger.info(f"   Files would be processed: {min(len(files_needing_reassignment), max_reassignments_per_batch)}")
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ Reassignment selection test completed")
        
    except Exception as e:
        logger.error(f"❌ Error during reassignment selection test: {e}")
    finally:
        await close_db_pool()


async def main():
    """Main entry point."""
    await test_reassignment_selection()


if __name__ == "__main__":
    asyncio.run(main()) 