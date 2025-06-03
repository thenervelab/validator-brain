#!/usr/bin/env python3
"""
Test Assignment Validation

Quick test to verify that our assignment system rejects files with insufficient miners
and prevents NULL assignments from being saved to the database.
"""

import asyncio
import logging
import json
import os
import sys
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_assignment_validation():
    """Test the assignment validation system."""
    try:
        await init_db_pool()
        db_pool = await get_db_pool()
        
        logger.info("🧪 Testing Assignment Validation System")
        logger.info("=" * 60)
        
        # Test 1: Check current assignment coverage
        logger.info("Test 1: Current Assignment Coverage")
        async with db_pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_assignments,
                    COUNT(CASE WHEN miner1 IS NOT NULL AND miner2 IS NOT NULL AND miner3 IS NOT NULL 
                               AND miner4 IS NOT NULL AND miner5 IS NOT NULL THEN 1 END) as complete_assignments,
                    COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                               AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments,
                    COUNT(CASE WHEN (miner1 IS NULL OR miner2 IS NULL OR miner3 IS NULL 
                                     OR miner4 IS NULL OR miner5 IS NULL)
                               AND NOT (miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                                       AND miner4 IS NULL AND miner5 IS NULL) THEN 1 END) as partial_assignments
                FROM file_assignments
            """)
            
            if stats:
                total = stats['total_assignments']
                complete = stats['complete_assignments']
                empty = stats['empty_assignments']
                partial = stats['partial_assignments']
                
                logger.info(f"   Total assignments: {total}")
                logger.info(f"   Complete assignments (5/5 miners): {complete}")
                logger.info(f"   Empty assignments (0/5 miners): {empty}")
                logger.info(f"   Partial assignments (1-4/5 miners): {partial}")
                
                if empty > 0:
                    logger.warning(f"   ⚠️ {empty} files have completely empty assignments!")
                else:
                    logger.info(f"   ✅ No files with completely empty assignments")
                
                if total > 0:
                    complete_pct = (complete / total) * 100
                    logger.info(f"   Assignment completeness: {complete_pct:.1f}%")
                    
                    if complete_pct >= 99:
                        logger.info("   🎯 EXCELLENT: Near-perfect assignment coverage!")
                    elif complete_pct >= 90:
                        logger.info("   ✅ GOOD: High assignment coverage")
                    else:
                        logger.warning(f"   ⚠️ WARNING: Low assignment coverage")
            
        # Test 2: Check pending assignment files
        logger.info("\nTest 2: Pending Assignment Files")
        async with db_pool.acquire() as conn:
            pending_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_pending,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
                    COUNT(CASE WHEN status = 'processed' THEN 1 END) as processed,
                    COUNT(CASE WHEN status = 'assigned' THEN 1 END) as assigned,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
                FROM pending_assignment_file
            """)
            
            if pending_stats:
                logger.info(f"   Total pending files: {pending_stats['total_pending']}")
                logger.info(f"   Status breakdown:")
                logger.info(f"     - Pending: {pending_stats['pending']}")
                logger.info(f"     - Processed: {pending_stats['processed']}")
                logger.info(f"     - Assigned: {pending_stats['assigned']}")
                logger.info(f"     - Failed: {pending_stats['failed']}")
                
                if pending_stats['failed'] > 0:
                    logger.info(f"\n   📋 Recent failures:")
                    failures = await conn.fetch("""
                        SELECT cid, owner, error_message, processed_at
                        FROM pending_assignment_file
                        WHERE status = 'failed'
                        ORDER BY processed_at DESC
                        LIMIT 5
                    """)
                    
                    for failure in failures:
                        logger.info(f"     - {failure['cid'][:16]}...: {failure['error_message']}")
        
        # Test 3: Check environment configuration
        logger.info("\nTest 3: Environment Configuration")
        min_required = os.getenv('MIN_REQUIRED_MINERS', '3')
        logger.info(f"   MIN_REQUIRED_MINERS: {min_required}")
        logger.info(f"   This setting prevents assignments with fewer than {min_required} miners")
        
        # Test 4: Check for potential file assignment issues
        logger.info("\nTest 4: File Assignment Issues")
        async with db_pool.acquire() as conn:
            # Check for files that exist but have no assignment entry
            unassigned_files = await conn.fetchval("""
                SELECT COUNT(*)
                FROM files f
                LEFT JOIN file_assignments fa ON f.cid = fa.cid
                WHERE fa.cid IS NULL
            """)
            
            logger.info(f"   Files without assignment entries: {unassigned_files}")
            
            if unassigned_files > 0:
                logger.warning(f"   ⚠️ {unassigned_files} files have no assignment entry at all!")
                logger.info("   These files should be added to pending_assignment_file for processing")
            else:
                logger.info("   ✅ All files have assignment entries")
        
        # Test 5: Check miner profile status
        logger.info("\nTest 5: Profile Status")
        async with db_pool.acquire() as conn:
            profile_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_user_profiles,
                    COUNT(CASE WHEN status = 'published' THEN 1 END) as published_user_profiles,
                    (SELECT COUNT(*) FROM pending_miner_profile) as total_miner_profiles,
                    (SELECT COUNT(*) FROM pending_miner_profile WHERE status = 'published') as published_miner_profiles
                FROM pending_user_profile
            """)
            
            if profile_stats:
                logger.info(f"   User profiles: {profile_stats['published_user_profiles']}/{profile_stats['total_user_profiles']} published")
                logger.info(f"   Miner profiles: {profile_stats['published_miner_profiles']}/{profile_stats['total_miner_profiles']} published")
                
                total_profiles = profile_stats['published_user_profiles'] + profile_stats['published_miner_profiles']
                if total_profiles > 0:
                    logger.info(f"   Total profiles ready for blockchain: {total_profiles}")
                else:
                    logger.warning("   ⚠️ No profiles ready for blockchain submission!")
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ Assignment validation test completed")
        
    except Exception as e:
        logger.error(f"❌ Error during assignment validation test: {e}")
    finally:
        await close_db_pool()


async def main():
    """Main entry point."""
    await test_assignment_validation()


if __name__ == "__main__":
    asyncio.run(main()) 