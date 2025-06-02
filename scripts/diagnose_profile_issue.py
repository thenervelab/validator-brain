#!/usr/bin/env python3
"""
Diagnose Profile Issue

Debug why user profiles show 0 miners when file assignments exist.
"""

import asyncio
import logging
import os
import sys
from typing import Dict, List, Any

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


async def diagnose_user_profile_issue():
    """Diagnose why user profiles show 0 miners."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        # Initialize database
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            logger.info("🔍 DIAGNOSING USER PROFILE ISSUE")
            logger.info("=" * 60)
            
            # 1. Check file assignments status
            assignment_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
                    COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments
                FROM file_assignments
            """)
            
            logger.info("📊 File Assignment Status:")
            logger.info(f"   Total files: {assignment_stats['total_files']}")
            logger.info(f"   Files with miners: {assignment_stats['files_with_miners']}")
            logger.info(f"   Empty assignments: {assignment_stats['empty_assignments']}")
            
            # 2. Check user profile status
            profile_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_profiles,
                    COUNT(CASE WHEN status = 'published' THEN 1 END) as published_profiles,
                    COUNT(CASE WHEN files_count = 0 OR files_count IS NULL THEN 1 END) as zero_file_profiles
                FROM pending_user_profile
            """)
            
            logger.info("\n📊 User Profile Status:")
            logger.info(f"   Total user profiles: {profile_stats['total_profiles']}")
            logger.info(f"   Published profiles: {profile_stats['published_profiles']}")
            logger.info(f"   Profiles with 0 files: {profile_stats['zero_file_profiles']}")
            
            # 3. Check sample user to see what's happening
            sample_user = await conn.fetchrow("""
                SELECT 
                    owner,
                    COUNT(*) as assigned_files,
                    COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners
                FROM file_assignments
                GROUP BY owner
                HAVING COUNT(*) > 0
                ORDER BY COUNT(*) DESC
                LIMIT 1
            """)
            
            if sample_user:
                logger.info(f"\n🔍 Sample User Analysis: {sample_user['owner']}")
                logger.info(f"   Total assigned files: {sample_user['assigned_files']}")
                logger.info(f"   Files with miners: {sample_user['files_with_miners']}")
                
                # Check this user's profile
                user_profile = await conn.fetchrow("""
                    SELECT 
                        owner, status, files_count, files_size, cid, published_at, updated_at
                    FROM pending_user_profile
                    WHERE owner = $1
                """, sample_user['owner'])
                
                if user_profile:
                    logger.info(f"   Profile status: {user_profile['status']}")
                    logger.info(f"   Profile files_count: {user_profile['files_count']}")
                    logger.info(f"   Profile files_size: {user_profile['files_size']}")
                    logger.info(f"   Profile CID: {user_profile['cid']}")
                    logger.info(f"   Profile updated: {user_profile['updated_at']}")
                else:
                    logger.info("   ❌ No profile found for this user!")
                
                # Check the actual files for this user
                user_files = await conn.fetch("""
                    SELECT 
                        fa.cid, f.name, f.size,
                        fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5,
                        fa.updated_at
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE fa.owner = $1
                    ORDER BY fa.updated_at DESC
                    LIMIT 5
                """, sample_user['owner'])
                
                logger.info(f"\n📁 Sample files for user {sample_user['owner']}:")
                for i, file_info in enumerate(user_files, 1):
                    miners = [m for m in [file_info['miner1'], file_info['miner2'], 
                                        file_info['miner3'], file_info['miner4'], file_info['miner5']] 
                             if m is not None]
                    logger.info(f"   {i}. {file_info['name']} -> {len(miners)} miners")
                    if miners:
                        logger.info(f"      Miners: {', '.join(miners[:2])}{'...' if len(miners) > 2 else ''}")
            
            # 4. Check what the profile reconstruction query would return
            logger.info(f"\n🔍 Testing Profile Reconstruction Query for user: {sample_user['owner']}")
            
            profile_data = await conn.fetchrow("""
                WITH user_files AS (
                    SELECT 
                        fa.owner,
                        f.cid,
                        f.name,
                        f.size,
                        ARRAY_REMOVE(ARRAY[fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5], NULL) as miners
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE fa.owner = $1
                      AND (fa.miner1 IS NOT NULL OR fa.miner2 IS NOT NULL OR fa.miner3 IS NOT NULL 
                           OR fa.miner4 IS NOT NULL OR fa.miner5 IS NOT NULL)
                ),
                file_miner_pairs AS (
                    SELECT 
                        uf.owner,
                        uf.cid,
                        uf.name,
                        uf.size,
                        UNNEST(uf.miners) as miner_id
                    FROM user_files uf
                )
                SELECT 
                    $1 as owner,
                    COUNT(DISTINCT cid) as files_count,
                    SUM(size) as files_size,
                    COUNT(DISTINCT miner_id) as unique_miners,
                    COUNT(*) as total_assignments,
                    ARRAY_AGG(DISTINCT miner_id ORDER BY miner_id) as all_miners
                FROM file_miner_pairs
            """, sample_user['owner'])
            
            if profile_data:
                logger.info(f"   Files in query: {profile_data['files_count']}")
                logger.info(f"   Total size: {profile_data['files_size']}")
                logger.info(f"   Unique miners: {profile_data['unique_miners']}")
                logger.info(f"   Total assignments: {profile_data['total_assignments']}")
                logger.info(f"   Miners: {profile_data['all_miners'][:5]}{'...' if len(profile_data['all_miners']) > 5 else ''}")
            else:
                logger.info("   ❌ Profile reconstruction query returned no data!")
            
            # 5. Check if the issue is in the profile building logic
            logger.info(f"\n🔍 Checking Raw File Assignment Data:")
            
            raw_assignments = await conn.fetch("""
                SELECT 
                    fa.cid, fa.owner, 
                    fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5,
                    f.size
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE fa.owner = $1
                  AND (fa.miner1 IS NOT NULL OR fa.miner2 IS NOT NULL OR fa.miner3 IS NOT NULL 
                       OR fa.miner4 IS NOT NULL OR fa.miner5 IS NOT NULL)
                LIMIT 3
            """, sample_user['owner'])
            
            for i, assignment in enumerate(raw_assignments, 1):
                miners = [m for m in [assignment['miner1'], assignment['miner2'], 
                                    assignment['miner3'], assignment['miner4'], assignment['miner5']] 
                         if m is not None]
                logger.info(f"   File {i}: {assignment['cid'][:16]}... -> {len(miners)} miners")
                logger.info(f"            Size: {assignment['size']} bytes")
                logger.info(f"            Miners: {miners}")
            
            # 6. Check if there are any filtering issues
            logger.info(f"\n🔍 Checking Potential Filtering Issues:")
            
            # Check for files with NULL sizes
            null_size_count = await conn.fetchval("""
                SELECT COUNT(*)
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE fa.owner = $1 AND f.size IS NULL
            """, sample_user['owner'])
            
            logger.info(f"   Files with NULL size: {null_size_count}")
            
            # Check for very recent assignments that might be filtered out
            recent_assignments = await conn.fetchval("""
                SELECT COUNT(*)
                FROM file_assignments fa
                WHERE fa.owner = $1 
                  AND fa.updated_at > NOW() - INTERVAL '1 hour'
                  AND (fa.miner1 IS NOT NULL OR fa.miner2 IS NOT NULL OR fa.miner3 IS NOT NULL 
                       OR fa.miner4 IS NOT NULL OR fa.miner5 IS NOT NULL)
            """, sample_user['owner'])
            
            logger.info(f"   Recent assignments (last hour): {recent_assignments}")
            
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"❌ Error during diagnosis: {e}")
        logger.exception("Full traceback:")


async def main():
    """Main entry point."""
    await diagnose_user_profile_issue()


if __name__ == "__main__":
    asyncio.run(main()) 