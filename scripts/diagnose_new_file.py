#!/usr/bin/env python3
"""
Diagnose New File Status

This script checks where a new file is in the processing pipeline:
1. pinning_requests (original storage request)
2. pending_assignment_file (parsed from request)
3. file_assignments (assigned to miners)
4. files table (file metadata)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime

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


async def diagnose_file(cid: str):
    """Diagnose where a file is in the processing pipeline."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        logger.info(f"🔍 Diagnosing file: {cid}")
        logger.info("=" * 80)
        
        # Initialize database
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Check 1: pinning_requests table
            logger.info("1️⃣ Checking pinning_requests table...")
            pinning_req = await conn.fetchrow("""
                SELECT request_hash, status, owner, replicas, created_at
                FROM pinning_requests 
                WHERE request_hash = $1
                ORDER BY created_at DESC
                LIMIT 1
            """, cid)
            
            if pinning_req:
                logger.info(f"   ✅ Found in pinning_requests:")
                logger.info(f"      Status: {pinning_req['status']}")
                logger.info(f"      Owner: {pinning_req['owner']}")
                logger.info(f"      Replicas: {pinning_req['replicas']}")
                logger.info(f"      Created: {pinning_req['created_at']}")
            else:
                logger.info("   ❌ Not found in pinning_requests")
            
            # Check 2: pending_assignment_file table
            logger.info("\n2️⃣ Checking pending_assignment_file table...")
            pending_file = await conn.fetchrow("""
                SELECT cid, owner, filename, file_size_bytes, status, created_at, processed_at
                FROM pending_assignment_file 
                WHERE cid = $1
                ORDER BY created_at DESC
                LIMIT 1
            """, cid)
            
            if pending_file:
                logger.info(f"   ✅ Found in pending_assignment_file:")
                logger.info(f"      Filename: {pending_file['filename']}")
                logger.info(f"      Owner: {pending_file['owner']}")
                logger.info(f"      Size: {pending_file['file_size_bytes']:,} bytes")
                logger.info(f"      Status: {pending_file['status']}")
                logger.info(f"      Created: {pending_file['created_at']}")
                logger.info(f"      Processed: {pending_file['processed_at']}")
            else:
                logger.info("   ❌ Not found in pending_assignment_file")
            
            # Check 3: files table
            logger.info("\n3️⃣ Checking files table...")
            file_info = await conn.fetchrow("""
                SELECT cid, name, size, created_date
                FROM files 
                WHERE cid = $1
            """, cid)
            
            if file_info:
                logger.info(f"   ✅ Found in files table:")
                logger.info(f"      Name: {file_info['name']}")
                logger.info(f"      Size: {file_info['size']:,} bytes")
                logger.info(f"      Created: {file_info['created_date']}")
            else:
                logger.info("   ❌ Not found in files table")
            
            # Check 4: file_assignments table
            logger.info("\n4️⃣ Checking file_assignments table...")
            assignment = await conn.fetchrow("""
                SELECT cid, owner, miner1, miner2, miner3, miner4, miner5, created_at, updated_at
                FROM file_assignments 
                WHERE cid = $1
            """, cid)
            
            if assignment:
                miners = [assignment[f'miner{i}'] for i in range(1, 6)]
                assigned_miners = [m for m in miners if m is not None]
                
                logger.info(f"   ✅ Found in file_assignments:")
                logger.info(f"      Owner: {assignment['owner']}")
                logger.info(f"      Assigned miners: {len(assigned_miners)}/5")
                if assigned_miners:
                    for i, miner in enumerate(assigned_miners):
                        logger.info(f"         {i+1}. {miner}")
                else:
                    logger.warning("      ⚠️ ALL MINERS ARE NULL!")
                logger.info(f"      Created: {assignment['created_at']}")
                logger.info(f"      Updated: {assignment['updated_at']}")
            else:
                logger.info("   ❌ Not found in file_assignments")
            
            # Check 5: Recent activity
            logger.info("\n5️⃣ Checking recent processing activity...")
            
            # Check if file assignment processor has run recently
            recent_assignments = await conn.fetchval("""
                SELECT COUNT(*) FROM file_assignments 
                WHERE updated_at > NOW() - INTERVAL '1 hour'
            """)
            
            logger.info(f"   Files assigned in last hour: {recent_assignments}")
            
            # Check pending files count
            pending_count = await conn.fetchval("""
                SELECT COUNT(*) FROM pending_assignment_file 
                WHERE status = 'pending'
            """)
            
            logger.info(f"   Pending assignment files: {pending_count}")
            
            # Summary and recommendations
            logger.info("\n" + "=" * 80)
            logger.info("📋 DIAGNOSIS SUMMARY:")
            
            if not pinning_req and not pending_file and not file_info and not assignment:
                logger.error("❌ File not found anywhere in the system!")
                logger.info("💡 This file may not have been submitted as a storage request yet")
                
            elif pinning_req and not pending_file:
                logger.warning("⚠️ File is in pinning_requests but not parsed yet")
                logger.info("💡 Run: python rabbitmq/pinning_file_processor.py")
                
            elif pending_file and not file_info:
                logger.warning("⚠️ File is pending but not processed by consumer")
                logger.info("💡 Check if pinning_file_consumer is running")
                
            elif file_info and not assignment:
                logger.warning("⚠️ File exists but no assignment record")
                logger.info("💡 Run: python rabbitmq/file_assignment_processor.py")
                
            elif assignment and len([m for m in [assignment.get(f'miner{i}') for i in range(1, 6)] if m]) == 0:
                logger.error("❌ File has assignment record but NO MINERS!")
                logger.info("💡 Run: python scripts/fix_empty_assignment_simple.py --fix")
                
            else:
                logger.info("✅ File appears to be properly processed")
        
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"❌ Error during diagnosis: {e}")
        logger.exception("Full traceback:")


async def check_system_health():
    """Check overall system health for file processing."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        logger.info("🏥 Checking file processing system health...")
        logger.info("=" * 60)
        
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Check pending files by status
            pending_stats = await conn.fetch("""
                SELECT status, COUNT(*) as count
                FROM pending_assignment_file
                GROUP BY status
                ORDER BY status
            """)
            
            logger.info("📊 Pending assignment file status:")
            for stat in pending_stats:
                logger.info(f"   {stat['status']}: {stat['count']} files")
            
            # Check assignment completeness
            assignment_stats = await conn.fetch("""
                SELECT 
                    CASE 
                        WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                             AND miner4 IS NULL AND miner5 IS NULL THEN 'ALL_EMPTY'
                        WHEN (CASE WHEN miner1 IS NOT NULL THEN 1 ELSE 0 END +
                              CASE WHEN miner2 IS NOT NULL THEN 1 ELSE 0 END +
                              CASE WHEN miner3 IS NOT NULL THEN 1 ELSE 0 END +
                              CASE WHEN miner4 IS NOT NULL THEN 1 ELSE 0 END +
                              CASE WHEN miner5 IS NOT NULL THEN 1 ELSE 0 END) < 5 THEN 'PARTIAL'
                        ELSE 'FULL'
                    END as assignment_status,
                    COUNT(*) as count
                FROM file_assignments
                GROUP BY 
                    CASE 
                        WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                             AND miner4 IS NULL AND miner5 IS NULL THEN 'ALL_EMPTY'
                        WHEN (CASE WHEN miner1 IS NOT NULL THEN 1 ELSE 0 END +
                              CASE WHEN miner2 IS NOT NULL THEN 1 ELSE 0 END +
                              CASE WHEN miner3 IS NOT NULL THEN 1 ELSE 0 END +
                              CASE WHEN miner4 IS NOT NULL THEN 1 ELSE 0 END +
                              CASE WHEN miner5 IS NOT NULL THEN 1 ELSE 0 END) < 5 THEN 'PARTIAL'
                        ELSE 'FULL'
                    END
                ORDER BY assignment_status
            """)
            
            logger.info("\n📊 File assignment completeness:")
            for stat in assignment_stats:
                status = stat['assignment_status']
                count = stat['count']
                if status == 'ALL_EMPTY':
                    logger.error(f"   🚨 {status}: {count} files (NEEDS IMMEDIATE ATTENTION!)")
                elif status == 'PARTIAL':
                    logger.warning(f"   ⚠️ {status}: {count} files")
                else:
                    logger.info(f"   ✅ {status}: {count} files")
            
            # Check recent activity
            recent_activity = await conn.fetchval("""
                SELECT COUNT(*) FROM file_assignments 
                WHERE updated_at > NOW() - INTERVAL '2 hours'
            """)
            
            logger.info(f"\n⏰ Files assigned in last 2 hours: {recent_activity}")
            
            if recent_activity == 0:
                logger.warning("⚠️ No recent assignment activity - processors may not be running")
        
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"❌ Error checking system health: {e}")


async def main():
    """Main entry point."""
    logger.info("🔍 File Diagnosis Tool")
    logger.info("=" * 50)
    
    if len(sys.argv) > 1:
        cid = sys.argv[1]
        if cid == "--health":
            await check_system_health()
        else:
            await diagnose_file(cid)
    else:
        print("\nUsage:")
        print("  python scripts/diagnose_new_file.py <CID>     - Diagnose specific file")
        print("  python scripts/diagnose_new_file.py --health  - Check system health")
        print("\nExample:")
        print("  python scripts/diagnose_new_file.py bafkreifdekt47g5i5bfcbjojdwk6k53ybmjrxjeime3reolpixegh6zsh4")


if __name__ == "__main__":
    asyncio.run(main()) 