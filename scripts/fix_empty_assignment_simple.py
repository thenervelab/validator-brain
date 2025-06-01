#!/usr/bin/env python3
"""
Simple Empty Assignment Fix

Quick fix for files with empty miner assignments.
This runs the availability manager logic directly for immediate resolution.
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


async def fix_empty_assignments():
    """Quick fix for empty assignments using availability manager logic."""
    try:
        # Import availability manager processor
        from rabbitmq.availability_manager_processor import AvailabilityManagerProcessor
        
        logger.info("🔧 Starting quick fix for empty assignments...")
        
        # Initialize processor
        processor = AvailabilityManagerProcessor()
        
        # Try to initialize
        success = await processor.initialize()
        if not success:
            logger.error("❌ Failed to initialize availability manager")
            logger.error("   Make sure DATABASE_URL and other environment variables are set")
            return False
        
        # Run just the empty assignment check
        logger.info("🔍 Checking for files with empty assignments...")
        empty_stats = await processor.check_empty_assignments()
        
        logger.info("📊 Results:")
        logger.info(f"   Files checked: {empty_stats['files_checked']}")
        logger.info(f"   Files fixed: {empty_stats['files_fixed']}")
        logger.info(f"   Files failed: {empty_stats['files_failed']}")
        
        # Generate report
        logger.info("📋 Generating availability report...")
        await processor.generate_availability_report()
        
        # Cleanup
        await processor.cleanup()
        
        if empty_stats['files_fixed'] > 0:
            logger.info("✅ Successfully fixed empty assignments!")
            logger.info("📝 User profiles will be reconstructed in the next epoch cycle")
        elif empty_stats['files_checked'] == 0:
            logger.info("✅ No files with empty assignments found - all good!")
        else:
            logger.warning("⚠️ Found files but couldn't fix them - check logs above")
        
        return empty_stats['files_failed'] == 0
        
    except ImportError as e:
        logger.error(f"❌ Failed to import availability manager: {e}")
        logger.error("   Make sure all dependencies are installed")
        return False
    except Exception as e:
        logger.error(f"❌ Error during fix: {e}")
        logger.exception("Full traceback:")
        return False


async def check_specific_file(cid: str):
    """Check status of a specific file."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        # Initialize database
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Check file assignment status
            file_info = await conn.fetchrow("""
                SELECT 
                    fa.cid,
                    fa.owner,
                    f.name as filename,
                    f.size,
                    fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5,
                    fa.created_at,
                    fa.updated_at
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE fa.cid = $1
            """, cid)
            
            if not file_info:
                logger.error(f"❌ File {cid} not found in assignments")
                return
            
            # Count assigned miners
            miners = [file_info[f'miner{i}'] for i in range(1, 6)]
            assigned_miners = [m for m in miners if m is not None]
            
            logger.info(f"📁 File: {file_info['filename']}")
            logger.info(f"   CID: {cid}")
            logger.info(f"   Size: {file_info['size']:,} bytes")
            logger.info(f"   Owner: {file_info['owner']}")
            logger.info(f"   Assigned miners: {len(assigned_miners)}/5")
            
            if len(assigned_miners) == 0:
                logger.warning("⚠️ ALL MINERS ARE NULL - this file needs immediate fixing!")
            elif len(assigned_miners) < 5:
                logger.warning(f"⚠️ Only {len(assigned_miners)} miners assigned - needs {5 - len(assigned_miners)} more")
                for i, miner in enumerate(assigned_miners):
                    logger.info(f"     {i+1}. {miner}")
            else:
                logger.info("✅ File has full assignment")
                for i, miner in enumerate(assigned_miners):
                    logger.info(f"     {i+1}. {miner}")
        
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"❌ Error checking file: {e}")


async def main():
    """Main entry point."""
    logger.info("🛠️ Simple Empty Assignment Fix Tool")
    logger.info("=" * 50)
    
    if len(sys.argv) > 1:
        # Check specific file
        cid = sys.argv[1]
        if cid == "--fix":
            # Run fix for all empty assignments
            logger.info("🔧 Fixing all empty assignments...")
            success = await fix_empty_assignments()
            exit_code = 0 if success else 1
        else:
            # Check specific file status
            logger.info(f"🔍 Checking file: {cid}")
            await check_specific_file(cid)
            exit_code = 0
    else:
        # Interactive mode
        print("\nOptions:")
        print("1. Fix all empty assignments")
        print("2. Check specific file")
        
        choice = input("Choose option (1-2): ").strip()
        
        if choice == "1":
            success = await fix_empty_assignments()
            exit_code = 0 if success else 1
        elif choice == "2":
            cid = input("Enter file CID: ").strip()
            await check_specific_file(cid)
            exit_code = 0
        else:
            logger.error("Invalid choice")
            exit_code = 1
    
    return exit_code


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 