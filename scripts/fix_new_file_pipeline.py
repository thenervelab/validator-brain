#!/usr/bin/env python3
"""
Fix New File Pipeline

This script runs through the entire new file processing pipeline to ensure
files from storage requests get properly assigned miners and included in profiles.

Pipeline:
1. Pinning requests → pending_assignment_file (pinning_file_processor)
2. pending_assignment_file → files + file_assignments (file_assignment_processor) 
3. Empty assignments → fixed assignments (availability_manager)
4. File assignments → user profiles (profile reconstruction)
"""

import asyncio
import logging
import os
import subprocess
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


def run_processor(processor_name: str, description: str) -> bool:
    """Run a processor script and return success status."""
    try:
        logger.info(f"🚀 Running {description}...")
        
        result = subprocess.run([
            'python', f'rabbitmq/{processor_name}'
        ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
        
        if result.returncode == 0:
            logger.info(f"✅ {description} completed successfully")
            if result.stdout.strip():
                logger.info(f"   Output: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"❌ {description} failed with return code {result.returncode}")
            if result.stderr.strip():
                logger.error(f"   Error: {result.stderr.strip()}")
            if result.stdout.strip():
                logger.info(f"   Output: {result.stdout.strip()}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {description} timed out after 5 minutes")
        return False
    except Exception as e:
        logger.error(f"❌ Error running {description}: {e}")
        return False


async def run_availability_fix() -> bool:
    """Run the availability manager to fix empty assignments."""
    try:
        logger.info("🔧 Running availability manager to fix empty assignments...")
        
        from rabbitmq.availability_manager_processor import AvailabilityManagerProcessor
        
        processor = AvailabilityManagerProcessor()
        success = await processor.initialize()
        if not success:
            logger.error("Failed to initialize availability manager")
            return False
        
        # Run maintenance
        stats = await processor.run_maintenance()
        
        logger.info(f"📊 Availability maintenance results:")
        logger.info(f"   Empty assignments fixed: {stats['empty_assignments_fixed']}")
        logger.info(f"   Availability reassignments: {stats['availability_reassignments']}")
        logger.info(f"   Total failures: {stats['total_failures']}")
        
        await processor.cleanup()
        
        return stats['total_failures'] == 0
        
    except Exception as e:
        logger.error(f"❌ Error running availability manager: {e}")
        return False


async def check_pipeline_health() -> dict:
    """Check the health of the file processing pipeline."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        await init_db_pool()
        db_pool = await get_db_pool()
        
        stats = {}
        
        async with db_pool.acquire() as conn:
            # Count pending files
            stats['pending_files'] = await conn.fetchval("""
                SELECT COUNT(*) FROM pending_assignment_file WHERE status = 'pending'
            """)
            
            # Count empty assignments
            stats['empty_assignments'] = await conn.fetchval("""
                SELECT COUNT(*) FROM file_assignments 
                WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                  AND miner4 IS NULL AND miner5 IS NULL
            """)
            
            # Count recent activity
            stats['recent_assignments'] = await conn.fetchval("""
                SELECT COUNT(*) FROM file_assignments 
                WHERE updated_at > NOW() - INTERVAL '1 hour'
            """)
            
            # Count total files
            stats['total_files'] = await conn.fetchval("SELECT COUNT(*) FROM file_assignments")
        
        await close_db_pool()
        return stats
        
    except Exception as e:
        logger.error(f"Error checking pipeline health: {e}")
        return {}


async def fix_entire_pipeline() -> bool:
    """Run the entire file processing pipeline to fix stuck files."""
    logger.info("🔄 Starting complete file processing pipeline fix...")
    logger.info("=" * 70)
    
    # Check initial state
    logger.info("📊 Checking initial pipeline state...")
    initial_stats = await check_pipeline_health()
    if initial_stats:
        logger.info(f"   Pending files: {initial_stats.get('pending_files', 0)}")
        logger.info(f"   Empty assignments: {initial_stats.get('empty_assignments', 0)}")
        logger.info(f"   Total files: {initial_stats.get('total_files', 0)}")
    
    success_count = 0
    total_steps = 4
    
    # Step 1: Process pinning files (pinning_requests → pending_assignment_file)
    logger.info(f"\n1️⃣ Step 1/{total_steps}: Processing pinning files...")
    if run_processor('pinning_file_processor.py', 'Pinning file processor'):
        success_count += 1
    
    # Step 2: Assign files to miners (pending_assignment_file → file_assignments)
    logger.info(f"\n2️⃣ Step 2/{total_steps}: Assigning files to miners...")
    if run_processor('file_assignment_processor.py', 'File assignment processor'):
        success_count += 1
    
    # Step 3: Fix empty assignments (availability manager)
    logger.info(f"\n3️⃣ Step 3/{total_steps}: Fixing empty assignments...")
    if await run_availability_fix():
        success_count += 1
    
    # Step 4: Check final state
    logger.info(f"\n4️⃣ Step 4/{total_steps}: Checking final state...")
    final_stats = await check_pipeline_health()
    if final_stats:
        logger.info(f"📊 Final pipeline state:")
        logger.info(f"   Pending files: {final_stats.get('pending_files', 0)}")
        logger.info(f"   Empty assignments: {final_stats.get('empty_assignments', 0)}")
        logger.info(f"   Total files: {final_stats.get('total_files', 0)}")
        
        # Compare with initial state
        if initial_stats:
            pending_diff = (initial_stats.get('pending_files', 0) - 
                          final_stats.get('pending_files', 0))
            empty_diff = (initial_stats.get('empty_assignments', 0) - 
                         final_stats.get('empty_assignments', 0))
            
            logger.info(f"📈 Changes:")
            if pending_diff > 0:
                logger.info(f"   ✅ Processed {pending_diff} pending files")
            if empty_diff > 0:
                logger.info(f"   ✅ Fixed {empty_diff} empty assignments")
            
            if final_stats.get('empty_assignments', 0) == 0:
                logger.info("   🎉 No empty assignments remaining!")
            elif final_stats.get('empty_assignments', 0) > 0:
                logger.warning(f"   ⚠️ {final_stats.get('empty_assignments', 0)} empty assignments still remain")
        
        success_count += 1
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info(f"📋 PIPELINE FIX SUMMARY:")
    logger.info(f"   Steps completed: {success_count}/{total_steps}")
    
    if success_count == total_steps:
        logger.info("✅ All pipeline steps completed successfully!")
        logger.info("📝 User profiles will be reconstructed in the next epoch cycle")
        return True
    else:
        logger.warning(f"⚠️ Only {success_count}/{total_steps} steps completed successfully")
        logger.info("💡 Check the logs above for specific failures")
        return False


async def diagnose_specific_file(cid: str) -> bool:
    """Diagnose and potentially fix a specific file."""
    logger.info(f"🔍 Diagnosing and fixing file: {cid}")
    logger.info("=" * 60)
    
    try:
        from scripts.diagnose_new_file import diagnose_file
        await diagnose_file(cid)
        
        # Try to fix if needed
        logger.info(f"\n🔧 Attempting to fix file assignment...")
        await run_availability_fix()
        
        # Re-diagnose
        logger.info(f"\n🔍 Re-checking file after fix...")
        await diagnose_file(cid)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error diagnosing file: {e}")
        return False


async def main():
    """Main entry point."""
    logger.info("🔧 New File Pipeline Fixer")
    logger.info("=" * 50)
    
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        
        if arg == "--full":
            # Run complete pipeline fix
            success = await fix_entire_pipeline()
            exit_code = 0 if success else 1
            
        elif arg == "--health":
            # Just check pipeline health
            stats = await check_pipeline_health()
            if stats:
                logger.info("📊 Pipeline Health Report:")
                logger.info(f"   Pending files: {stats.get('pending_files', 0)}")
                logger.info(f"   Empty assignments: {stats.get('empty_assignments', 0)}")
                logger.info(f"   Recent assignments: {stats.get('recent_assignments', 0)}")
                logger.info(f"   Total files: {stats.get('total_files', 0)}")
                
                if stats.get('empty_assignments', 0) > 0:
                    logger.warning("⚠️ Empty assignments detected - run with --full to fix")
                else:
                    logger.info("✅ No empty assignments found")
            exit_code = 0
            
        else:
            # Diagnose specific file
            cid = arg
            success = await diagnose_specific_file(cid)
            exit_code = 0 if success else 1
    else:
        # Interactive mode
        print("\nOptions:")
        print("1. Fix entire pipeline (recommended)")
        print("2. Check pipeline health")
        print("3. Diagnose specific file")
        
        choice = input("Choose option (1-3): ").strip()
        
        if choice == "1":
            success = await fix_entire_pipeline()
            exit_code = 0 if success else 1
        elif choice == "2":
            stats = await check_pipeline_health()
            if stats:
                logger.info("📊 Pipeline Health Report:")
                logger.info(f"   Pending files: {stats.get('pending_files', 0)}")
                logger.info(f"   Empty assignments: {stats.get('empty_assignments', 0)}")
                logger.info(f"   Recent assignments: {stats.get('recent_assignments', 0)}")
                logger.info(f"   Total files: {stats.get('total_files', 0)}")
                
                if stats.get('empty_assignments', 0) > 0:
                    logger.warning("⚠️ Empty assignments detected - run with --full to fix")
                else:
                    logger.info("✅ No empty assignments found")
            exit_code = 0
        elif choice == "3":
            cid = input("Enter file CID: ").strip()
            success = await diagnose_specific_file(cid)
            exit_code = 0 if success else 1
        else:
            logger.error("Invalid choice")
            exit_code = 1
    
    return exit_code


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 