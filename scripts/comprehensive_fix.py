#!/usr/bin/env python3
"""
Comprehensive Network Fix

Fixes both assignment and profile issues:
1. Fix empty file assignments with simple distribution
2. Rebuild user profiles from corrected assignments
3. Verify everything is working correctly
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


async def run_comprehensive_fix():
    """Run the complete fix process."""
    try:
        logger.info("🚀 COMPREHENSIVE NETWORK FIX")
        logger.info("=" * 60)
        logger.info(f"Started at: {datetime.now()}")
        
        # Step 1: Diagnose the current situation
        logger.info("\n📊 STEP 1: DIAGNOSIS")
        logger.info("-" * 40)
        
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        # Initialize database
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Check assignment status
            assignment_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                               OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
                    COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                               AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments
                FROM file_assignments
            """)
            
            # Check profile status
            profile_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_profiles,
                    COUNT(CASE WHEN status = 'published' THEN 1 END) as published_profiles,
                    COUNT(CASE WHEN files_count > 0 THEN 1 END) as profiles_with_files,
                    SUM(files_count) as total_profile_files
                FROM pending_user_profile
            """)
            
            # Check miner availability
            miner_count = await conn.fetchval("""
                SELECT COUNT(*)
                FROM registration 
                WHERE node_type = 'StorageMiner' 
                  AND status = 'active'
                  AND registered_at <= NOW() - INTERVAL '1 day'
            """)
            
            logger.info(f"📁 File Assignments:")
            logger.info(f"   Total files: {assignment_stats['total_files']}")
            logger.info(f"   Files with miners: {assignment_stats['files_with_miners']}")
            logger.info(f"   Empty assignments: {assignment_stats['empty_assignments']}")
            
            logger.info(f"\n👤 User Profiles:")
            logger.info(f"   Total profiles: {profile_stats['total_profiles']}")
            logger.info(f"   Published profiles: {profile_stats['published_profiles']}")
            logger.info(f"   Profiles with files: {profile_stats['profiles_with_files']}")
            logger.info(f"   Files in profiles: {profile_stats['total_profile_files']}")
            
            logger.info(f"\n⛏️ Available Miners:")
            logger.info(f"   Reliable miners (1+ day old): {miner_count}")
            
            # Check if we need fixes
            needs_assignment_fix = assignment_stats['empty_assignments'] > 0
            needs_profile_fix = (profile_stats['total_profile_files'] or 0) < assignment_stats['files_with_miners']
            
            logger.info(f"\n🔧 Fix Requirements:")
            logger.info(f"   Assignment fix needed: {'YES' if needs_assignment_fix else 'NO'}")
            logger.info(f"   Profile fix needed: {'YES' if needs_profile_fix else 'NO'}")
        
        # Step 2: Fix assignments if needed
        if needs_assignment_fix:
            logger.info("\n🔧 STEP 2: FIXING ASSIGNMENTS")
            logger.info("-" * 40)
            
            # Import and run the simple assignment fixer
            try:
                # Run the assignment fix as a subprocess to avoid module conflicts
                import subprocess
                result = subprocess.run([
                    sys.executable, 
                    os.path.join(os.path.dirname(__file__), "simple_reliable_assignment.py"),
                    "--fix-all"
                ], capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    logger.info("✅ Assignment fix completed successfully")
                    logger.info(result.stdout.split('\n')[-5:])  # Show last few lines
                else:
                    logger.error(f"❌ Assignment fix failed: {result.stderr}")
                    return False
                    
            except subprocess.TimeoutExpired:
                logger.error("❌ Assignment fix timed out")
                return False
            except Exception as e:
                logger.error(f"❌ Error running assignment fix: {e}")
                return False
        else:
            logger.info("\n✅ STEP 2: ASSIGNMENTS OK")
            logger.info("-" * 40)
            logger.info("No assignment fixes needed")
        
        # Step 3: Fix profiles
        logger.info("\n🔧 STEP 3: FIXING PROFILES")
        logger.info("-" * 40)
        
        try:
            # Run the profile fix as a subprocess
            result = subprocess.run([
                sys.executable, 
                os.path.join(os.path.dirname(__file__), "simple_profile_fix.py"),
                "--fix-all"
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info("✅ Profile fix completed successfully")
                # Show relevant output lines
                output_lines = result.stdout.split('\n')
                for line in output_lines[-10:]:
                    if line.strip() and ('✅' in line or 'profiles' in line.lower()):
                        logger.info(line.strip())
            else:
                logger.error(f"❌ Profile fix failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Profile fix timed out")
            return False
        except Exception as e:
            logger.error(f"❌ Error running profile fix: {e}")
            return False
        
        # Step 4: Final verification
        logger.info("\n📊 STEP 4: FINAL VERIFICATION")
        logger.info("-" * 40)
        
        async with db_pool.acquire() as conn:
            # Re-check assignment status
            final_assignment_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                               OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
                    COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                               AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments
                FROM file_assignments
            """)
            
            # Re-check profile status
            final_profile_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_profiles,
                    COUNT(CASE WHEN status = 'published' THEN 1 END) as published_profiles,
                    COUNT(CASE WHEN files_count > 0 THEN 1 END) as profiles_with_files,
                    SUM(files_count) as total_profile_files
                FROM pending_user_profile
            """)
            
            logger.info(f"📁 Final Assignment Status:")
            logger.info(f"   Total files: {final_assignment_stats['total_files']}")
            logger.info(f"   Files with miners: {final_assignment_stats['files_with_miners']}")
            logger.info(f"   Empty assignments: {final_assignment_stats['empty_assignments']}")
            
            logger.info(f"\n👤 Final Profile Status:")
            logger.info(f"   Total profiles: {final_profile_stats['total_profiles']}")
            logger.info(f"   Published profiles: {final_profile_stats['published_profiles']}")
            logger.info(f"   Profiles with files: {final_profile_stats['profiles_with_files']}")
            logger.info(f"   Files in profiles: {final_profile_stats['total_profile_files']}")
            
            # Calculate improvements
            assignment_improvement = assignment_stats['empty_assignments'] - final_assignment_stats['empty_assignments']
            profile_improvement = (final_profile_stats['total_profile_files'] or 0) - (profile_stats['total_profile_files'] or 0)
            
            logger.info(f"\n📈 IMPROVEMENTS:")
            logger.info(f"   Empty assignments fixed: {assignment_improvement}")
            logger.info(f"   Profile files added: {profile_improvement}")
            
            # Check if everything is working
            success = (
                final_assignment_stats['empty_assignments'] <= 5 and  # Allow few empty assignments
                final_profile_stats['total_profile_files'] >= final_assignment_stats['files_with_miners'] * 0.95  # 95% coverage
            )
            
            if success:
                logger.info(f"\n🎉 SUCCESS!")
                logger.info(f"   Network is now properly functioning")
                logger.info(f"   File assignments: {final_assignment_stats['files_with_miners']}/{final_assignment_stats['total_files']} have miners")
                logger.info(f"   User profiles: {final_profile_stats['total_profile_files']} files covered")
            else:
                logger.warning(f"\n⚠️ PARTIAL SUCCESS")
                logger.warning(f"   Some issues remain but significant improvements made")
        
        await close_db_pool()
        
        logger.info(f"\n🏁 COMPLETED at: {datetime.now()}")
        return success
        
    except Exception as e:
        logger.error(f"❌ Fatal error during comprehensive fix: {e}")
        logger.exception("Full traceback:")
        return False


async def quick_status_check():
    """Quick status check without fixes."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    (SELECT COUNT(*) FROM file_assignments) as total_files,
                    (SELECT COUNT(*) FROM file_assignments 
                     WHERE miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                           OR miner4 IS NOT NULL OR miner5 IS NOT NULL) as files_with_miners,
                    (SELECT COUNT(*) FROM file_assignments 
                     WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                           AND miner4 IS NULL AND miner5 IS NULL) as empty_assignments,
                    (SELECT COUNT(*) FROM pending_user_profile 
                     WHERE status = 'published') as published_profiles,
                    (SELECT SUM(files_count) FROM pending_user_profile 
                     WHERE status = 'published') as profile_files,
                    (SELECT COUNT(*) FROM registration 
                     WHERE node_type = 'StorageMiner' AND status = 'active') as active_miners
            """)
            
            logger.info("📊 QUICK STATUS CHECK")
            logger.info("=" * 40)
            logger.info(f"📁 Files: {stats['files_with_miners']}/{stats['total_files']} have miners ({stats['empty_assignments']} empty)")
            logger.info(f"👤 Profiles: {stats['published_profiles']} published, {stats['profile_files']} files covered")
            logger.info(f"⛏️ Miners: {stats['active_miners']} active")
            
            # Quick health check
            assignment_health = ((stats['files_with_miners'] / stats['total_files']) * 100) if stats['total_files'] > 0 else 0
            profile_health = ((stats['profile_files'] / stats['files_with_miners']) * 100) if stats['files_with_miners'] > 0 else 0
            
            logger.info(f"\n🏥 Health Scores:")
            logger.info(f"   Assignment coverage: {assignment_health:.1f}%")
            logger.info(f"   Profile coverage: {profile_health:.1f}%")
            
            if assignment_health >= 95 and profile_health >= 95:
                logger.info("✅ System is healthy!")
            elif assignment_health >= 90 and profile_health >= 90:
                logger.info("⚠️ System is mostly healthy with minor issues")
            else:
                logger.info("❌ System needs fixes")
        
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"❌ Error during status check: {e}")


async def main():
    """Main entry point."""
    if len(sys.argv) > 1 and sys.argv[1] == "--status":
        await quick_status_check()
    else:
        success = await run_comprehensive_fix()
        return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 