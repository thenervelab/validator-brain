#!/usr/bin/env python3
"""
Simple Profile Reconstruction Fix

Fix user profiles that show 0 miners by rebuilding them from file assignments.
This addresses the gap between file assignments and profile data.
"""

import asyncio
import logging
import os
import sys
import json
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


class SimpleProfileReconstructor:
    def __init__(self):
        self.db_pool = None
        
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
    
    async def get_users_with_assignments(self):
        """Get all users who have file assignments."""
        try:
            async with self.db_pool.acquire() as conn:
                users = await conn.fetch("""
                    SELECT 
                        owner,
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                   OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners
                    FROM file_assignments
                    GROUP BY owner
                    HAVING COUNT(*) > 0
                    ORDER BY COUNT(*) DESC
                """)
                
                logger.info(f"📊 Found {len(users)} users with file assignments")
                return users
                
        except Exception as e:
            logger.error(f"❌ Error getting users: {e}")
            return []
    
    async def build_user_profile_simple(self, owner):
        """Build a user profile using simple, reliable logic."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get all files for this user with their assignments
                user_files = await conn.fetch("""
                    SELECT 
                        f.cid,
                        f.name,
                        f.size,
                        fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE fa.owner = $1
                      AND f.size IS NOT NULL
                      AND (fa.miner1 IS NOT NULL OR fa.miner2 IS NOT NULL OR fa.miner3 IS NOT NULL 
                           OR fa.miner4 IS NOT NULL OR fa.miner5 IS NOT NULL)
                    ORDER BY f.name
                """, owner)
                
                if not user_files:
                    logger.warning(f"⚠️ No files with miners found for user {owner}")
                    return None
                
                # Build simple profile structure
                profile_files = []
                total_size = 0
                all_miners = set()
                
                for file_info in user_files:
                    # Get miners for this file
                    file_miners = []
                    for miner in [file_info['miner1'], file_info['miner2'], file_info['miner3'], 
                                 file_info['miner4'], file_info['miner5']]:
                        if miner:
                            file_miners.append(miner)
                            all_miners.add(miner)
                    
                    if file_miners:  # Only include files that have miners
                        profile_files.append({
                            "cid": file_info['cid'],
                            "name": file_info['name'],
                            "size": file_info['size'],
                            "miners": file_miners
                        })
                        total_size += file_info['size']
                
                if not profile_files:
                    logger.warning(f"⚠️ No valid files found for user {owner}")
                    return None
                
                # Create profile data
                profile_data = {
                    "owner": owner,
                    "files": profile_files,
                    "summary": {
                        "files_count": len(profile_files),
                        "files_size": total_size,
                        "unique_miners": len(all_miners),
                        "miners": sorted(list(all_miners))
                    },
                    "created_at": "auto-generated",
                    "version": "simple-v1"
                }
                
                return profile_data
                
        except Exception as e:
            logger.error(f"❌ Error building profile for {owner}: {e}")
            return None
    
    async def create_mock_ipfs_cid(self, profile_data):
        """Create a deterministic mock CID for the profile."""
        import hashlib
        
        # Create a simple hash of the profile content
        content = json.dumps(profile_data, sort_keys=True)
        hash_obj = hashlib.sha256(content.encode('utf-8'))
        hash_hex = hash_obj.hexdigest()
        
        # Format as a mock IPFS CID (Qm prefix + base58-like)
        mock_cid = f"Qm{hash_hex[:44]}"
        return mock_cid
    
    async def update_user_profile_in_db(self, owner, profile_data):
        """Update or create user profile in database."""
        try:
            async with self.db_pool.acquire() as conn:
                # Create mock CID
                profile_cid = await self.create_mock_ipfs_cid(profile_data)
                
                files_count = profile_data['summary']['files_count']
                files_size = profile_data['summary']['files_size']
                
                # Update or insert profile
                await conn.execute("""
                    INSERT INTO pending_user_profile (
                        owner, cid, files_count, files_size, status, published_at, updated_at
                    )
                    VALUES ($1, $2, $3, $4, 'published', NOW(), NOW())
                    ON CONFLICT (owner) DO UPDATE SET
                        cid = $2,
                        files_count = $3,
                        files_size = $4,
                        status = 'published',
                        published_at = NOW(),
                        updated_at = NOW()
                """, owner, profile_cid, files_count, files_size)
                
                logger.info(f"✅ Updated profile for {owner}: {files_count} files, {files_size} bytes")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error updating profile for {owner}: {e}")
            return False
    
    async def fix_all_user_profiles(self):
        """Fix all user profiles by rebuilding from assignments."""
        try:
            users = await self.get_users_with_assignments()
            
            if not users:
                logger.info("No users found with assignments")
                return True, 0
            
            logger.info(f"🔧 Rebuilding profiles for {len(users)} users...")
            
            success_count = 0
            
            for i, user_info in enumerate(users, 1):
                owner = user_info['owner']
                
                logger.info(f"\n{'='*50}")
                logger.info(f"Processing {i}/{len(users)}: {owner}")
                logger.info(f"  Files: {user_info['total_files']}, With miners: {user_info['files_with_miners']}")
                
                # Build profile
                profile_data = await self.build_user_profile_simple(owner)
                
                if profile_data:
                    # Update database
                    success = await self.update_user_profile_in_db(owner, profile_data)
                    if success:
                        success_count += 1
                        logger.info(f"  ✅ Profile rebuilt: {profile_data['summary']['files_count']} files, "
                                   f"{profile_data['summary']['unique_miners']} miners")
                    else:
                        logger.error(f"  ❌ Failed to update profile in database")
                else:
                    logger.warning(f"  ⚠️ Could not build valid profile")
                
                # Brief pause
                if i % 10 == 0:
                    await asyncio.sleep(1)
            
            logger.info(f"\n{'='*50}")
            logger.info(f"✅ Profile reconstruction complete: {success_count}/{len(users)} profiles fixed")
            
            return success_count == len(users), success_count
            
        except Exception as e:
            logger.error(f"❌ Error fixing profiles: {e}")
            return False, 0
    
    async def fix_specific_user_profile(self, owner):
        """Fix profile for a specific user."""
        try:
            logger.info(f"🔧 Rebuilding profile for user: {owner}")
            
            # Build profile
            profile_data = await self.build_user_profile_simple(owner)
            
            if not profile_data:
                logger.error(f"❌ Could not build profile for {owner}")
                return False
            
            # Update database
            success = await self.update_user_profile_in_db(owner, profile_data)
            
            if success:
                logger.info(f"✅ Profile rebuilt for {owner}:")
                logger.info(f"   Files: {profile_data['summary']['files_count']}")
                logger.info(f"   Size: {profile_data['summary']['files_size']} bytes") 
                logger.info(f"   Miners: {profile_data['summary']['unique_miners']}")
                return True
            else:
                logger.error(f"❌ Failed to update profile for {owner}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error fixing profile for {owner}: {e}")
            return False
    
    async def check_profile_status(self):
        """Check current profile status."""
        try:
            async with self.db_pool.acquire() as conn:
                stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_profiles,
                        COUNT(CASE WHEN status = 'published' THEN 1 END) as published_profiles,
                        COUNT(CASE WHEN files_count > 0 THEN 1 END) as profiles_with_files,
                        COUNT(CASE WHEN files_count = 0 OR files_count IS NULL THEN 1 END) as zero_file_profiles,
                        SUM(files_count) as total_profile_files,
                        SUM(files_size) as total_profile_size
                    FROM pending_user_profile
                """)
                
                logger.info("📊 Profile Status:")
                logger.info(f"   Total profiles: {stats['total_profiles']}")
                logger.info(f"   Published profiles: {stats['published_profiles']}")
                logger.info(f"   Profiles with files: {stats['profiles_with_files']}")
                logger.info(f"   Zero-file profiles: {stats['zero_file_profiles']}")
                logger.info(f"   Total files in profiles: {stats['total_profile_files']}")
                logger.info(f"   Total size in profiles: {stats['total_profile_size']} bytes")
                
                # Compare with assignments
                assignment_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                   OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners
                    FROM file_assignments
                """)
                
                logger.info("\n📊 Assignment Comparison:")
                logger.info(f"   Total file assignments: {assignment_stats['total_files']}")
                logger.info(f"   Assignments with miners: {assignment_stats['files_with_miners']}")
                logger.info(f"   Files in profiles: {stats['total_profile_files']}")
                
                if stats['total_profile_files'] and assignment_stats['files_with_miners']:
                    coverage = (stats['total_profile_files'] / assignment_stats['files_with_miners']) * 100
                    logger.info(f"   Profile coverage: {coverage:.1f}%")
                
        except Exception as e:
            logger.error(f"❌ Error checking profile status: {e}")
    
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
    logger.info("🔧 Simple Profile Reconstruction Tool")
    logger.info("=" * 60)
    
    reconstructor = SimpleProfileReconstructor()
    
    try:
        # Initialize
        success = await reconstructor.initialize()
        if not success:
            logger.error("Failed to initialize")
            return 1
        
        # Parse command line arguments
        if len(sys.argv) > 1:
            arg = sys.argv[1]
            
            if arg == "--fix-all":
                # Fix all user profiles
                success, count = await reconstructor.fix_all_user_profiles()
                logger.info(f"\n{'='*60}")
                if success:
                    logger.info(f"✅ Successfully fixed {count} user profiles")
                else:
                    logger.error(f"❌ Fixed {count} profiles but some failed")
                
            elif arg == "--status":
                # Check profile status
                await reconstructor.check_profile_status()
                
            else:
                # Fix specific user
                owner = arg
                success = await reconstructor.fix_specific_user_profile(owner)
                if success:
                    logger.info(f"✅ Successfully fixed profile for: {owner}")
                else:
                    logger.error(f"❌ Failed to fix profile for: {owner}")
        else:
            # Interactive mode
            print("\nOptions:")
            print("1. Fix all user profiles")
            print("2. Check profile status")
            print("3. Fix specific user profile")
            
            choice = input("Choose option (1-3): ").strip()
            
            if choice == "1":
                success, count = await reconstructor.fix_all_user_profiles()
                if success:
                    logger.info(f"✅ Successfully fixed {count} user profiles")
                else:
                    logger.error(f"❌ Fixed {count} profiles but some failed")
                    
            elif choice == "2":
                await reconstructor.check_profile_status()
                
            elif choice == "3":
                owner = input("Enter user owner address: ").strip()
                success = await reconstructor.fix_specific_user_profile(owner)
                if success:
                    logger.info(f"✅ Successfully fixed profile for: {owner}")
                else:
                    logger.error(f"❌ Failed to fix profile for: {owner}")
            else:
                logger.error("Invalid choice")
                return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        logger.exception("Full traceback:")
        return 1
    finally:
        await reconstructor.cleanup()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 