#!/usr/bin/env python3
"""Test the fixed profile rebuilding functionality."""

import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

async def test_profile_rebuild():
    """Test the profile rebuild functionality."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        from app.utils.blockchain_submission import rebuild_user_profiles_simple, collect_miner_profiles_for_submission
        
        await init_db_pool()
        db_pool = await get_db_pool()
        
        print("🧪 Testing profile rebuild functionality...")
        
        # Test user profile rebuilding
        print("\n1. Testing user profile rebuild:")
        user_count = await rebuild_user_profiles_simple(db_pool)
        print(f"   ✅ Rebuilt {user_count} user profiles")
        
        # Test miner profile collection
        print("\n2. Testing miner profile collection:")
        miner_profiles = await collect_miner_profiles_for_submission(db_pool)
        print(f"   ✅ Collected {len(miner_profiles)} miner profiles")
        
        # Verify profiles were created
        async with db_pool.acquire() as conn:
            user_profiles = await conn.fetchval("SELECT COUNT(*) FROM pending_user_profile")
            miner_profiles_count = await conn.fetchval("SELECT COUNT(*) FROM pending_miner_profile")
            
        print(f"\n📊 Results:")
        print(f"   User profiles in table: {user_profiles}")
        print(f"   Miner profiles in table: {miner_profiles_count}")
        
        if user_count > 0 and len(miner_profiles) > 0:
            print("\n✅ SUCCESS: Profile rebuilding works correctly!")
        else:
            print("\n⚠️ WARNING: No profiles found, but no errors occurred")
        
        await close_db_pool()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_profile_rebuild()) 