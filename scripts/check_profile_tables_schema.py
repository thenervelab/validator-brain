#!/usr/bin/env python3
"""Check the schema of pending profile tables to fix updated_at issues."""

import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

async def check_profile_schemas():
    """Check database schema for pending profile tables."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Check pending_user_profile table schema
            user_schema = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'pending_user_profile'
                ORDER BY ordinal_position
            """)
            
            print("pending_user_profile table schema:")
            for col in user_schema:
                print(f"  {col['column_name']}: {col['data_type']} ({'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
            
            # Check pending_miner_profile table schema
            miner_schema = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'pending_miner_profile'
                ORDER BY ordinal_position
            """)
            
            print("\npending_miner_profile table schema:")
            for col in miner_schema:
                print(f"  {col['column_name']}: {col['data_type']} ({'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
            
        await close_db_pool()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_profile_schemas()) 