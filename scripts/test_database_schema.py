#!/usr/bin/env python3
"""Test database schema to understand timestamp format."""

import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

async def check_schema():
    """Check database schema for timestamp format."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Check registration table schema
            schema = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'registration'
                ORDER BY ordinal_position
            """)
            
            print("registration table schema:")
            for col in schema:
                print(f"  {col['column_name']}: {col['data_type']} ({'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
            
            # Check sample data
            sample = await conn.fetchrow("SELECT node_id, registered_at FROM registration LIMIT 1")
            if sample:
                print(f"\nSample data:")
                print(f"  node_id: {sample['node_id']}")
                print(f"  registered_at: {sample['registered_at']} (type: {type(sample['registered_at'])})")
            
            # Check if it's Unix timestamp
            if sample and isinstance(sample['registered_at'], int):
                from datetime import datetime
                converted = datetime.fromtimestamp(sample['registered_at'])
                print(f"  converted to datetime: {converted}")
            
        await close_db_pool()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_schema()) 