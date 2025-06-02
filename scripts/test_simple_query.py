#!/usr/bin/env python3
"""Test simple database query to isolate the parameter issue."""

import asyncio
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

async def test_query():
    """Test the exact query that's failing."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            print("Testing database query...")
            
            # Test basic date query first
            cutoff_date = datetime.now() - timedelta(days=1)
            print(f"Cutoff date: {cutoff_date}")
            print(f"Cutoff date type: {type(cutoff_date)}")
            
            # Test simple query
            result = await conn.fetchval("SELECT COUNT(*) FROM registration WHERE registered_at <= $1", cutoff_date)
            print(f"Simple date query result: {result}")
            
            # Test with health score parameter
            try:
                result2 = await conn.fetchval("""
                    SELECT COUNT(*) FROM registration r 
                    LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                    WHERE r.registered_at <= $1 
                      AND COALESCE(ms.health_score, 100) >= $2
                """, cutoff_date, 50)
                print(f"Two parameter query result: {result2}")
            except Exception as e:
                print(f"Two parameter query failed: {e}")
                print(f"Error type: {type(e)}")
            
            # Check if there are any miners at all
            total_miners = await conn.fetchval("SELECT COUNT(*) FROM registration WHERE node_type = 'StorageMiner'")
            print(f"Total storage miners: {total_miners}")
            
        await close_db_pool()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_query()) 