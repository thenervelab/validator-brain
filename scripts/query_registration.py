"""
Query registration data from the database.

This script provides examples of how to query the registration table.
"""

import asyncio
import os
import sys
from datetime import datetime

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import asyncpg

# Load environment variables
load_dotenv()

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/substrate_fetcher')


async def get_registration_stats():
    """Get registration statistics."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get total count
        total_count = await conn.fetchval("SELECT COUNT(*) FROM registration")
        print(f"Total registrations: {total_count}")
        
        # Get count by node type
        type_stats = await conn.fetch("""
            SELECT node_type, COUNT(*) as count
            FROM registration
            GROUP BY node_type
            ORDER BY count DESC
        """)
        
        print("\nRegistrations by node type:")
        for row in type_stats:
            print(f"  {row['node_type']}: {row['count']}")
        
        # Get count by status
        status_stats = await conn.fetch("""
            SELECT status, COUNT(*) as count
            FROM registration
            GROUP BY status
            ORDER BY count DESC
        """)
        
        print("\nRegistrations by status:")
        for row in status_stats:
            print(f"  {row['status']}: {row['count']}")
        
        # Get sample records
        sample_records = await conn.fetch("""
            SELECT node_id, ipfs_peer_id, node_type, status, registered_at
            FROM registration
            ORDER BY registered_at DESC
            LIMIT 5
        """)
        
        print("\nSample registrations (latest 5):")
        for row in sample_records:
            print(f"  {row['node_id'][:20]}... | {row['ipfs_peer_id'][:20]}... | {row['node_type']} | {row['status']} | Block {row['registered_at']}")
        
    finally:
        await conn.close()


async def main():
    """Main entry point."""
    try:
        await get_registration_stats()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 