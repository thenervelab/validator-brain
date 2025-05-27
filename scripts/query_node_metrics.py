"""
Query node metrics from the database.

This script provides examples of how to query the node_metrics table.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import asyncpg

# Load environment variables
load_dotenv()

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/substrate_fetcher')


async def get_latest_metrics():
    """Get the latest metrics for all miners."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get the latest block number
        latest_block = await conn.fetchval("""
            SELECT MAX(block_number) FROM node_metrics
        """)
        
        print(f"Latest block with metrics: {latest_block}")
        print("-" * 80)
        
        # Get all metrics from the latest block
        rows = await conn.fetch("""
            SELECT 
                miner_id,
                ipfs_repo_size,
                ipfs_storage_max,
                block_number,
                created_at
            FROM node_metrics
            WHERE block_number = $1
            ORDER BY miner_id
        """, latest_block)
        
        print(f"Found {len(rows)} miners with metrics at block {latest_block}")
        print("-" * 80)
        
        # Show first 10 miners
        for i, row in enumerate(rows[:10]):
            repo_size_gb = row['ipfs_repo_size'] / (1024**3)
            storage_max_gb = row['ipfs_storage_max'] / (1024**3)
            usage_percent = (row['ipfs_repo_size'] / row['ipfs_storage_max']) * 100 if row['ipfs_storage_max'] > 0 else 0
            
            print(f"Miner: {row['miner_id']}")
            print(f"  Repo Size: {repo_size_gb:.2f} GB")
            print(f"  Max Storage: {storage_max_gb:.2f} GB")
            print(f"  Usage: {usage_percent:.2f}%")
            print(f"  Block: {row['block_number']}")
            print(f"  Timestamp: {row['created_at']}")
            print()
        
        if len(rows) > 10:
            print(f"... and {len(rows) - 10} more miners")
        
    finally:
        await conn.close()


async def get_miner_history(miner_id: str, limit: int = 10):
    """Get historical metrics for a specific miner."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        rows = await conn.fetch("""
            SELECT 
                ipfs_repo_size,
                ipfs_storage_max,
                block_number,
                created_at
            FROM node_metrics
            WHERE miner_id = $1
            ORDER BY block_number DESC
            LIMIT $2
        """, miner_id, limit)
        
        if not rows:
            print(f"No metrics found for miner {miner_id}")
            return
        
        print(f"Metrics history for miner {miner_id} (last {limit} blocks):")
        print("-" * 80)
        
        for row in rows:
            repo_size_gb = row['ipfs_repo_size'] / (1024**3)
            storage_max_gb = row['ipfs_storage_max'] / (1024**3)
            usage_percent = (row['ipfs_repo_size'] / row['ipfs_storage_max']) * 100 if row['ipfs_storage_max'] > 0 else 0
            
            print(f"Block {row['block_number']} ({row['created_at']})")
            print(f"  Repo: {repo_size_gb:.2f} GB / {storage_max_gb:.2f} GB ({usage_percent:.2f}%)")
        
    finally:
        await conn.close()


async def get_storage_stats():
    """Get aggregate storage statistics."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get the latest block number
        latest_block = await conn.fetchval("""
            SELECT MAX(block_number) FROM node_metrics
        """)
        
        # Get aggregate stats for the latest block
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as miner_count,
                SUM(ipfs_repo_size) as total_repo_size,
                SUM(ipfs_storage_max) as total_storage_max,
                AVG(ipfs_repo_size) as avg_repo_size,
                AVG(ipfs_storage_max) as avg_storage_max,
                MIN(ipfs_repo_size) as min_repo_size,
                MAX(ipfs_repo_size) as max_repo_size
            FROM node_metrics
            WHERE block_number = $1
        """, latest_block)
        
        print(f"Storage Statistics at block {latest_block}:")
        print("-" * 80)
        print(f"Total Miners: {stats['miner_count']}")
        print(f"Total Repo Size: {stats['total_repo_size'] / (1024**4):.2f} TB")
        print(f"Total Storage Capacity: {stats['total_storage_max'] / (1024**4):.2f} TB")
        print(f"Average Repo Size: {stats['avg_repo_size'] / (1024**3):.2f} GB")
        print(f"Average Storage Capacity: {stats['avg_storage_max'] / (1024**3):.2f} GB")
        print(f"Min Repo Size: {stats['min_repo_size'] / (1024**3):.2f} GB")
        print(f"Max Repo Size: {stats['max_repo_size'] / (1024**3):.2f} GB")
        
        total_usage_percent = (stats['total_repo_size'] / stats['total_storage_max']) * 100
        print(f"Overall Network Usage: {total_usage_percent:.2f}%")
        
    finally:
        await conn.close()


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Query node metrics from database')
    parser.add_argument('--miner', help='Get history for specific miner ID')
    parser.add_argument('--stats', action='store_true', help='Show aggregate statistics')
    parser.add_argument('--limit', type=int, default=10, help='Limit for history queries')
    args = parser.parse_args()
    
    if args.miner:
        await get_miner_history(args.miner, args.limit)
    elif args.stats:
        await get_storage_stats()
    else:
        await get_latest_metrics()


if __name__ == "__main__":
    asyncio.run(main()) 