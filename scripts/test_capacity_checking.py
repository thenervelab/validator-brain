#!/usr/bin/env python3
"""
Test Capacity Checking

Test script to verify that the file assignment logic properly checks storage capacity.
"""

import asyncio
import logging
import os
import sys
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


async def test_capacity_logic():
    """Test the capacity checking logic."""
    try:
        from app.db.connection import init_db_pool, get_db_pool
        
        # Initialize database
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Get sample files with various sizes
            files = await conn.fetch("""
                SELECT 
                    f.cid,
                    f.name,
                    f.size,
                    fa.owner
                FROM files f
                LEFT JOIN file_assignments fa ON f.cid = fa.cid
                WHERE f.size IS NOT NULL
                ORDER BY f.size DESC
                LIMIT 10
            """)
            
            # Get sample miners with storage info
            miners = await conn.fetch("""
                SELECT 
                    r.node_id,
                    COALESCE(nm.ipfs_storage_max, 1000000000) as storage_max,
                    COALESCE(nm.ipfs_repo_size, 0) as storage_used,
                    COALESCE(nm.ipfs_storage_max, 1000000000) - COALESCE(nm.ipfs_repo_size, 0) as available_space
                FROM registration r
                LEFT JOIN (
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max, ipfs_repo_size
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) nm ON r.node_id = nm.miner_id
                WHERE r.node_type = 'StorageMiner' 
                  AND r.status = 'active'
                ORDER BY available_space DESC
                LIMIT 20
            """)
            
            print("🧪 CAPACITY CHECKING TEST")
            print("=" * 60)
            
            print(f"\n📁 Sample Files (largest first):")
            for file in files:
                size_mb = (file['size'] or 0) / 1_000_000
                print(f"  {file['name'][:40]:<40} {size_mb:>8.1f} MB")
            
            print(f"\n⛏️ Sample Miners (most space first):")
            for miner in miners:
                available_gb = miner['available_space'] / 1_000_000_000
                used_gb = miner['storage_used'] / 1_000_000_000
                total_gb = miner['storage_max'] / 1_000_000_000
                print(f"  {miner['node_id'][:20]:<20} {available_gb:>6.1f} GB free / {total_gb:>6.1f} GB total")
            
            print(f"\n🔍 Testing Assignment Logic:")
            print("-" * 60)
            
            # Test assignment logic for different file sizes
            test_file_sizes = [
                100_000,      # 100 KB
                1_000_000,    # 1 MB  
                10_000_000,   # 10 MB
                100_000_000,  # 100 MB
                1_000_000_000 # 1 GB
            ]
            
            for file_size in test_file_sizes:
                print(f"\n📄 Testing file size: {file_size:,} bytes ({file_size/1_000_000:.1f} MB)")
                
                # Calculate required space (file + 20% margin)
                safety_margin = int(file_size * 0.2)
                required_space = file_size + safety_margin
                print(f"   Required space: {required_space:,} bytes ({required_space/1_000_000:.1f} MB)")
                
                # Count suitable miners
                suitable_miners = []
                for miner in miners:
                    if miner['available_space'] >= required_space:
                        suitable_miners.append(miner['node_id'])
                
                print(f"   Suitable miners: {len(suitable_miners)}/{len(miners)}")
                if suitable_miners:
                    print(f"   Examples: {', '.join(suitable_miners[:3])}{'...' if len(suitable_miners) > 3 else ''}")
                else:
                    print("   ⚠️ NO suitable miners found!")
            
            # Test with actual files from database
            print(f"\n📂 Testing with actual files from database:")
            print("-" * 60)
            
            for file in files[:5]:  # Test first 5 files
                file_size = file['size'] or 0
                if file_size == 0:
                    continue
                    
                safety_margin = int(file_size * 0.2)
                required_space = file_size + safety_margin
                
                suitable_count = sum(1 for m in miners if m['available_space'] >= required_space)
                
                print(f"📄 {file['name'][:30]:<30} {file_size/1_000_000:>6.1f} MB → {suitable_count:>2} suitable miners")
            
            print(f"\n✅ Capacity checking test completed!")
            
        # Close database
        from app.db.connection import close_db_pool
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"❌ Error during capacity test: {e}")
        logger.exception("Full traceback:")


async def main():
    """Main entry point."""
    await test_capacity_logic()


if __name__ == "__main__":
    asyncio.run(main()) 