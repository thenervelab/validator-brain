#!/usr/bin/env python3
"""
Storage capacity diagnostic script to identify why all miners show 0 bytes available.

This script will:
1. Check miner storage capacity vs usage
2. Check node_metrics data for storage info
3. Analyze miner_stats calculations
4. Identify storage calculation issues
"""

import asyncio
import os
import sys
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.connection import init_db_pool, close_db_pool, get_db_pool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def diagnose_storage_issues():
    """Diagnose miner storage capacity issues."""
    
    print("🔍 STORAGE CAPACITY DIAGNOSTIC")
    print("=" * 50)
    
    try:
        # Initialize database
        await init_db_pool()
        db_pool = get_db_pool()
        
        print("1️⃣ CHECKING STORAGE DATA SOURCES:")
        
        # Check node_metrics data
        async with db_pool.acquire() as conn:
            node_metrics_count = await conn.fetchval("SELECT COUNT(*) FROM node_metrics")
            recent_metrics = await conn.fetchval("""
                SELECT COUNT(DISTINCT miner_id) FROM node_metrics 
                WHERE block_number >= (SELECT MAX(block_number) - 100 FROM node_metrics)
            """)
            
            print(f"   Total node_metrics records: {node_metrics_count:,}")
            print(f"   Miners with recent metrics: {recent_metrics}")
            
            # Check storage values in node_metrics
            storage_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_miners,
                    COUNT(CASE WHEN ipfs_storage_max > 0 THEN 1 END) as with_storage_max,
                    COUNT(CASE WHEN ipfs_repo_size > 0 THEN 1 END) as with_repo_size,
                    AVG(CASE WHEN ipfs_storage_max > 0 THEN ipfs_storage_max END) as avg_storage_max,
                    AVG(CASE WHEN ipfs_repo_size > 0 THEN ipfs_repo_size END) as avg_repo_size
                FROM (
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max, ipfs_repo_size
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) latest_metrics
            """)
            
            print(f"\n   NODE_METRICS STORAGE ANALYSIS:")
            print(f"     Total miners: {storage_stats['total_miners']}")
            print(f"     With storage_max > 0: {storage_stats['with_storage_max']}")
            print(f"     With repo_size > 0: {storage_stats['with_repo_size']}")
            print(f"     Avg storage_max: {storage_stats['avg_storage_max']:,.0f} bytes" if storage_stats['avg_storage_max'] else "     Avg storage_max: None")
            print(f"     Avg repo_size: {storage_stats['avg_repo_size']:,.0f} bytes" if storage_stats['avg_repo_size'] else "     Avg repo_size: None")
            
        print("\n2️⃣ CHECKING MINER_STATS DATA:")
        
        async with db_pool.acquire() as conn:
            miner_stats_count = await conn.fetchval("SELECT COUNT(*) FROM miner_stats")
            
            miner_storage_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_miners,
                    COUNT(CASE WHEN health_score >= 20 THEN 1 END) as healthy_miners,
                    AVG(total_files_size_bytes) as avg_files_size,
                    SUM(total_files_size_bytes) as total_files_size,
                    MAX(total_files_size_bytes) as max_files_size
                FROM miner_stats
            """)
            
            print(f"     Total miner_stats records: {miner_stats_count}")
            print(f"     Healthy miners (score >= 20): {miner_storage_stats['healthy_miners']}")
            print(f"     Avg files size per miner: {miner_storage_stats['avg_files_size']:,.0f} bytes" if miner_storage_stats['avg_files_size'] else "     Avg files size: 0")
            print(f"     Total files size across all miners: {miner_storage_stats['total_files_size']:,.0f} bytes" if miner_storage_stats['total_files_size'] else "     Total files size: 0")
            print(f"     Max files size for any miner: {miner_storage_stats['max_files_size']:,.0f} bytes" if miner_storage_stats['max_files_size'] else "     Max files size: 0")
        
        print("\n3️⃣ STORAGE CALCULATION ANALYSIS:")
        
        async with db_pool.acquire() as conn:
            # Get detailed storage calculation for top 10 miners
            detailed_miners = await conn.fetch("""
                SELECT 
                    r.node_id,
                    COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                    COALESCE(nm.ipfs_repo_size, 0) as ipfs_repo_size,
                    COALESCE(ms.total_files_size_bytes, 0) as calculated_size,
                    COALESCE(ms.health_score, 100) as health_score,
                    COALESCE(ms.total_files_pinned, 0) as total_files_pinned,
                    -- Calculate available storage using both methods
                    GREATEST(
                        COALESCE(nm.ipfs_storage_max, 1000000000) - COALESCE(nm.ipfs_repo_size, 0),
                        COALESCE(nm.ipfs_storage_max, 1000000000) - COALESCE(ms.total_files_size_bytes, 0)
                    ) as available_storage_optimistic,
                    LEAST(
                        COALESCE(nm.ipfs_storage_max, 1000000000) - COALESCE(nm.ipfs_repo_size, 0),
                        COALESCE(nm.ipfs_storage_max, 1000000000) - COALESCE(ms.total_files_size_bytes, 0)
                    ) as available_storage_conservative
                FROM registration r
                LEFT JOIN (
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max, ipfs_repo_size
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) nm ON r.node_id = nm.miner_id
                LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                WHERE r.node_type = 'StorageMiner' 
                  AND r.status = 'active'
                  AND COALESCE(ms.health_score, 100) >= 20.0
                ORDER BY COALESCE(ms.total_files_pinned, 0) DESC
                LIMIT 10
            """)
            
            print("     TOP 10 MINERS BY FILE COUNT:")
            for i, miner in enumerate(detailed_miners, 1):
                print(f"\n     {i}. {miner['node_id'][:20]}...")
                print(f"        Storage capacity: {miner['storage_capacity_bytes']:,} bytes")
                print(f"        IPFS repo size: {miner['ipfs_repo_size']:,} bytes")
                print(f"        Calculated files size: {miner['calculated_size']:,} bytes")
                print(f"        Files pinned: {miner['total_files_pinned']}")
                print(f"        Health score: {miner['health_score']:.1f}")
                print(f"        Available (optimistic): {miner['available_storage_optimistic']:,} bytes")
                print(f"        Available (conservative): {miner['available_storage_conservative']:,} bytes")
                
                # Identify the issue
                if miner['storage_capacity_bytes'] <= miner['ipfs_repo_size']:
                    print(f"        ❌ ISSUE: Storage capacity ({miner['storage_capacity_bytes']:,}) <= repo size ({miner['ipfs_repo_size']:,})")
                elif miner['storage_capacity_bytes'] <= miner['calculated_size']:
                    print(f"        ❌ ISSUE: Storage capacity ({miner['storage_capacity_bytes']:,}) <= calculated size ({miner['calculated_size']:,})")
                else:
                    print(f"        ✅ OK: Has available storage")
        
        print("\n4️⃣ STORAGE CAPACITY DISTRIBUTION:")
        
        async with db_pool.acquire() as conn:
            capacity_distribution = await conn.fetch("""
                SELECT 
                    CASE 
                        WHEN COALESCE(nm.ipfs_storage_max, 1000000000) = 1000000000 THEN 'Default (1GB)'
                        WHEN COALESCE(nm.ipfs_storage_max, 1000000000) < 1000000000 THEN 'Under 1GB'
                        WHEN COALESCE(nm.ipfs_storage_max, 1000000000) > 1000000000 THEN 'Over 1GB'
                    END as capacity_range,
                    COUNT(*) as miner_count
                FROM registration r
                LEFT JOIN (
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) nm ON r.node_id = nm.miner_id
                WHERE r.node_type = 'StorageMiner' AND r.status = 'active'
                GROUP BY capacity_range
                ORDER BY miner_count DESC
            """)
            
            print("     CAPACITY DISTRIBUTION:")
            for dist in capacity_distribution:
                print(f"       {dist['capacity_range']}: {dist['miner_count']} miners")
        
        print("\n🎯 DIAGNOSIS SUMMARY:")
        
        # Calculate if the issue is default storage capacity
        async with db_pool.acquire() as conn:
            default_capacity_miners = await conn.fetchval("""
                SELECT COUNT(*) FROM registration r
                LEFT JOIN (
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) nm ON r.node_id = nm.miner_id
                WHERE r.node_type = 'StorageMiner' 
                  AND r.status = 'active'
                  AND COALESCE(nm.ipfs_storage_max, 1000000000) = 1000000000
            """)
            
            total_active_miners = await conn.fetchval("""
                SELECT COUNT(*) FROM registration 
                WHERE node_type = 'StorageMiner' AND status = 'active'
            """)
            
            if default_capacity_miners / total_active_miners > 0.8:
                print("   ❌ CRITICAL: Most miners using default 1GB capacity!")
                print("      - 1GB is very small for storage miners")
                print("      - Miners likely exceeded their capacity")
                print("      - Need to increase storage capacity or clear old files")
            else:
                print("   ✅ Storage capacities seem reasonable")
                print("   🔍 Need to investigate why available storage is 0")
        
    except Exception as e:
        print(f"❌ Error during storage diagnostic: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(diagnose_storage_issues()) 