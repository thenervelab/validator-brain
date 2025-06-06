#!/usr/bin/env python3
"""
Diagnostic script to identify why file assignment processor is not processing assignments.

This script will:
1. Check available miners and their criteria
2. Check files needing reassignment 
3. Check storage capacity requirements
4. Check environment variable constraints
5. Simulate the assignment process to find bottlenecks
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.connection import init_db_pool, close_db_pool, get_db_pool
from app.utils.config import NODE_URL
from app.utils.epoch_validator import calculate_epoch_from_block
from substrateinterface import SubstrateInterface
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def check_assignment_bottlenecks():
    """Comprehensive diagnostic of assignment bottlenecks."""
    
    print("🔍 ASSIGNMENT DIAGNOSTIC REPORT")
    print("=" * 60)
    
    try:
        # Initialize database
        await init_db_pool()
        db_pool = await get_db_pool()
        
        # Get current epoch
        substrate = SubstrateInterface(url=NODE_URL)
        block_number = substrate.get_block_number(None)
        current_epoch = calculate_epoch_from_block(block_number)
        substrate.close()
        
        print(f"📅 Current epoch: {current_epoch}")
        print(f"📅 Current block: {block_number}")
        print()
        
        # 1. CHECK ENVIRONMENT VARIABLES
        print("1️⃣ ENVIRONMENT CONFIGURATION:")
        min_required_miners = int(os.getenv('MIN_REQUIRED_MINERS', '5'))
        min_health_score = float(os.getenv('MIN_MINER_HEALTH_SCORE', '20.0'))
        replicas_per_file = int(os.getenv('REPLICAS_PER_FILE', '5'))
        max_files_per_batch = int(os.getenv('MAX_FILES_PER_BATCH', '100'))
        max_reassignments_per_batch = int(os.getenv('MAX_REASSIGNMENTS_PER_BATCH', '50'))
        
        print(f"   MIN_REQUIRED_MINERS: {min_required_miners}")
        print(f"   MIN_MINER_HEALTH_SCORE: {min_health_score}")
        print(f"   REPLICAS_PER_FILE: {replicas_per_file}")
        print(f"   MAX_FILES_PER_BATCH: {max_files_per_batch}")
        print(f"   MAX_REASSIGNMENTS_PER_BATCH: {max_reassignments_per_batch}")
        print()
        
        # 2. CHECK FILES NEEDING REASSIGNMENT
        print("2️⃣ FILES NEEDING REASSIGNMENT:")
        async with db_pool.acquire() as conn:
            null_files = await conn.fetch("""
                SELECT 
                    fa.cid,
                    f.name as filename,
                    f.size as file_size_bytes,
                    fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5,
                    (CASE WHEN fa.miner1 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner2 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner3 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner4 IS NULL THEN 1 ELSE 0 END +
                     CASE WHEN fa.miner5 IS NULL THEN 1 ELSE 0 END) as null_count
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL OR 
                       fa.miner4 IS NULL OR fa.miner5 IS NULL)
                ORDER BY null_count DESC, fa.updated_at ASC
                LIMIT 10
            """)
            
            print(f"   Total files with NULL miners: {len(null_files)}")
            if null_files:
                print("   Sample files with NULL miners:")
                for i, file_info in enumerate(null_files[:5], 1):
                    print(f"     {i}. CID: {file_info['cid'][:20]}...")
                    print(f"        Size: {file_info['file_size_bytes']:,} bytes")
                    print(f"        NULL slots: {file_info['null_count']}/5")
                    print(f"        Current miners: {[m for m in [file_info['miner1'], file_info['miner2'], file_info['miner3'], file_info['miner4'], file_info['miner5']] if m is not None]}")
            print()
        
        # 3. CHECK AVAILABLE MINERS
        print("3️⃣ AVAILABLE MINERS ANALYSIS:")
        async with db_pool.acquire() as conn:
            # Get all miners with their status
            all_miners = await conn.fetch("""
                SELECT 
                    r.node_id,
                    r.status,
                    r.node_type,
                    COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                    COALESCE(nm.ipfs_repo_size, 0) as used_storage_bytes,
                    COALESCE(ms.health_score, 100) as health_score,
                    COALESCE(ms.last_online_block, 0) as last_online_block,
                    COALESCE(ms.total_files_pinned, 0) as total_files_pinned,
                    COALESCE(ms.total_files_size_bytes, 0) as total_files_size_bytes
                FROM registration r
                LEFT JOIN (
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max, ipfs_repo_size
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) nm ON r.node_id = nm.miner_id
                LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                WHERE r.node_type = 'StorageMiner'
                ORDER BY r.node_id
            """)
            
            print(f"   Total StorageMiner registrations: {len(all_miners)}")
            
            # Filter by status
            active_miners = [m for m in all_miners if m['status'] == 'active']
            print(f"   Active miners: {len(active_miners)}")
            
            # Filter by health score
            healthy_miners = [m for m in active_miners if m['health_score'] >= min_health_score]
            print(f"   Healthy miners (score >= {min_health_score}): {len(healthy_miners)}")
            
            # Filter by recent activity
            recent_block_threshold = current_epoch * 100 - 1000
            recent_miners = [m for m in healthy_miners if m['last_online_block'] == 0 or m['last_online_block'] >= recent_block_threshold]
            print(f"   Recent miners (last_online_block >= {recent_block_threshold}): {len(recent_miners)}")
            
            print()
            print("   MINER FILTERING BREAKDOWN:")
            print(f"     Total registrations: {len(all_miners)}")
            print(f"     ↓ Active status: {len(active_miners)} (-{len(all_miners) - len(active_miners)})")
            print(f"     ↓ Health score >= {min_health_score}: {len(healthy_miners)} (-{len(active_miners) - len(healthy_miners)})")
            print(f"     ↓ Recent activity: {len(recent_miners)} (-{len(healthy_miners) - len(recent_miners)})")
            
            if recent_miners:
                print()
                print("   TOP 5 AVAILABLE MINERS:")
                for i, miner in enumerate(recent_miners[:5], 1):
                    available_storage = max(0, miner['storage_capacity_bytes'] - miner['used_storage_bytes'])
                    print(f"     {i}. {miner['node_id'][:20]}...")
                    print(f"        Health: {miner['health_score']:.1f}")
                    print(f"        Storage: {available_storage:,} bytes available")
                    print(f"        Files: {miner['total_files_pinned']} pinned")
            print()
        
        # 4. CHECK STORAGE CAPACITY ISSUES
        print("4️⃣ STORAGE CAPACITY ANALYSIS:")
        if recent_miners and null_files:
            # Take a sample file and check how many miners can handle it
            sample_file = null_files[0]
            file_size = sample_file['file_size_bytes'] or 0
            
            # Add 20% safety margin like the processor does
            safety_margin = int(file_size * 0.2)
            required_space = file_size + safety_margin
            
            print(f"   Sample file size: {file_size:,} bytes")
            print(f"   Required space (with 20% margin): {required_space:,} bytes")
            
            suitable_miners = []
            for miner in recent_miners:
                available_storage = max(0, miner['storage_capacity_bytes'] - miner['used_storage_bytes'])
                if available_storage >= required_space:
                    suitable_miners.append(miner)
            
            print(f"   Miners with sufficient storage: {len(suitable_miners)}")
            print(f"   Required miners per file: {min_required_miners}")
            
            if len(suitable_miners) < min_required_miners:
                print(f"   ❌ BOTTLENECK: Only {len(suitable_miners)} suitable miners, need {min_required_miners}")
                print("      SOLUTIONS:")
                print("      - Increase storage capacity on miners")
                print("      - Lower MIN_REQUIRED_MINERS environment variable")
                print("      - Clear old files from miners")
            else:
                print(f"   ✅ Sufficient miners available for assignments")
            print()
        
        # 5. CHECK HEALTH DATA AVAILABILITY
        print("5️⃣ HEALTH DATA ANALYSIS:")
        async with db_pool.acquire() as conn:
            current_health_data = await conn.fetchval("""
                SELECT COUNT(DISTINCT node_id) 
                FROM miner_epoch_health 
                WHERE epoch = $1
            """, current_epoch)
            
            recent_health_data = await conn.fetchval("""
                SELECT COUNT(DISTINCT node_id) 
                FROM miner_epoch_health 
                WHERE epoch >= $1 - 2
            """, current_epoch)
            
            print(f"   Current epoch ({current_epoch}) health data: {current_health_data} miners")
            print(f"   Recent epochs health data: {recent_health_data} miners")
            
            if current_health_data < 10:
                print("   ⚠️ WARNING: Low current epoch health data")
            print()
        
        # 6. SIMULATE ASSIGNMENT PROCESS
        print("6️⃣ ASSIGNMENT SIMULATION:")
        if recent_miners and null_files:
            sample_files = null_files[:3]  # Test with first 3 files
            successful_sims = 0
            failed_sims = 0
            
            for file_info in sample_files:
                file_size = file_info['file_size_bytes'] or 0
                required_space = file_size + int(file_size * 0.2)
                
                # Get current miners for this file
                current_miners = [m for m in [file_info['miner1'], file_info['miner2'], 
                                            file_info['miner3'], file_info['miner4'], 
                                            file_info['miner5']] if m is not None]
                
                # Find available miners (exclude current ones)
                available_for_file = []
                for miner in recent_miners:
                    if miner['node_id'] not in current_miners:
                        available_storage = max(0, miner['storage_capacity_bytes'] - miner['used_storage_bytes'])
                        if available_storage >= required_space:
                            available_for_file.append(miner)
                
                empty_slots = 5 - len(current_miners)
                
                print(f"   File {file_info['cid'][:16]}... (size: {file_size:,} bytes)")
                print(f"     Empty slots: {empty_slots}")
                print(f"     Available miners: {len(available_for_file)}")
                print(f"     Can fill slots: {'YES' if len(available_for_file) >= empty_slots else 'NO'}")
                
                if len(available_for_file) >= empty_slots:
                    successful_sims += 1
                else:
                    failed_sims += 1
            
            print()
            print(f"   SIMULATION RESULTS:")
            print(f"     ✅ Files that can be assigned: {successful_sims}")
            print(f"     ❌ Files that cannot be assigned: {failed_sims}")
        
        print()
        print("🎯 RECOMMENDATIONS:")
        if len(recent_miners) == 0:
            print("   1. ❌ CRITICAL: No available miners found!")
            print("      - Check miner health scores and registration status")
            print("      - Verify health check system is working")
            print("      - Consider lowering MIN_MINER_HEALTH_SCORE")
        elif len(recent_miners) < min_required_miners:
            print(f"   1. ⚠️ WARNING: Only {len(recent_miners)} available miners, need {min_required_miners}")
            print("      - Consider lowering MIN_REQUIRED_MINERS")
            print("      - Add more miners to the network")
        else:
            print("   1. ✅ Sufficient miners available")
            print("   2. Check storage capacity and file sizes")
            print("   3. Review batch processing limits")
        
    except Exception as e:
        print(f"❌ Error during diagnostic: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(check_assignment_bottlenecks()) 