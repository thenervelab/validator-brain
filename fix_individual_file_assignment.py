#!/usr/bin/env python3
"""
Fix Individual File Assignment Script

This script assigns miners to a specific file that has empty miner assignments.
It processes files individually to avoid batch processing issues.
"""

import asyncio
import asyncpg
import os
import sys
import random
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()


class IndividualFileAssigner:
    def __init__(self):
        self.replicas_per_file = 5
        self.min_miner_health_score = 70.0
    
    async def get_available_miners(self, conn, file_size: int):
        """Get available miners that can handle the file size."""
        
        # Add 20% safety margin for IPFS overhead
        safety_margin = int(file_size * 0.2)
        required_space = file_size + safety_margin
        
        miners = await conn.fetch("""
            SELECT 
                r.node_id,
                r.ipfs_peer_id,
                COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                COALESCE(nm.ipfs_repo_size, 0) as used_storage_bytes,
                COALESCE(ms.total_files_pinned, 0) as total_files_pinned,
                COALESCE(ms.total_files_size_bytes, 0) as total_files_size_bytes,
                COALESCE(ms.health_score, 100) as health_score
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
              AND COALESCE(ms.health_score, 100) >= $1
            ORDER BY COALESCE(ms.health_score, 100) DESC, r.node_id
        """, self.min_miner_health_score)
        
        # Filter miners with enough capacity
        suitable_miners = []
        for miner in miners:
            # Use actual IPFS repo size as primary indicator
            ipfs_repo_size = miner['used_storage_bytes']
            calculated_size = miner['total_files_size_bytes']
            
            # Use the higher value as safety measure
            if ipfs_repo_size > 0:
                used_storage = ipfs_repo_size
                if calculated_size > ipfs_repo_size:
                    used_storage = calculated_size
            else:
                used_storage = calculated_size
            
            storage_capacity = miner['storage_capacity_bytes']
            available_storage = max(0, storage_capacity - used_storage)
            
            if available_storage >= required_space:
                suitable_miners.append({
                    'node_id': miner['node_id'],
                    'health_score': miner['health_score'],
                    'available_storage': available_storage,
                    'files_pinned': miner['total_files_pinned']
                })
                print(f"  ✅ {miner['node_id']}: {available_storage:,} available >= {required_space:,} required")
            else:
                print(f"  ❌ {miner['node_id']}: {available_storage:,} available < {required_space:,} required")
        
        return suitable_miners
    
    def select_best_miners(self, miners, count: int):
        """Select the best miners using a combination of health and load balancing."""
        if len(miners) <= count:
            return [m['node_id'] for m in miners]
        
        # Score miners based on health and current load
        scored_miners = []
        for miner in miners:
            # Health score (0-1)
            health_score = min(1.0, miner['health_score'] / 100.0)
            
            # Load score (0-1) - prefer miners with fewer files
            file_count_normalized = min(1.0, miner['files_pinned'] / 1000.0)
            load_score = 1.0 - file_count_normalized
            
            # Combined score
            final_score = health_score * 0.7 + load_score * 0.3
            
            scored_miners.append({
                'node_id': miner['node_id'],
                'score': final_score
            })
        
        # Sort by score and add some randomization
        scored_miners.sort(key=lambda x: x['score'], reverse=True)
        
        # Take top candidates and randomly select from them
        top_candidates = scored_miners[:min(count * 2, len(scored_miners))]
        selected = random.sample(top_candidates, min(count, len(top_candidates)))
        
        return [m['node_id'] for m in selected]
    
    async def assign_miners_to_file(self, conn, cid: str, file_size: int, owner: str):
        """Assign miners to a specific file."""
        
        print(f"\n🔧 Assigning miners to file: {cid}")
        print(f"   Size: {file_size:,} bytes")
        print(f"   Owner: {owner}")
        
        # Get available miners
        print("\n🔍 Finding available miners...")
        available_miners = await self.get_available_miners(conn, file_size)
        
        if not available_miners:
            print("❌ No suitable miners found!")
            return False
        
        print(f"✅ Found {len(available_miners)} suitable miners")
        
        # Select best miners
        selected_miners = self.select_best_miners(available_miners, self.replicas_per_file)
        
        if len(selected_miners) < self.replicas_per_file:
            print(f"⚠️  Only found {len(selected_miners)} miners, need {self.replicas_per_file}")
        
        print(f"\n📋 Selected miners:")
        for i, miner_id in enumerate(selected_miners):
            print(f"  {i+1}. {miner_id}")
        
        # Pad to 5 miners
        miners_padded = (selected_miners + [None] * 5)[:5]
        
        # Update file assignments
        async with conn.transaction():
            result = await conn.execute("""
                UPDATE file_assignments
                SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6,
                    updated_at = CURRENT_TIMESTAMP
                WHERE cid = $1
            """, cid, miners_padded[0], miners_padded[1], miners_padded[2], 
                miners_padded[3], miners_padded[4])
            
            if result == "UPDATE 0":
                print("❌ Failed to update file assignment - file not found")
                return False
            
            # Update miner stats
            for miner_id in selected_miners:
                if miner_id:
                    await conn.execute("""
                        INSERT INTO miner_stats (
                            node_id, total_files_pinned, total_files_size_bytes, updated_at
                        )
                        VALUES ($1, 1, $2, NOW())
                        ON CONFLICT (node_id) DO UPDATE SET
                            total_files_pinned = miner_stats.total_files_pinned + 1,
                            total_files_size_bytes = miner_stats.total_files_size_bytes + $2,
                            updated_at = NOW()
                    """, miner_id, file_size)
            
            # Mark as assigned in pending_assignment_file if it exists
            await conn.execute("""
                UPDATE pending_assignment_file
                SET status = 'assigned', processed_at = CURRENT_TIMESTAMP
                WHERE cid = $1
            """, cid)
        
        print(f"✅ Successfully assigned {len(selected_miners)} miners to file!")
        print("📝 File assignment updated, user profile will be reconstructed in next cycle")
        return True


async def fix_specific_file(cid: str):
    """Fix assignment for a specific file."""
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    assigner = IndividualFileAssigner()
    
    try:
        # Check if file exists and get info
        file_info = await conn.fetchrow("""
            SELECT 
                f.cid, f.name, f.size,
                fa.owner, fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5,
                fa.updated_at
            FROM files f
            JOIN file_assignments fa ON f.cid = fa.cid
            WHERE f.cid = $1
        """, cid)
        
        if not file_info:
            print(f"❌ File {cid} not found in file_assignments table")
            return False
        
        # Check if already has miners assigned
        assigned_miners = [
            m for m in [file_info['miner1'], file_info['miner2'], 
                       file_info['miner3'], file_info['miner4'], file_info['miner5']]
            if m is not None
        ]
        
        if len(assigned_miners) >= 5:
            print(f"✅ File already has {len(assigned_miners)} miners assigned")
            for i, miner in enumerate(assigned_miners):
                print(f"  {i+1}. {miner}")
            return True
        
        if assigned_miners:
            print(f"⚠️  File has {len(assigned_miners)} miners, need {5 - len(assigned_miners)} more")
            for i, miner in enumerate(assigned_miners):
                print(f"  Existing {i+1}. {miner}")
        else:
            print("📋 File has no miners assigned")
        
        # Assign miners
        success = await assigner.assign_miners_to_file(
            conn, cid, file_info['size'], file_info['owner']
        )
        
        return success
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        await conn.close()


async def fix_all_empty_assignments():
    """Fix all files with empty assignments."""
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    assigner = IndividualFileAssigner()
    
    try:
        # Get all files with empty assignments
        empty_files = await conn.fetch("""
            SELECT 
                f.cid, f.name, f.size, fa.owner
            FROM files f
            JOIN file_assignments fa ON f.cid = fa.cid
            WHERE fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
              AND fa.miner4 IS NULL AND fa.miner5 IS NULL
            ORDER BY f.size ASC  -- Process smaller files first
        """)
        
        if not empty_files:
            print("✅ No files with empty assignments found!")
            return True
        
        print(f"📋 Found {len(empty_files)} files with empty assignments")
        
        success_count = 0
        for i, file_info in enumerate(empty_files):
            print(f"\n{'='*60}")
            print(f"Processing {i+1}/{len(empty_files)}: {file_info['name']}")
            
            success = await assigner.assign_miners_to_file(
                conn, file_info['cid'], file_info['size'], file_info['owner']
            )
            
            if success:
                success_count += 1
            
            # Small delay to avoid overwhelming the system
            await asyncio.sleep(0.1)
        
        print(f"\n{'='*60}")
        print(f"✅ Completed: {success_count}/{len(empty_files)} files successfully assigned")
        
        return success_count == len(empty_files)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        await conn.close()


async def show_assignment_summary():
    """Show current assignment status."""
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    
    try:
        stats = await conn.fetch("""
            SELECT 
                CASE 
                    WHEN fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
                         AND fa.miner4 IS NULL AND fa.miner5 IS NULL THEN 'ALL_EMPTY'
                    WHEN (CASE WHEN fa.miner1 IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN fa.miner2 IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN fa.miner3 IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN fa.miner4 IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN fa.miner5 IS NOT NULL THEN 1 ELSE 0 END) < 5 THEN 'PARTIAL'
                    ELSE 'FULL'
                END as assignment_status,
                COUNT(*) as count
            FROM file_assignments fa
            GROUP BY 
                CASE 
                    WHEN fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
                         AND fa.miner4 IS NULL AND fa.miner5 IS NULL THEN 'ALL_EMPTY'
                    WHEN (CASE WHEN fa.miner1 IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN fa.miner2 IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN fa.miner3 IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN fa.miner4 IS NOT NULL THEN 1 ELSE 0 END +
                          CASE WHEN fa.miner5 IS NOT NULL THEN 1 ELSE 0 END) < 5 THEN 'PARTIAL'
                    ELSE 'FULL'
                END
            ORDER BY assignment_status
        """)
        
        print("📊 Assignment Status Summary:")
        for stat in stats:
            print(f"   {stat['assignment_status']}: {stat['count']} files")
    
    finally:
        await conn.close()


async def main():
    print("🔧 Individual File Assignment Fixer")
    print("=" * 50)
    
    await show_assignment_summary()
    print()
    
    # Check if specific file was provided as argument
    if len(sys.argv) > 1:
        cid = sys.argv[1]
        print(f"🎯 Fixing specific file: {cid}")
        success = await fix_specific_file(cid)
        if success:
            print("\n✅ File assignment completed!")
        else:
            print("\n❌ File assignment failed!")
    else:
        # Interactive mode
        print("Options:")
        print("1. Fix all files with empty assignments")
        print("2. Fix a specific file")
        print("3. Just show status")
        
        choice = input("\nChoose option (1-3): ").strip()
        
        if choice == "1":
            print("\n🔄 Fixing all files with empty assignments...")
            success = await fix_all_empty_assignments()
        elif choice == "2":
            cid = input("Enter file CID: ").strip()
            success = await fix_specific_file(cid)
        else:
            success = True
    
    print("\n" + "=" * 50)
    await show_assignment_summary()


if __name__ == "__main__":
    asyncio.run(main()) 