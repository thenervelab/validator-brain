#!/usr/bin/env python3
"""
Query File Assignment System

This script provides monitoring and reporting capabilities for the file assignment system.
It can show pending files, assignment statistics, miner distribution, and more.
"""

import asyncio
import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://user:password@localhost:5432/substrate_fetcher')


async def get_pending_files_summary():
    """Get summary of pending files awaiting assignment."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get pending files by status
        status_summary = await conn.fetch("""
            SELECT 
                status,
                COUNT(*) as file_count,
                SUM(file_size_bytes) as total_size_bytes,
                AVG(file_size_bytes) as avg_size_bytes,
                MIN(created_at) as oldest_file,
                MAX(created_at) as newest_file
            FROM pending_assignment_file
            GROUP BY status
            ORDER BY status
        """)
        
        print("📋 Pending Files Summary")
        print("-" * 80)
        
        total_files = 0
        total_size = 0
        
        for row in status_summary:
            status = row['status']
            count = row['file_count']
            size_gb = (row['total_size_bytes'] or 0) / (1024**3)
            avg_size_mb = (row['avg_size_bytes'] or 0) / (1024**2)
            
            total_files += count
            total_size += (row['total_size_bytes'] or 0)
            
            print(f"Status: {status}")
            print(f"  Files: {count:,}")
            print(f"  Total Size: {size_gb:.2f} GB")
            print(f"  Average Size: {avg_size_mb:.2f} MB")
            print(f"  Date Range: {row['oldest_file']} to {row['newest_file']}")
            print()
        
        print(f"Total Files: {total_files:,}")
        print(f"Total Size: {total_size / (1024**3):.2f} GB")
        
        # Get files ready for assignment
        ready_files = await conn.fetchval("""
            SELECT COUNT(*) FROM pending_assignment_file paf
            WHERE paf.status = 'processed' 
              AND paf.file_size_bytes IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_assignments fa 
                  WHERE fa.cid = paf.cid
              )
        """)
        
        print(f"Files Ready for Assignment: {ready_files:,}")
        
    finally:
        await conn.close()


async def get_reassignment_summary():
    """Get summary of files needing reassignment."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("\n🔄 Reassignment Summary")
        print("-" * 80)
        
        # Get files with empty slots
        reassignment_stats = await conn.fetch("""
            SELECT 
                (CASE WHEN miner1 IS NULL THEN 1 ELSE 0 END +
                 CASE WHEN miner2 IS NULL THEN 1 ELSE 0 END +
                 CASE WHEN miner3 IS NULL THEN 1 ELSE 0 END +
                 CASE WHEN miner4 IS NULL THEN 1 ELSE 0 END +
                 CASE WHEN miner5 IS NULL THEN 1 ELSE 0 END) as empty_slots,
                COUNT(*) as file_count
            FROM file_assignments
            WHERE (miner1 IS NULL OR miner2 IS NULL OR miner3 IS NULL OR 
                   miner4 IS NULL OR miner5 IS NULL)
            GROUP BY empty_slots
            ORDER BY empty_slots
        """)
        
        total_files_needing_reassignment = 0
        total_empty_slots = 0
        
        if reassignment_stats:
            print("Files Needing Reassignment by Empty Slots:")
            for row in reassignment_stats:
                empty_slots = row['empty_slots']
                file_count = row['file_count']
                total_files_needing_reassignment += file_count
                total_empty_slots += empty_slots * file_count
                print(f"  {empty_slots} empty slots: {file_count:,} files")
            
            print(f"\nTotal Files Needing Reassignment: {total_files_needing_reassignment:,}")
            print(f"Total Empty Slots to Fill: {total_empty_slots:,}")
        else:
            print("No files need reassignment - all slots are filled!")
        
        # Get recent reassignment activity
        recent_reassignments = await conn.fetchval("""
            SELECT COUNT(*) FROM file_assignments
            WHERE updated_at >= NOW() - INTERVAL '24 hours'
            AND (miner1 IS NOT NULL AND miner2 IS NOT NULL AND miner3 IS NOT NULL AND 
                 miner4 IS NOT NULL AND miner5 IS NOT NULL)
        """)
        
        print(f"Files with Full Assignment in Last 24 Hours: {recent_reassignments:,}")
        
    finally:
        await conn.close()


async def get_assignment_statistics():
    """Get statistics about file assignments."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("\n📊 Assignment Statistics")
        print("-" * 80)
        
        # Total assignments
        total_assignments = await conn.fetchval("SELECT COUNT(*) FROM file_assignments")
        print(f"Total File Assignments: {total_assignments:,}")
        
        # Files by replica count
        replica_stats = await conn.fetch("""
            SELECT 
                (CASE WHEN miner1 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN miner2 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN miner3 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN miner4 IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN miner5 IS NOT NULL THEN 1 ELSE 0 END) as replica_count,
                COUNT(*) as file_count
            FROM file_assignments
            GROUP BY replica_count
            ORDER BY replica_count
        """)
        
        print("\nFiles by Replica Count:")
        for row in replica_stats:
            print(f"  {row['replica_count']} replicas: {row['file_count']:,} files")
        
        # Recent assignments
        recent_assignments = await conn.fetchval("""
            SELECT COUNT(*) FROM file_assignments
            WHERE updated_at >= NOW() - INTERVAL '24 hours'
        """)
        print(f"\nAssignments in Last 24 Hours: {recent_assignments:,}")
        
        # Total file size assigned
        total_size = await conn.fetchval("""
            SELECT SUM(f.size) FROM files f
            JOIN file_assignments fa ON f.cid = fa.cid
        """)
        
        if total_size:
            print(f"Total Assigned File Size: {total_size / (1024**3):.2f} GB")
        
        # Assignment completeness
        complete_assignments = await conn.fetchval("""
            SELECT COUNT(*) FROM file_assignments
            WHERE miner1 IS NOT NULL AND miner2 IS NOT NULL AND miner3 IS NOT NULL AND 
                  miner4 IS NOT NULL AND miner5 IS NOT NULL
        """)
        
        if total_assignments > 0:
            completeness_percent = (complete_assignments / total_assignments) * 100
            print(f"Assignment Completeness: {completeness_percent:.1f}% ({complete_assignments:,}/{total_assignments:,} fully assigned)")
        
    finally:
        await conn.close()


async def get_miner_distribution():
    """Get distribution of files across miners."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("\n⚖️ Miner Distribution")
        print("-" * 80)
        
        # Files per miner
        miner_stats = await conn.fetch("""
            WITH miner_assignments AS (
                SELECT miner1 as miner_id FROM file_assignments WHERE miner1 IS NOT NULL
                UNION ALL
                SELECT miner2 as miner_id FROM file_assignments WHERE miner2 IS NOT NULL
                UNION ALL
                SELECT miner3 as miner_id FROM file_assignments WHERE miner3 IS NOT NULL
                UNION ALL
                SELECT miner4 as miner_id FROM file_assignments WHERE miner4 IS NOT NULL
                UNION ALL
                SELECT miner5 as miner_id FROM file_assignments WHERE miner5 IS NOT NULL
            )
            SELECT 
                ma.miner_id,
                COUNT(*) as file_count,
                r.registered_at,
                EXTRACT(EPOCH FROM (NOW() - TO_TIMESTAMP(r.registered_at))) / 86400 as days_since_registration
            FROM miner_assignments ma
            LEFT JOIN registration r ON ma.miner_id = r.node_id
            GROUP BY ma.miner_id, r.registered_at
            ORDER BY file_count DESC
            LIMIT 20
        """)
        
        print("Top 20 Miners by File Count:")
        print(f"{'Miner ID':<20} {'Files':<8} {'Days Since Reg':<15} {'Reg Block':<12}")
        print("-" * 65)
        
        for row in miner_stats:
            miner_id = row['miner_id'][:18] + "..." if len(row['miner_id']) > 18 else row['miner_id']
            file_count = row['file_count']
            days_since_reg = row['days_since_registration']
            reg_block = row['registered_at']
            
            days_str = f"{days_since_reg:.1f}" if days_since_reg else "Unknown"
            reg_str = str(reg_block) if reg_block else "Unknown"
            
            print(f"{miner_id:<20} {file_count:<8} {days_str:<15} {reg_str:<12}")
        
        # Distribution statistics
        distribution_stats = await conn.fetchrow("""
            WITH miner_file_counts AS (
                WITH miner_assignments AS (
                    SELECT miner1 as miner_id FROM file_assignments WHERE miner1 IS NOT NULL
                    UNION ALL
                    SELECT miner2 as miner_id FROM file_assignments WHERE miner2 IS NOT NULL
                    UNION ALL
                    SELECT miner3 as miner_id FROM file_assignments WHERE miner3 IS NOT NULL
                    UNION ALL
                    SELECT miner4 as miner_id FROM file_assignments WHERE miner4 IS NOT NULL
                    UNION ALL
                    SELECT miner5 as miner_id FROM file_assignments WHERE miner5 IS NOT NULL
                )
                SELECT miner_id, COUNT(*) as file_count
                FROM miner_assignments
                GROUP BY miner_id
            )
            SELECT 
                COUNT(*) as total_miners,
                AVG(file_count) as avg_files_per_miner,
                MIN(file_count) as min_files,
                MAX(file_count) as max_files,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY file_count) as median_files
            FROM miner_file_counts
        """)
        
        print(f"\nDistribution Statistics:")
        print(f"  Total Miners with Files: {distribution_stats['total_miners']:,}")
        print(f"  Average Files per Miner: {distribution_stats['avg_files_per_miner']:.1f}")
        print(f"  Min Files: {distribution_stats['min_files']}")
        print(f"  Max Files: {distribution_stats['max_files']}")
        print(f"  Median Files: {distribution_stats['median_files']:.1f}")
        
    finally:
        await conn.close()


async def get_new_miner_analysis():
    """Analyze file distribution for new miners."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("\n🆕 New Miner Analysis")
        print("-" * 80)
        
        # Get miners registered in last 30 days
        new_miners = await conn.fetch("""
            WITH miner_assignments AS (
                SELECT miner1 as miner_id FROM file_assignments WHERE miner1 IS NOT NULL
                UNION ALL
                SELECT miner2 as miner_id FROM file_assignments WHERE miner2 IS NOT NULL
                UNION ALL
                SELECT miner3 as miner_id FROM file_assignments WHERE miner3 IS NOT NULL
                UNION ALL
                SELECT miner4 as miner_id FROM file_assignments WHERE miner4 IS NOT NULL
                UNION ALL
                SELECT miner5 as miner_id FROM file_assignments WHERE miner5 IS NOT NULL
            )
            SELECT 
                r.node_id,
                r.registered_at,
                EXTRACT(EPOCH FROM (NOW() - TO_TIMESTAMP(r.registered_at))) / 86400 as days_since_registration,
                COALESCE(ma.file_count, 0) as file_count
            FROM registration r
            LEFT JOIN (
                SELECT miner_id, COUNT(*) as file_count
                FROM miner_assignments
                GROUP BY miner_id
            ) ma ON r.node_id = ma.miner_id
            WHERE r.node_type = 'StorageMiner' 
              AND r.status = 'active'
              AND EXTRACT(EPOCH FROM (NOW() - TO_TIMESTAMP(r.registered_at))) / 86400 <= 30
            ORDER BY r.registered_at DESC
        """)
        
        if new_miners:
            print(f"Miners Registered in Last 30 Days: {len(new_miners)}")
            print(f"{'Miner ID':<20} {'Days Ago':<10} {'Files':<8} {'Reg Block':<12}")
            print("-" * 55)
            
            total_files_new = 0
            for row in new_miners[:10]:  # Show top 10
                miner_id = row['node_id'][:18] + "..." if len(row['node_id']) > 18 else row['node_id']
                days_ago = row['days_since_registration']
                file_count = row['file_count']
                reg_block = row['registered_at']
                
                total_files_new += file_count
                
                print(f"{miner_id:<20} {days_ago:.1f}d{'':<6} {file_count:<8} {reg_block:<12}")
            
            avg_files_new = total_files_new / len(new_miners) if new_miners else 0
            print(f"\nAverage Files per New Miner: {avg_files_new:.1f}")
        else:
            print("No new miners found in the last 30 days")
        
    finally:
        await conn.close()


async def get_capacity_analysis():
    """Analyze miner capacity and usage."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("\n💾 Capacity Analysis")
        print("-" * 80)
        
        # Get miners with capacity information
        capacity_stats = await conn.fetch("""
            WITH miner_file_stats AS (
                WITH miner_assignments AS (
                    SELECT miner1 as miner_id FROM file_assignments WHERE miner1 IS NOT NULL
                    UNION ALL
                    SELECT miner2 as miner_id FROM file_assignments WHERE miner2 IS NOT NULL
                    UNION ALL
                    SELECT miner3 as miner_id FROM file_assignments WHERE miner3 IS NOT NULL
                    UNION ALL
                    SELECT miner4 as miner_id FROM file_assignments WHERE miner4 IS NOT NULL
                    UNION ALL
                    SELECT miner5 as miner_id FROM file_assignments WHERE miner5 IS NOT NULL
                )
                SELECT 
                    ma.miner_id,
                    COUNT(*) as assigned_files,
                    SUM(f.size) as assigned_size_bytes
                FROM miner_assignments ma
                JOIN file_assignments fa ON (
                    ma.miner_id = fa.miner1 OR ma.miner_id = fa.miner2 OR 
                    ma.miner_id = fa.miner3 OR ma.miner_id = fa.miner4 OR ma.miner_id = fa.miner5
                )
                JOIN files f ON fa.cid = f.cid
                GROUP BY ma.miner_id
            )
            SELECT 
                r.node_id,
                COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                COALESCE(nm.ipfs_repo_size, 0) as used_storage_bytes,
                COALESCE(mfs.assigned_files, 0) as assigned_files,
                COALESCE(mfs.assigned_size_bytes, 0) as assigned_size_bytes,
                COALESCE(ms.health_score, 100) as health_score
            FROM registration r
            LEFT JOIN (
                SELECT DISTINCT ON (miner_id) 
                    miner_id, ipfs_storage_max, ipfs_repo_size
                FROM node_metrics 
                ORDER BY miner_id, block_number DESC
            ) nm ON r.node_id = nm.miner_id
            LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
            LEFT JOIN miner_file_stats mfs ON r.node_id = mfs.miner_id
            WHERE r.node_type = 'StorageMiner' 
              AND r.status = 'active'
              AND COALESCE(ms.health_score, 100) >= 70
            ORDER BY assigned_files DESC
            LIMIT 15
        """)
        
        print("Top 15 Miners by Capacity Usage:")
        print(f"{'Miner ID':<20} {'Files':<6} {'Capacity':<10} {'Used':<10} {'Available':<10} {'Health':<7}")
        print("-" * 75)
        
        for row in capacity_stats:
            miner_id = row['node_id'][:18] + "..." if len(row['node_id']) > 18 else row['node_id']
            files = row['assigned_files']
            capacity_gb = row['storage_capacity_bytes'] / (1024**3)
            used_gb = max(row['used_storage_bytes'], row['assigned_size_bytes']) / (1024**3)
            available_gb = max(0, capacity_gb - used_gb)
            health = row['health_score']
            
            print(f"{miner_id:<20} {files:<6} {capacity_gb:.1f}GB{'':<4} {used_gb:.1f}GB{'':<4} {available_gb:.1f}GB{'':<4} {health:.0f}%")
        
        # Overall capacity statistics
        overall_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_miners,
                SUM(COALESCE(nm.ipfs_storage_max, 1000000000)) as total_capacity,
                SUM(COALESCE(nm.ipfs_repo_size, 0)) as total_used,
                AVG(COALESCE(ms.health_score, 100)) as avg_health
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
              AND COALESCE(ms.health_score, 100) >= 70
        """)
        
        if overall_stats:
            total_capacity_tb = overall_stats['total_capacity'] / (1024**4)
            total_used_tb = overall_stats['total_used'] / (1024**4)
            usage_percent = (overall_stats['total_used'] / overall_stats['total_capacity']) * 100
            
            print(f"\nOverall Network Statistics:")
            print(f"  Active Miners: {overall_stats['total_miners']:,}")
            print(f"  Total Capacity: {total_capacity_tb:.2f} TB")
            print(f"  Total Used: {total_used_tb:.2f} TB")
            print(f"  Network Usage: {usage_percent:.1f}%")
            print(f"  Average Health Score: {overall_stats['avg_health']:.1f}%")
        
    finally:
        await conn.close()


async def process_pending_assignments():
    """Trigger processing of pending assignments (for testing)."""
    print("\n🔄 Processing Pending Assignments")
    print("-" * 80)
    print("This would trigger the file assignment processor...")
    print("Run: kubectl apply -f k8s/file-assignment-processor-job.yaml")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Query file assignment system')
    parser.add_argument('--pending', action='store_true', help='Show pending files summary')
    parser.add_argument('--reassignments', action='store_true', help='Show files needing reassignment')
    parser.add_argument('--stats', action='store_true', help='Show assignment statistics')
    parser.add_argument('--distribution', action='store_true', help='Show miner distribution')
    parser.add_argument('--new-miners', action='store_true', help='Analyze new miner assignments')
    parser.add_argument('--capacity', action='store_true', help='Show capacity analysis')
    parser.add_argument('--process', action='store_true', help='Trigger assignment processing')
    parser.add_argument('--all', action='store_true', help='Show all reports')
    
    args = parser.parse_args()
    
    if not any([args.pending, args.reassignments, args.stats, args.distribution, args.new_miners, args.capacity, args.process, args.all]):
        args.all = True  # Default to showing all
    
    try:
        if args.all or args.pending:
            await get_pending_files_summary()
        
        if args.all or args.reassignments:
            await get_reassignment_summary()
        
        if args.all or args.stats:
            await get_assignment_statistics()
        
        if args.all or args.distribution:
            await get_miner_distribution()
        
        if args.all or args.new_miners:
            await get_new_miner_analysis()
        
        if args.all or args.capacity:
            await get_capacity_analysis()
        
        if args.process:
            await process_pending_assignments()
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 