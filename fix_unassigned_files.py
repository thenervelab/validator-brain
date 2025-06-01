#!/usr/bin/env python3
"""
Fix Unassigned Files Script

This script finds files that exist in the 'files' table but don't have entries in the 
'file_assignments' table, and adds them to 'pending_assignment_file' for processing.
"""

import asyncio
import asyncpg
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()


async def fix_unassigned_files():
    """Find unassigned files and add them to pending assignment processing."""
    
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    
    try:
        print("🔍 Finding unassigned files...")
        
        # Find files that don't have assignments
        unassigned_files = await conn.fetch("""
            SELECT f.cid, f.name, f.size, f.created_date
            FROM files f
            WHERE NOT EXISTS (
                SELECT 1 FROM file_assignments fa WHERE fa.cid = f.cid
            )
            AND f.cid IS NOT NULL
            ORDER BY f.created_date DESC
        """)
        
        if not unassigned_files:
            print("✅ No unassigned files found!")
            return
        
        print(f"📋 Found {len(unassigned_files)} unassigned files")
        
        # Show the files
        print("\nUnassigned files:")
        for i, file_info in enumerate(unassigned_files[:10]):  # Show first 10
            print(f"  {i+1}. {file_info['name']} ({file_info['cid'][:16]}...) - {file_info['size']:,} bytes")
        
        if len(unassigned_files) > 10:
            print(f"  ... and {len(unassigned_files) - 10} more")
        
        # Check if we can find owners for these files from user_profile table
        print("\n🔍 Looking for file owners in user_profile table...")
        
        files_with_owners = []
        files_without_owners = []
        
        for file_info in unassigned_files:
            owner_info = await conn.fetchrow("""
                SELECT owner_account, user_id 
                FROM user_profile 
                WHERE file_hash = $1 
                LIMIT 1
            """, file_info['cid'])
            
            if owner_info:
                files_with_owners.append({
                    'cid': file_info['cid'],
                    'name': file_info['name'],
                    'size': file_info['size'],
                    'owner': owner_info['owner_account'],
                    'user_id': owner_info['user_id']
                })
            else:
                files_without_owners.append(file_info)
        
        print(f"✅ Found owners for {len(files_with_owners)} files")
        print(f"❌ No owners found for {len(files_without_owners)} files")
        
        if files_with_owners:
            print("\n📤 Adding files with known owners to pending assignment...")
            
            for file_info in files_with_owners:
                # Check if already in pending_assignment_file
                existing = await conn.fetchrow("""
                    SELECT id FROM pending_assignment_file 
                    WHERE cid = $1
                """, file_info['cid'])
                
                if existing:
                    print(f"  ⏭️  {file_info['name']} already in pending assignment")
                    continue
                
                # Add to pending_assignment_file
                await conn.execute("""
                    INSERT INTO pending_assignment_file 
                    (cid, filename, owner, file_size_bytes, status, created_at)
                    VALUES ($1, $2, $3, $4, 'processed', NOW())
                """, file_info['cid'], file_info['name'], file_info['owner'], file_info['size'])
                
                print(f"  ✅ Added {file_info['name']} to pending assignment")
        
        if files_without_owners:
            print(f"\n⚠️  Files without known owners (need manual investigation):")
            for file_info in files_without_owners[:5]:  # Show first 5
                print(f"  - {file_info['name']} ({file_info['cid'][:16]}...)")
        
        print(f"\n✅ Processing complete!")
        print(f"   - {len(files_with_owners)} files added to pending assignment")
        print(f"   - {len(files_without_owners)} files need manual owner assignment")
        
        if files_with_owners:
            print("\n💡 Next steps:")
            print("   1. Run the file assignment processor to assign miners")
            print("   2. Run user profile reconstruction to update profiles")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        await conn.close()


async def show_assignment_status():
    """Show current assignment status."""
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    
    try:
        stats = await conn.fetch("""
            SELECT 
                'Total files' as metric,
                COUNT(*) as count
            FROM files
            
            UNION ALL
            
            SELECT 
                'Files with assignments' as metric,
                COUNT(*) as count
            FROM file_assignments
            
            UNION ALL
            
            SELECT 
                'Unassigned files' as metric,
                COUNT(*) as count
            FROM files f
            WHERE NOT EXISTS (
                SELECT 1 FROM file_assignments fa WHERE fa.cid = f.cid
            )
            
            UNION ALL
            
            SELECT 
                'Pending assignment files' as metric,
                COUNT(*) as count
            FROM pending_assignment_file
            WHERE status = 'processed'
        """)
        
        print("📊 Current Assignment Status:")
        for stat in stats:
            print(f"   {stat['metric']}: {stat['count']}")
    
    finally:
        await conn.close()


async def main():
    print("🔧 Fix Unassigned Files Tool")
    print("=" * 50)
    
    await show_assignment_status()
    print()
    
    response = input("Do you want to fix unassigned files? (y/N): ").strip().lower()
    if response in ('y', 'yes'):
        await fix_unassigned_files()
        print()
        await show_assignment_status()
    else:
        print("👋 No changes made.")


if __name__ == "__main__":
    asyncio.run(main()) 