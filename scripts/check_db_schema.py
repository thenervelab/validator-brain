#!/usr/bin/env python3
"""Check database schema and current data state."""

import asyncio
import asyncpg
import os

async def check_schema():
    """Check database schema and data."""
    database_url = os.getenv('DATABASE_URL', 'postgres://user:password@localhost:5432/substrate_fetcher?sslmode=disable')
    
    conn = await asyncpg.connect(database_url)
    
    print("🔍 DATABASE SCHEMA CHECK")
    print("=" * 50)
    
    # Check miner_epoch_health table structure
    columns = await conn.fetch("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'miner_epoch_health'
        ORDER BY ordinal_position
    """)
    
    print('\n📋 miner_epoch_health columns:')
    for col in columns:
        print(f'  {col["column_name"]}: {col["data_type"]}')
    
    # Check current data
    count = await conn.fetchval('SELECT COUNT(*) FROM miner_epoch_health')
    print(f'\n📊 Total health records: {count}')
    
    if count > 0:
        sample = await conn.fetch('SELECT * FROM miner_epoch_health LIMIT 3')
        print('\n📝 Sample records:')
        for row in sample:
            print(f'  {dict(row)}')
    
    # Check file_assignments table
    print('\n📋 file_assignments status:')
    assignment_stats = await conn.fetchrow("""
        SELECT 
            COUNT(*) as total_files,
            COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                       OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
            COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                       AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments
        FROM file_assignments
    """)
    
    total_files = assignment_stats['total_files'] or 0
    files_with_miners = assignment_stats['files_with_miners'] or 0
    empty_assignments = assignment_stats['empty_assignments'] or 0
    
    print(f'  Total files: {total_files}')
    print(f'  Files with miners: {files_with_miners}')
    print(f'  Empty assignments: {empty_assignments}')
    
    if total_files > 0:
        coverage = (files_with_miners / total_files) * 100
        print(f'  Coverage: {coverage:.1f}%')
        
        if coverage == 0:
            print('  🚨 NO FILES HAVE MINERS ASSIGNED!')
        elif coverage < 50:
            print('  ⚠️ VERY LOW assignment coverage')
        else:
            print('  ✅ Assignment coverage looks good')
    
    # Check miners available
    miner_count = await conn.fetchval("""
        SELECT COUNT(*) FROM registration 
        WHERE node_type = 'StorageMiner' AND status = 'active'
    """)
    
    print(f'\n⛏️ Active storage miners: {miner_count}')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_schema()) 