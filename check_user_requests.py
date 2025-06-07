#!/usr/bin/env python3
import asyncio
import asyncpg

async def check_requests():
    conn = await asyncpg.connect('postgresql://user:password@localhost:5433/substrate_fetcher')
    
    # Check for the user's hex-encoded file hashes
    hex_hashes = [
        '6261666b72656962766e6d3469727963786d6837616f637968693579633374676533346c7234713364337374366a637a35716973346167366c7a79',
        '6261666b726569613633617673776d777878786779706f6876636777757476776667326a7134736d6c356468723474796e6b7274617465686b6465'
    ]
    
    print('🔍 Checking for storage requests in validator-1 database...')
    for i, hex_hash in enumerate(hex_hashes, 1):
        print(f'\n📋 Request {i}: {hex_hash[:30]}...')
        
        # Check pinning_requests table
        pinning_count = await conn.fetchval('SELECT COUNT(*) FROM pinning_requests WHERE file_hash = $1', hex_hash)
        print(f'   Pinning requests: {pinning_count}')
        
        # Check processed_pinning_requests
        processed_count = await conn.fetchval('SELECT COUNT(*) FROM processed_pinning_requests WHERE request_hash LIKE $1', f'%{hex_hash[:10]}%')
        print(f'   Processed requests: {processed_count}')
        
        # Check file_assignments
        assignments = await conn.fetchval('SELECT COUNT(*) FROM file_assignments WHERE owner = $1', '5HoreGVb17XhY3wanDvzoAWS7yHYbc5uMteXqRNTiZ6Txkqq')
        print(f'   File assignments for user: {assignments}')
    
    # Check if any requests from this user exist at all
    total_user_files = await conn.fetchval('SELECT COUNT(*) FROM file_assignments WHERE owner = $1', '5HoreGVb17XhY3wanDvzoAWS7yHYbc5uMteXqRNTiZ6Txkqq')
    print(f'\n📊 Total file assignments for user: {total_user_files}')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_requests()) 