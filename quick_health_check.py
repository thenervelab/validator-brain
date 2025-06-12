#!/usr/bin/env python3
import asyncio
import sys
import os
sys.path.append('.')
from app.db.connection import init_db_pool, get_db_pool

async def quick_check():
    await init_db_pool()
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        # Check how many miners have health scores
        total_miners = await conn.fetchval('SELECT COUNT(*) FROM registration WHERE node_type = \'StorageMiner\' AND status = \'active\'')
        miners_with_health = await conn.fetchval('SELECT COUNT(*) FROM miner_stats WHERE health_score IS NOT NULL')
        miners_health_20 = await conn.fetchval('SELECT COUNT(*) FROM miner_stats WHERE health_score >= 20.0')
        miners_health_70 = await conn.fetchval('SELECT COUNT(*) FROM miner_stats WHERE health_score >= 70.0')
        
        print(f'Total active miners: {total_miners}')
        print(f'Miners with health scores: {miners_with_health}')
        print(f'Miners with health >= 20: {miners_health_20}') 
        print(f'Miners with health >= 70: {miners_health_70}')
        
        # Check the join logic with COALESCE
        available_miners = await conn.fetchval('''
            SELECT COUNT(*) FROM registration r
            LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
            WHERE r.node_type = \'StorageMiner\' 
              AND r.status = \'active\'
              AND COALESCE(ms.health_score, 100) >= 20.0
        ''')
        print(f'Available miners (with COALESCE fallback to 100): {available_miners}')
        
        # Check how many have node_metrics data
        miners_with_metrics = await conn.fetchval('''
            SELECT COUNT(DISTINCT r.node_id) FROM registration r
            LEFT JOIN (
                SELECT DISTINCT ON (miner_id) miner_id
                FROM node_metrics 
                ORDER BY miner_id, block_number DESC
            ) nm ON r.node_id = nm.miner_id
            WHERE r.node_type = \'StorageMiner\' 
              AND r.status = \'active\'
              AND nm.miner_id IS NOT NULL
        ''')
        print(f'Miners with node_metrics data: {miners_with_metrics}')

asyncio.run(quick_check()) 