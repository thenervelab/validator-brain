"""
Script to query and display miner health check results from the database.
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Optional

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from app.db.connection import get_db_pool, init_db_pool, close_db_pool

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def query_health_summary(epoch: Optional[int] = None):
    """Query health summary for all miners."""
    try:
        await init_db_pool()
        db_pool = get_db_pool()
        
        async with db_pool.acquire() as conn:
            if epoch is not None:
                # Query specific epoch
                rows = await conn.fetch("""
                    SELECT 
                        node_id,
                        ipfs_peer_id,
                        epoch,
                        ping_successes,
                        ping_failures,
                        pin_check_successes,
                        pin_check_failures,
                        last_ping_attempt,
                        last_pin_check_attempt,
                        last_activity_at,
                        CASE 
                            WHEN (ping_successes + ping_failures) = 0 THEN 0
                            ELSE ROUND((ping_successes * 100.0 / (ping_successes + ping_failures))::numeric, 2)
                        END as ping_success_rate,
                        CASE 
                            WHEN (pin_check_successes + pin_check_failures) = 0 THEN 0
                            ELSE ROUND((pin_check_successes * 100.0 / (pin_check_successes + pin_check_failures))::numeric, 2)
                        END as pin_success_rate
                    FROM miner_epoch_health
                    WHERE epoch = $1
                    ORDER BY node_id
                """, epoch)
                print(f"\nHealth Summary for Epoch {epoch}")
            else:
                # Query latest epoch for each miner
                rows = await conn.fetch("""
                    SELECT 
                        node_id,
                        ipfs_peer_id,
                        epoch,
                        ping_successes,
                        ping_failures,
                        pin_check_successes,
                        pin_check_failures,
                        last_ping_attempt,
                        last_pin_check_attempt,
                        last_activity_at,
                        CASE 
                            WHEN (ping_successes + ping_failures) = 0 THEN 0
                            ELSE ROUND((ping_successes * 100.0 / (ping_successes + ping_failures))::numeric, 2)
                        END as ping_success_rate,
                        CASE 
                            WHEN (pin_check_successes + pin_check_failures) = 0 THEN 0
                            ELSE ROUND((pin_check_successes * 100.0 / (pin_check_successes + pin_check_failures))::numeric, 2)
                        END as pin_success_rate
                    FROM miner_epoch_health meh1
                    WHERE epoch = (
                        SELECT MAX(epoch) 
                        FROM miner_epoch_health meh2 
                        WHERE meh2.node_id = meh1.node_id
                    )
                    ORDER BY node_id
                """)
                print(f"\nLatest Health Summary for All Miners")
            
            print("=" * 120)
            print(f"{'Node ID':<20} {'IPFS Peer ID':<20} {'Epoch':<6} {'Ping %':<8} {'Pin %':<8} {'Last Ping':<20} {'Last Pin Check':<20}")
            print("=" * 120)
            
            for row in rows:
                last_ping = row['last_ping_attempt'].strftime('%Y-%m-%d %H:%M:%S') if row['last_ping_attempt'] else 'Never'
                last_pin = row['last_pin_check_attempt'].strftime('%Y-%m-%d %H:%M:%S') if row['last_pin_check_attempt'] else 'Never'
                
                print(f"{row['node_id']:<20} {row['ipfs_peer_id']:<20} {row['epoch']:<6} "
                      f"{row['ping_success_rate']:<8}% {row['pin_success_rate']:<8}% "
                      f"{last_ping:<20} {last_pin:<20}")
            
            print(f"\nTotal miners: {len(rows)}")
        
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"Error querying health summary: {e}")
        raise


async def query_miner_details(node_id: str, limit: int = 10):
    """Query detailed health history for a specific miner."""
    try:
        await init_db_pool()
        db_pool = get_db_pool()
        
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    node_id,
                    ipfs_peer_id,
                    epoch,
                    ping_successes,
                    ping_failures,
                    pin_check_successes,
                    pin_check_failures,
                    last_ping_attempt,
                    last_ping_block,
                    last_pin_check_attempt,
                    last_activity_at,
                    CASE 
                        WHEN (ping_successes + ping_failures) = 0 THEN 0
                        ELSE ROUND((ping_successes * 100.0 / (ping_successes + ping_failures))::numeric, 2)
                    END as ping_success_rate,
                    CASE 
                        WHEN (pin_check_successes + pin_check_failures) = 0 THEN 0
                        ELSE ROUND((pin_check_successes * 100.0 / (pin_check_successes + pin_check_failures))::numeric, 2)
                    END as pin_success_rate
                FROM miner_epoch_health
                WHERE node_id = $1
                ORDER BY epoch DESC
                LIMIT $2
            """, node_id, limit)
            
            if not rows:
                print(f"No health data found for miner: {node_id}")
                return
            
            print(f"\nHealth History for Miner: {node_id}")
            print("=" * 100)
            print(f"{'Epoch':<6} {'Ping Success':<12} {'Ping Fail':<10} {'Pin Success':<12} {'Pin Fail':<10} {'Last Activity':<20}")
            print("=" * 100)
            
            for row in rows:
                last_activity = row['last_activity_at'].strftime('%Y-%m-%d %H:%M:%S') if row['last_activity_at'] else 'Never'
                
                print(f"{row['epoch']:<6} {row['ping_successes']:<12} {row['ping_failures']:<10} "
                      f"{row['pin_check_successes']:<12} {row['pin_check_failures']:<10} {last_activity:<20}")
            
            # Show overall statistics
            total_ping_success = sum(row['ping_successes'] for row in rows)
            total_ping_fail = sum(row['ping_failures'] for row in rows)
            total_pin_success = sum(row['pin_check_successes'] for row in rows)
            total_pin_fail = sum(row['pin_check_failures'] for row in rows)
            
            overall_ping_rate = 0
            if total_ping_success + total_ping_fail > 0:
                overall_ping_rate = round((total_ping_success * 100.0) / (total_ping_success + total_ping_fail), 2)
            
            overall_pin_rate = 0
            if total_pin_success + total_pin_fail > 0:
                overall_pin_rate = round((total_pin_success * 100.0) / (total_pin_success + total_pin_fail), 2)
            
            print("\nOverall Statistics:")
            print(f"  Ping Success Rate: {overall_ping_rate}% ({total_ping_success}/{total_ping_success + total_ping_fail})")
            print(f"  Pin Success Rate: {overall_pin_rate}% ({total_pin_success}/{total_pin_success + total_pin_fail})")
        
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"Error querying miner details: {e}")
        raise


async def query_epoch_stats(epoch: int):
    """Query statistics for a specific epoch."""
    try:
        await init_db_pool()
        db_pool = get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Get overall stats for the epoch
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_miners,
                    SUM(ping_successes) as total_ping_success,
                    SUM(ping_failures) as total_ping_fail,
                    SUM(pin_check_successes) as total_pin_success,
                    SUM(pin_check_failures) as total_pin_fail,
                    COUNT(CASE WHEN last_ping_attempt IS NOT NULL THEN 1 END) as miners_pinged,
                    COUNT(CASE WHEN last_pin_check_attempt IS NOT NULL THEN 1 END) as miners_pin_checked
                FROM miner_epoch_health
                WHERE epoch = $1
            """, epoch)
            
            if stats['total_miners'] == 0:
                print(f"No health data found for epoch {epoch}")
                return
            
            print(f"\nEpoch {epoch} Statistics")
            print("=" * 50)
            print(f"Total miners: {stats['total_miners']}")
            print(f"Miners pinged: {stats['miners_pinged']}")
            print(f"Miners pin-checked: {stats['miners_pin_checked']}")
            
            if stats['total_ping_success'] + stats['total_ping_fail'] > 0:
                ping_rate = round((stats['total_ping_success'] * 100.0) / (stats['total_ping_success'] + stats['total_ping_fail']), 2)
                print(f"Overall ping success rate: {ping_rate}% ({stats['total_ping_success']}/{stats['total_ping_success'] + stats['total_ping_fail']})")
            
            if stats['total_pin_success'] + stats['total_pin_fail'] > 0:
                pin_rate = round((stats['total_pin_success'] * 100.0) / (stats['total_pin_success'] + stats['total_pin_fail']), 2)
                print(f"Overall pin success rate: {pin_rate}% ({stats['total_pin_success']}/{stats['total_pin_success'] + stats['total_pin_fail']})")
        
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"Error querying epoch stats: {e}")
        raise


def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(description='Query miner health check results')
    parser.add_argument('--epoch', type=int, help='Query specific epoch')
    parser.add_argument('--miner', type=str, help='Query specific miner node ID')
    parser.add_argument('--limit', type=int, default=10, help='Limit results for miner query')
    parser.add_argument('--stats', action='store_true', help='Show epoch statistics')
    
    args = parser.parse_args()
    
    if args.miner:
        asyncio.run(query_miner_details(args.miner, args.limit))
    elif args.stats and args.epoch:
        asyncio.run(query_epoch_stats(args.epoch))
    else:
        asyncio.run(query_health_summary(args.epoch))


if __name__ == "__main__":
    main() 