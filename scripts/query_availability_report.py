"""
Script to query and display comprehensive availability reports.

This script provides:
1. Overall availability statistics
2. File failure reports
3. Miner availability scores
4. Reassignment recommendations
"""

import asyncio
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Optional

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from app.db.connection import get_db_pool, init_db_pool, close_db_pool
from substrate_fetcher.file_availability_manager import FileAvailabilityManager, AvailabilityRules

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def display_availability_report():
    """Display comprehensive availability report."""
    try:
        await init_db_pool()
        db_pool = get_db_pool()
        
        # Initialize availability manager
        availability_manager = FileAvailabilityManager(db_pool)
        
        # Get comprehensive report
        report = await availability_manager.get_availability_report()
        
        print("\n" + "="*80)
        print("IPFS FILE AVAILABILITY REPORT")
        print("="*80)
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Overall Statistics
        print("\n📊 OVERALL STATISTICS")
        print("-" * 40)
        overall = report['overall_stats']
        if overall:
            print(f"Total Miners: {overall.get('total_miners', 0)}")
            print(f"Active Miners: {overall.get('active_miners', 0)}")
            print(f"Reliable Miners: {overall.get('reliable_miners', 0)}")
            print(f"Average Availability Score: {overall.get('avg_availability_score', 0):.4f}")
        else:
            print("No miner data available")
        
        # Failure Statistics
        print("\n🚨 FAILURE STATISTICS")
        print("-" * 40)
        failures = report['failure_stats']
        if failures:
            print(f"Files with Failures: {failures.get('files_with_failures', 0)}")
            print(f"Miners with Failures: {failures.get('miners_with_failures', 0)}")
            print(f"Total Active Failures: {failures.get('total_failures', 0)}")
        else:
            print("No recent failures")
        
        # Top Failing Miners
        print("\n⚠️  TOP FAILING MINERS")
        print("-" * 40)
        failing_miners = report['top_failing_miners']
        if failing_miners:
            print(f"{'Miner ID':<20} {'Failures':<10} {'Score':<8} {'Consecutive':<12} {'Active':<8}")
            print("-" * 70)
            for miner in failing_miners:
                print(f"{miner['miner_id']:<20} {miner['failure_count']:<10} "
                      f"{miner['availability_score']:<8.4f} {miner['consecutive_failures']:<12} "
                      f"{'Yes' if miner['is_active'] else 'No':<8}")
        else:
            print("No failing miners in recent period")
        
        # Configuration
        print("\n⚙️  AVAILABILITY RULES")
        print("-" * 40)
        rules = report['rules']
        print(f"Min Replicas: {rules['min_replicas']}")
        print(f"Max Replicas: {rules['max_replicas']}")
        print(f"Min Availability Score: {rules['min_availability_score']}")
        print(f"Max Consecutive Failures: {rules['max_consecutive_failures']}")
        print(f"Failure Window: {rules['failure_window_hours']} hours")
        
        print("\n" + "="*80)
        
    except Exception as e:
        logger.error(f"Error generating availability report: {e}")
        raise
    finally:
        await close_db_pool()


async def display_files_needing_reassignment():
    """Display files that need reassignment."""
    try:
        await init_db_pool()
        db_pool = get_db_pool()
        
        availability_manager = FileAvailabilityManager(db_pool)
        files_needing_reassignment = await availability_manager.get_files_needing_reassignment()
        
        print("\n" + "="*80)
        print("FILES NEEDING REASSIGNMENT")
        print("="*80)
        
        if files_needing_reassignment:
            print(f"Found {len(files_needing_reassignment)} files needing reassignment\n")
            
            for i, file_info in enumerate(files_needing_reassignment, 1):
                print(f"{i}. CID: {file_info['cid']}")
                print(f"   Owner: {file_info['owner']}")
                print(f"   Current Replicas: {file_info['current_replicas']}")
                print(f"   Failed Miners: {file_info['failed_miners_count']}")
                print(f"   Failed Miner IDs: {file_info['failed_miners']}")
                print()
        else:
            print("No files currently need reassignment")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Error getting files needing reassignment: {e}")
        raise
    finally:
        await close_db_pool()


async def process_reassignments():
    """Process all pending reassignments."""
    try:
        await init_db_pool()
        db_pool = get_db_pool()
        
        availability_manager = FileAvailabilityManager(db_pool)
        
        print("\n" + "="*80)
        print("PROCESSING REASSIGNMENTS")
        print("="*80)
        
        stats = await availability_manager.process_reassignments()
        
        print(f"Files Checked: {stats['files_checked']}")
        print(f"Files Reassigned: {stats['files_reassigned']}")
        print(f"Reassignment Failures: {stats['reassignment_failures']}")
        
        if stats['files_reassigned'] > 0:
            print(f"\n✅ Successfully reassigned {stats['files_reassigned']} files")
        
        if stats['reassignment_failures'] > 0:
            print(f"\n❌ Failed to reassign {stats['reassignment_failures']} files")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Error processing reassignments: {e}")
        raise
    finally:
        await close_db_pool()


async def display_miner_details(miner_id: str):
    """Display detailed information for a specific miner."""
    try:
        await init_db_pool()
        db_pool = get_db_pool()
        
        async with db_pool.acquire() as conn:
            # Get miner availability info
            miner_info = await conn.fetchrow("""
                SELECT * FROM miner_availability WHERE miner_id = $1
            """, miner_id)
            
            # Get recent failures
            recent_failures = await conn.fetch("""
                SELECT cid, failure_type, failure_reason, detected_at, retry_count
                FROM file_failures
                WHERE miner_id = $1 AND resolved_at IS NULL
                ORDER BY detected_at DESC
                LIMIT 20
            """, miner_id)
            
            # Get assigned files count
            assigned_files = await conn.fetchrow("""
                SELECT COUNT(*) as file_count
                FROM file_assignments
                WHERE $1 IN (miner1, miner2, miner3, miner4, miner5)
            """, miner_id)
        
        print("\n" + "="*80)
        print(f"MINER DETAILS: {miner_id}")
        print("="*80)
        
        if miner_info:
            print(f"Total Files Assigned: {assigned_files['file_count'] if assigned_files else 0}")
            print(f"Successful Checks: {miner_info['successful_checks']}")
            print(f"Failed Checks: {miner_info['failed_checks']}")
            print(f"Availability Score: {miner_info['availability_score']:.4f}")
            print(f"Consecutive Failures: {miner_info['consecutive_failures']}")
            print(f"Is Active: {'Yes' if miner_info['is_active'] else 'No'}")
            print(f"Last Successful Check: {miner_info['last_successful_check']}")
            print(f"Last Failed Check: {miner_info['last_failed_check']}")
            print(f"Updated At: {miner_info['updated_at']}")
        else:
            print("No availability data found for this miner")
        
        if recent_failures:
            print(f"\n🚨 RECENT FAILURES ({len(recent_failures)})")
            print("-" * 40)
            for failure in recent_failures:
                print(f"CID: {failure['cid']}")
                print(f"Type: {failure['failure_type']}")
                print(f"Reason: {failure['failure_reason']}")
                print(f"Detected: {failure['detected_at']}")
                print(f"Retries: {failure['retry_count']}")
                print()
        else:
            print("\n✅ No recent failures")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Error getting miner details: {e}")
        raise
    finally:
        await close_db_pool()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Query IPFS file availability reports")
    parser.add_argument('--report', action='store_true', help='Show overall availability report')
    parser.add_argument('--reassignments', action='store_true', help='Show files needing reassignment')
    parser.add_argument('--process', action='store_true', help='Process pending reassignments')
    parser.add_argument('--miner', type=str, help='Show details for specific miner')
    
    args = parser.parse_args()
    
    if args.report:
        asyncio.run(display_availability_report())
    elif args.reassignments:
        asyncio.run(display_files_needing_reassignment())
    elif args.process:
        asyncio.run(process_reassignments())
    elif args.miner:
        asyncio.run(display_miner_details(args.miner))
    else:
        # Default: show overall report
        asyncio.run(display_availability_report())


if __name__ == "__main__":
    main() 