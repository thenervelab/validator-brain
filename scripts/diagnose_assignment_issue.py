#!/usr/bin/env python3
"""
Diagnostic Script for Assignment Issues

Checks the current state of:
1. Health data availability
2. File assignment status
3. Miner availability
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def diagnose_assignment_issue():
    """Diagnose the current assignment and health data state."""
    try:
        from app.db.connection import init_db_pool, get_db_pool, close_db_pool
        
        # Initialize database
        await init_db_pool()
        db_pool = await get_db_pool()
        
        async with db_pool.acquire() as conn:
            logger.info("🔍 ASSIGNMENT DIAGNOSIS REPORT")
            logger.info("=" * 60)
            
            # 1. Check health data availability
            logger.info("\n1. HEALTH DATA AVAILABILITY:")
            
            # Recent health data (last 2 hours)
            recent_health = await conn.fetchval("""
                SELECT COUNT(*) FROM miner_epoch_health 
                WHERE updated_at >= NOW() - INTERVAL '2 hours'
            """)
            
            # Fallback health data (last 8 hours)
            fallback_health = await conn.fetchval("""
                SELECT COUNT(*) FROM miner_epoch_health 
                WHERE updated_at >= NOW() - INTERVAL '8 hours'
            """)
            
            # All health data
            total_health = await conn.fetchval("SELECT COUNT(*) FROM miner_epoch_health")
            
            logger.info(f"   Recent health data (2h): {recent_health} miners")
            logger.info(f"   Fallback health data (8h): {fallback_health} miners")
            logger.info(f"   Total health records: {total_health} miners")
            
            if recent_health == 0 and fallback_health == 0:
                logger.error("   🚨 NO HEALTH DATA AVAILABLE FOR ASSIGNMENTS!")
            elif recent_health == 0:
                logger.warning("   ⚠️ No recent health data, using fallback")
            else:
                logger.info("   ✅ Fresh health data available")
            
            # 2. Check file assignment status
            logger.info("\n2. FILE ASSIGNMENT STATUS:")
            
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
            
            if total_files > 0:
                coverage = (files_with_miners / total_files) * 100
                logger.info(f"   Total files: {total_files}")
                logger.info(f"   Files with miners: {files_with_miners} ({coverage:.1f}%)")
                logger.info(f"   Empty assignments: {empty_assignments}")
                
                if coverage < 50:
                    logger.error("   🚨 VERY LOW ASSIGNMENT COVERAGE!")
                elif coverage < 80:
                    logger.warning("   ⚠️ Low assignment coverage")
                else:
                    logger.info("   ✅ Good assignment coverage")
            else:
                logger.warning("   ⚠️ No files found in assignments table")
            
            # 3. Check miner availability
            logger.info("\n3. MINER AVAILABILITY:")
            
            miner_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_miners,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_miners,
                    COUNT(CASE WHEN node_type = 'StorageMiner' AND status = 'active' THEN 1 END) as storage_miners
                FROM registration
            """)
            
            total_miners = miner_stats['total_miners'] or 0
            active_miners = miner_stats['active_miners'] or 0
            storage_miners = miner_stats['storage_miners'] or 0
            
            logger.info(f"   Total registered miners: {total_miners}")
            logger.info(f"   Active miners: {active_miners}")
            logger.info(f"   Storage miners: {storage_miners}")
            
            if storage_miners < 5:
                logger.error("   🚨 INSUFFICIENT STORAGE MINERS FOR ASSIGNMENTS!")
            elif storage_miners < 20:
                logger.warning("   ⚠️ Low number of storage miners")
            else:
                logger.info("   ✅ Good number of storage miners")
            
            # 4. Check recent epochs in health data
            logger.info("\n4. HEALTH DATA BY EPOCH:")
            
            epoch_health = await conn.fetch("""
                SELECT 
                    epoch,
                    COUNT(*) as miner_count,
                    MIN(updated_at) as earliest_update,
                    MAX(updated_at) as latest_update
                FROM miner_epoch_health 
                WHERE epoch IS NOT NULL
                GROUP BY epoch 
                ORDER BY epoch DESC 
                LIMIT 5
            """)
            
            if epoch_health:
                for row in epoch_health:
                    epoch = row['epoch']
                    count = row['miner_count']
                    latest = row['latest_update']
                    age = datetime.now() - latest if latest else None
                    age_str = f"{age.total_seconds()/3600:.1f}h ago" if age else "unknown"
                    logger.info(f"   Epoch {epoch}: {count} miners (latest: {age_str})")
            else:
                logger.error("   🚨 NO EPOCH HEALTH DATA FOUND!")
            
            # 5. Test assignment with current data
            logger.info("\n5. ASSIGNMENT TEST:")
            
            # Try to get reliable miners using the same logic as SimpleFileAssigner
            cutoff_date = datetime.now() - timedelta(days=1)
            
            test_miners = await conn.fetch("""
                SELECT 
                    r.node_id,
                    COALESCE(ms.health_score, 100) as health_score,
                    meh.updated_at as health_updated
                FROM registration r
                LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                LEFT JOIN (
                    SELECT DISTINCT ON (node_id) 
                        node_id, updated_at
                    FROM miner_epoch_health 
                    ORDER BY node_id, updated_at DESC
                ) meh ON r.node_id = meh.node_id
                WHERE r.node_type = 'StorageMiner' 
                  AND r.status = 'active'
                  AND r.registered_at <= $1
                  AND COALESCE(ms.health_score, 100) >= 50
                LIMIT 10
            """, cutoff_date)
            
            if test_miners:
                logger.info(f"   Found {len(test_miners)} miners available for assignment:")
                for miner in test_miners[:5]:  # Show first 5
                    health_age = "no data"
                    if miner['health_updated']:
                        age = datetime.now() - miner['health_updated']
                        health_age = f"{age.total_seconds()/3600:.1f}h ago"
                    logger.info(f"     {miner['node_id']}: health_score={miner['health_score']}, last_check={health_age}")
                if len(test_miners) > 5:
                    logger.info(f"     ... and {len(test_miners) - 5} more")
            else:
                logger.error("   🚨 NO MINERS AVAILABLE FOR ASSIGNMENT!")
            
            logger.info("\n" + "=" * 60)
            logger.info("📋 SUMMARY:")
            
            if recent_health == 0 and fallback_health == 0 and total_health == 0:
                logger.error("🚨 ROOT CAUSE: NO HEALTH DATA - health checks never ran or data was deleted")
                logger.error("   SOLUTION: Run health checks manually or check epoch orchestrator health check phase")
            elif storage_miners < 5:
                logger.error("🚨 ROOT CAUSE: INSUFFICIENT STORAGE MINERS")
                logger.error("   SOLUTION: Wait for more miners to register or check registration data")
            elif empty_assignments > total_files * 0.5:
                logger.error("🚨 ROOT CAUSE: ASSIGNMENT LOGIC FAILING")
                logger.error("   SOLUTION: Run manual assignment or check SimpleFileAssigner logic")
            else:
                logger.info("✅ System appears healthy - assignments should be working")
        
        # Close database
        await close_db_pool()
        
    except Exception as e:
        logger.error(f"❌ Error during diagnosis: {e}")
        logger.exception("Full traceback:")


async def main():
    """Main entry point."""
    await diagnose_assignment_issue()


if __name__ == "__main__":
    asyncio.run(main()) 