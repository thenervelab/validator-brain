#!/usr/bin/env python3
"""
Check Miner Health Score Distribution

Quick script to see what health scores your miners actually have.
This helps determine if MIN_MINER_HEALTH_SCORE is set appropriately.
"""

import asyncio
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def check_health_distribution():
    """Check the health score distribution of miners."""
    try:
        await init_db_pool()
        db_pool = await get_db_pool()
        
        logger.info("🏥 Checking Miner Health Score Distribution")
        logger.info("=" * 60)
        
        async with db_pool.acquire() as conn:
            # Get health score distribution
            health_stats = await conn.fetch("""
                SELECT 
                    r.node_id,
                    COALESCE(ms.health_score, 100) as health_score,
                    ms.total_files_pinned,
                    ms.last_online_block
                FROM registration r
                LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                WHERE r.node_type = 'StorageMiner' 
                  AND r.status = 'active'
                ORDER BY COALESCE(ms.health_score, 100) DESC
            """)
            
            if not health_stats:
                logger.warning("No active storage miners found!")
                return
            
            logger.info(f"Found {len(health_stats)} active storage miners")
            
            # Calculate distribution
            scores = [float(row['health_score']) for row in health_stats]
            total_miners = len(scores)
            
            # Count miners in different health ranges
            excellent = sum(1 for s in scores if s >= 90)
            good = sum(1 for s in scores if 70 <= s < 90)
            fair = sum(1 for s in scores if 50 <= s < 70)
            poor = sum(1 for s in scores if 30 <= s < 50)
            critical = sum(1 for s in scores if s < 30)
            
            logger.info("\n📊 Health Score Distribution:")
            logger.info(f"   🟢 Excellent (90-100%): {excellent} miners ({excellent/total_miners*100:.1f}%)")
            logger.info(f"   🔵 Good (70-89%):       {good} miners ({good/total_miners*100:.1f}%)")
            logger.info(f"   🟡 Fair (50-69%):       {fair} miners ({fair/total_miners*100:.1f}%)")
            logger.info(f"   🟠 Poor (30-49%):       {poor} miners ({poor/total_miners*100:.1f}%)")
            logger.info(f"   🔴 Critical (<30%):     {critical} miners ({critical/total_miners*100:.1f}%)")
            
            # Check current threshold setting
            current_threshold = float(os.getenv('MIN_MINER_HEALTH_SCORE', '70.0'))
            available_at_threshold = sum(1 for s in scores if s >= current_threshold)
            
            logger.info(f"\n🎯 Current threshold: {current_threshold}%")
            logger.info(f"   Available miners: {available_at_threshold}/{total_miners} ({available_at_threshold/total_miners*100:.1f}%)")
            
            if available_at_threshold < 10:
                logger.error(f"🚨 CRITICAL: Only {available_at_threshold} miners meet threshold!")
                logger.error("   This is likely causing assignment failures")
                logger.error("   Consider lowering MIN_MINER_HEALTH_SCORE")
            elif available_at_threshold < total_miners * 0.5:
                logger.warning(f"⚠️ WARNING: Only {available_at_threshold/total_miners*100:.1f}% of miners meet threshold")
                logger.warning("   This may cause assignment issues")
            else:
                logger.info(f"✅ Good: {available_at_threshold/total_miners*100:.1f}% of miners meet threshold")
            
            # Show recommendations
            logger.info(f"\n💡 Recommendations:")
            if available_at_threshold < 5:
                logger.info("   🔥 EMERGENCY: Set MIN_MINER_HEALTH_SCORE=10.0 (allow almost all miners)")
            elif available_at_threshold < 10:
                logger.info("   🚨 CRITICAL: Set MIN_MINER_HEALTH_SCORE=30.0 (very permissive)")
            elif available_at_threshold < total_miners * 0.7:
                logger.info("   ⚠️ MODERATE: Set MIN_MINER_HEALTH_SCORE=50.0 (somewhat permissive)")
            else:
                logger.info("   ✅ GOOD: Current threshold seems reasonable")
            
            # Show worst miners for debugging
            logger.info(f"\n🔍 Miners with lowest health scores (bottom 5):")
            worst_miners = sorted(health_stats, key=lambda x: x['health_score'])[:5]
            for miner in worst_miners:
                node_id = miner['node_id']
                score = miner['health_score']
                files = miner['total_files_pinned'] or 0
                logger.info(f"   {node_id[:20]}...: {score:.1f}% ({files} files)")
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ Health distribution check completed")
        
    except Exception as e:
        logger.error(f"❌ Error checking health distribution: {e}")
    finally:
        await close_db_pool()


async def main():
    """Main entry point."""
    await check_health_distribution()


if __name__ == "__main__":
    asyncio.run(main()) 