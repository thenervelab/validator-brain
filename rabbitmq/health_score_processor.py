#!/usr/bin/env python3
"""
Health Score Processor

This processor:
1. Runs after health checks are completed
2. Calculates health scores from miner_epoch_health data
3. Updates miner_stats table with calculated health metrics
4. Ensures health scores are available for file assignment decisions
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.db.connection import get_db_pool, init_db_pool, close_db_pool
from app.utils.config import NODE_URL

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HealthScoreProcessor:
    """Processor for calculating and updating health scores from epoch health data."""
    
    def __init__(self):
        """Initialize the health score processor."""
        self.db_pool = None
        self.substrate = None
        
    async def connect(self):
        """Connect to database and substrate."""
        try:
            # Initialize database pool
            await init_db_pool()
            self.db_pool = get_db_pool()
            logger.info("Connected to database")
            
            # Connect to substrate (for current epoch info)
            self.substrate = SubstrateInterface(url=NODE_URL)
            logger.info("Connected to substrate")
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    def get_current_epoch(self) -> int:
        """Get the current epoch from the blockchain."""
        try:
            current_block = self.substrate.get_block_number(None)
            current_epoch = current_block // 100  # Assuming 100 blocks per epoch
            return current_epoch
        except Exception as e:
            logger.error(f"Error getting current epoch: {e}")
            return 0
    
    async def update_health_scores_from_epoch_data(self) -> Dict[str, int]:
        """
        Calculate health scores from epoch health data and update miner_stats.
        
        Returns:
            Dictionary with 'updated_count' and 'healthy_miners'
        """
        logger.info("🏥 Updating health scores from epoch health data")
        
        async with self.db_pool.acquire() as conn:
            # Get current epoch health data and calculate scores (most recent per miner)
            health_calculations = await conn.fetch("""
                SELECT DISTINCT ON (meh.node_id)
                    meh.node_id,
                    meh.epoch,
                    meh.ping_successes,
                    meh.ping_failures,
                    meh.pin_check_successes,
                    meh.pin_check_failures,
                    meh.last_activity_at
                FROM miner_epoch_health meh
                WHERE meh.last_activity_at >= NOW() - INTERVAL '2 hours'
                ORDER BY meh.node_id, meh.last_activity_at DESC
            """)
            
            logger.info(f"📊 Found {len(health_calculations):,} miners with recent health data")
            
            if not health_calculations:
                logger.warning("No recent health data found to process")
                return {'updated_count': 0, 'healthy_miners': 0}
            
            # Sanity check against registered miners
            total_registered_miners = await conn.fetchval(
                "SELECT COUNT(*) FROM registration WHERE node_type = 'StorageMiner' AND status = 'active'"
            )
            logger.info(f"📋 Expected active miners: {total_registered_miners:,}")
            
            if len(health_calculations) > total_registered_miners:
                logger.warning(f"⚠️ More health records ({len(health_calculations):,}) than registered miners ({total_registered_miners:,})")
            
            batch_data = []
            for health in health_calculations:
                node_id = health['node_id']
                ping_successes = health['ping_successes'] or 0
                ping_failures = health['ping_failures'] or 0
                pin_successes = health['pin_check_successes'] or 0
                pin_failures = health['pin_check_failures'] or 0
                
                total_successful = ping_successes + pin_successes
                total_checks = ping_successes + ping_failures + pin_successes + pin_failures
                
                batch_data.append((node_id, total_successful, total_checks))
            
            if batch_data:
                await conn.executemany("""
                    INSERT INTO miner_stats (
                        node_id, 
                        successful_pin_checks,
                        total_pin_checks,
                        updated_at
                    )
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (node_id) 
                    DO UPDATE SET 
                        successful_pin_checks = $2,
                        total_pin_checks = $3,
                        updated_at = NOW()
                """, batch_data)
                
                updated_count = len(batch_data)
            
            logger.info(f"✅ Updated health scores for {updated_count:,} miners")
            
            # Verify the updates
            healthy_miners = await conn.fetchval("""
                SELECT COUNT(*) FROM miner_stats 
                WHERE health_score > 0
            """)
            
            logger.info(f"📈 Verification: {healthy_miners:,} miners now have health scores > 0")
            
            # Log sample of updated health scores
            sample_health = await conn.fetch("""
                SELECT node_id, health_score, updated_at 
                FROM miner_stats 
                WHERE health_score > 0 
                ORDER BY updated_at DESC 
                LIMIT 5
            """)
            
            logger.info("📊 Sample updated health scores:")
            for miner in sample_health:
                node_short = miner['node_id'][:20] + "..." if len(miner['node_id']) > 20 else miner['node_id']
                score = miner['health_score'] or 0
                updated = miner['updated_at'].strftime("%H:%M:%S") if miner['updated_at'] else 'unknown'
                logger.info(f"   ✅ {node_short} - {score:.1f}% @ {updated}")
            
            return {
                'updated_count': updated_count,
                'healthy_miners': healthy_miners
            }
    
    async def process_health_scores(self) -> bool:
        """
        Main processing function for health score updates.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            current_epoch = self.get_current_epoch()
            current_block = self.substrate.get_block_number(None) if self.substrate else None
            
            logger.info(f"🏥 Processing health scores for epoch {current_epoch} (block: {current_block})")
            
            # Update health scores from epoch health data
            results = await self.update_health_scores_from_epoch_data()
            
            if results['updated_count'] > 0:
                logger.info(f"✅ Health score processing completed successfully")
                logger.info(f"   Updated: {results['updated_count']:,} miners")
                logger.info(f"   Healthy: {results['healthy_miners']:,} miners")
                return True
            else:
                logger.warning("⚠️ No health scores were updated")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error processing health scores: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def close(self):
        """Close all connections."""
        if self.substrate:
            self.substrate.close()
            logger.info("Closed substrate connection")
        
        if self.db_pool:
            await close_db_pool()
            logger.info("Closed database pool")


async def main():
    """Main entry point for the health score processor."""
    processor = HealthScoreProcessor()
    
    try:
        # Connect to services
        await processor.connect()
        
        # Process health scores
        success = await processor.process_health_scores()
        
        if success:
            logger.info("🎯 Health score processing completed successfully")
        else:
            logger.error("❌ Health score processing failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Health score processor error: {e}")
        sys.exit(1)
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main()) 