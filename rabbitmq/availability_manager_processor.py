#!/usr/bin/env python3
"""
Availability Manager Processor

This processor automatically runs the file availability manager to:
1. Handle files with empty miner assignments
2. Reassign files from failing miners
3. Maintain file availability across the network

Runs as part of the epoch workflow to ensure files don't get stuck.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from app.db.connection import init_db_pool, close_db_pool, get_db_pool
from substrate_fetcher.file_availability_manager import FileAvailabilityManager, AvailabilityRules

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AvailabilityManagerProcessor:
    def __init__(self):
        self.db_pool = None
        self.availability_manager = None
        
        # Configuration
        self.enable_reassignments = os.getenv('ENABLE_AUTOMATIC_REASSIGNMENTS', 'true').lower() == 'true'
        self.max_files_per_run = int(os.getenv('MAX_AVAILABILITY_FILES_PER_RUN', '50'))
        
        # Availability rules configuration
        self.rules = AvailabilityRules(
            min_replicas=int(os.getenv('MIN_REPLICAS_PER_FILE', '5')),
            max_replicas=int(os.getenv('MAX_REPLICAS_PER_FILE', '5')),
            min_availability_score=float(os.getenv('MIN_AVAILABILITY_SCORE', '0.7')),
            max_consecutive_failures=int(os.getenv('MAX_CONSECUTIVE_FAILURES', '3')),
            failure_window_hours=int(os.getenv('FAILURE_WINDOW_HOURS', '24')),
            reassignment_cooldown_hours=int(os.getenv('REASSIGNMENT_COOLDOWN_HOURS', '6'))
        )
    
    async def initialize(self):
        """Initialize the availability manager processor."""
        try:
            # Initialize database connection
            await init_db_pool()
            self.db_pool = get_db_pool()
            
            # Initialize availability manager
            self.availability_manager = FileAvailabilityManager(self.db_pool, self.rules)
            
            logger.info("Availability manager processor initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize availability manager processor: {e}")
            return False
    
    async def check_empty_assignments(self) -> dict:
        """Check for and fix files with empty miner assignments."""
        stats = {
            'files_checked': 0,
            'files_fixed': 0,
            'files_failed': 0
        }
        
        try:
            if not self.db_pool:
                logger.error("Database pool not initialized")
                return stats
            
            logger.info("🔍 Checking for files with empty miner assignments...")
            
            async with self.db_pool.acquire() as conn:
                # Find files with empty assignments (all miners NULL)
                empty_files = await conn.fetch("""
                    SELECT 
                        fa.cid,
                        fa.owner,
                        f.name as filename,
                        f.size
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
                      AND fa.miner4 IS NULL AND fa.miner5 IS NULL
                    ORDER BY fa.updated_at ASC
                    LIMIT $1
                """, self.max_files_per_run)
                
                stats['files_checked'] = len(empty_files)
                
                if not empty_files:
                    logger.info("✅ No files with empty assignments found")
                    return stats
                
                logger.info(f"📋 Found {len(empty_files)} files with empty assignments")
                
                # Process each file
                for file_info in empty_files:
                    cid = file_info['cid']
                    filename = file_info['filename']
                    
                    try:
                        logger.info(f"🔧 Fixing empty assignment for {filename} ({cid[:16]}...)")
                        
                        # Use availability manager to assign replicas
                        success = await self.availability_manager.reassign_file_replicas(cid, [])
                        
                        if success:
                            stats['files_fixed'] += 1
                            logger.info(f"✅ Fixed assignment for {filename}")
                        else:
                            stats['files_failed'] += 1
                            logger.warning(f"❌ Failed to fix assignment for {filename}")
                            
                    except Exception as e:
                        stats['files_failed'] += 1
                        logger.error(f"❌ Error fixing assignment for {filename}: {e}")
            
            logger.info(f"Empty assignments check complete: {stats['files_fixed']} fixed, {stats['files_failed']} failed")
            return stats
            
        except Exception as e:
            logger.error(f"Error checking empty assignments: {e}")
            return stats
    
    async def process_availability_failures(self) -> dict:
        """Process files that need reassignment due to availability failures."""
        stats = {
            'files_checked': 0,
            'files_reassigned': 0,
            'reassignment_failures': 0
        }
        
        try:
            if not self.availability_manager:
                logger.error("Availability manager not initialized")
                return stats
            
            logger.info("🔄 Processing availability-based reassignments...")
            
            # Use the availability manager's built-in reassignment logic
            result_stats = await self.availability_manager.process_reassignments()
            
            # Map the stats to our format
            stats['files_checked'] = result_stats.get('files_checked', 0)
            stats['files_reassigned'] = result_stats.get('files_reassigned', 0)
            stats['reassignment_failures'] = result_stats.get('reassignment_failures', 0)
            
            if stats['files_checked'] > 0:
                logger.info(f"Availability reassignments complete: {stats['files_reassigned']} reassigned, {stats['reassignment_failures']} failed")
            else:
                logger.info("✅ No files need availability-based reassignment")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error processing availability failures: {e}")
            return stats
    
    async def generate_availability_report(self) -> bool:
        """Generate and log availability report."""
        try:
            if not self.availability_manager:
                logger.error("Availability manager not initialized")
                return False
            
            logger.info("📊 Generating availability report...")
            
            report = await self.availability_manager.get_availability_report()
            
            # Log key metrics
            overall = report.get('overall_stats', {})
            failures = report.get('failure_stats', {})
            
            logger.info(f"📈 Availability Summary:")
            logger.info(f"   Total miners: {overall.get('total_miners', 0)}")
            logger.info(f"   Active miners: {overall.get('active_miners', 0)}")
            logger.info(f"   Reliable miners: {overall.get('reliable_miners', 0)}")
            logger.info(f"   Avg availability score: {overall.get('avg_availability_score', 0):.3f}")
            logger.info(f"   Files with failures: {failures.get('files_with_failures', 0)}")
            logger.info(f"   Recent failures: {failures.get('total_failures', 0)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error generating availability report: {e}")
            return False
    
    async def run_maintenance(self) -> dict:
        """Run complete availability maintenance cycle."""
        start_time = datetime.utcnow()
        logger.info("🛠️ Starting automatic availability maintenance")
        
        total_stats = {
            'empty_assignments_fixed': 0,
            'availability_reassignments': 0,
            'total_failures': 0,
            'execution_time_seconds': 0
        }
        
        try:
            # Step 1: Check for empty assignments
            logger.info("Phase 1: Checking empty assignments")
            empty_stats = await self.check_empty_assignments()
            total_stats['empty_assignments_fixed'] = empty_stats['files_fixed']
            total_stats['total_failures'] += empty_stats['files_failed']
            
            # Step 2: Process availability-based reassignments (if enabled)
            if self.enable_reassignments:
                logger.info("Phase 2: Processing availability reassignments")
                availability_stats = await self.process_availability_failures()
                total_stats['availability_reassignments'] = availability_stats['files_reassigned']
                total_stats['total_failures'] += availability_stats['reassignment_failures']
            else:
                logger.info("Phase 2: Skipped (automatic reassignments disabled)")
            
            # Step 3: Generate availability report
            logger.info("Phase 3: Generating availability report")
            await self.generate_availability_report()
            
            # Calculate execution time
            end_time = datetime.utcnow()
            total_stats['execution_time_seconds'] = (end_time - start_time).total_seconds()
            
            logger.info("✅ Availability maintenance complete")
            logger.info(f"   Empty assignments fixed: {total_stats['empty_assignments_fixed']}")
            logger.info(f"   Availability reassignments: {total_stats['availability_reassignments']}")
            logger.info(f"   Total failures: {total_stats['total_failures']}")
            logger.info(f"   Execution time: {total_stats['execution_time_seconds']:.1f} seconds")
            
            return total_stats
            
        except Exception as e:
            logger.error(f"Error during availability maintenance: {e}")
            total_stats['total_failures'] += 1
            return total_stats
    
    async def cleanup(self):
        """Clean up resources."""
        try:
            if self.db_pool:
                await close_db_pool()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


async def main():
    """Main entry point."""
    processor = AvailabilityManagerProcessor()
    
    try:
        # Initialize
        success = await processor.initialize()
        if not success:
            logger.error("Failed to initialize processor")
            return 1
        
        # Run maintenance
        stats = await processor.run_maintenance()
        
        # Return appropriate exit code
        if stats['total_failures'] > 0:
            logger.warning(f"Completed with {stats['total_failures']} failures")
            return 1
        else:
            logger.info("Completed successfully")
            return 0
            
    except Exception as e:
        logger.error(f"Fatal error in availability manager processor: {e}")
        return 1
    finally:
        await processor.cleanup()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 