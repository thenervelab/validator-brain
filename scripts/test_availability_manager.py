#!/usr/bin/env python3
"""
Test Availability Manager Processor

This script tests the availability manager processor to ensure it can:
1. Connect to the database
2. Initialize the availability manager
3. Check for empty assignments
4. Process availability failures
5. Generate reports

Usage: python scripts/test_availability_manager.py
"""

import asyncio
import logging
import os
import sys
from datetime import datetime

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


async def test_availability_manager():
    """Test the availability manager processor functionality."""
    logger.info("🧪 Testing Availability Manager Processor")
    
    try:
        # Import and test the availability manager processor
        from rabbitmq.availability_manager_processor import AvailabilityManagerProcessor
        
        processor = AvailabilityManagerProcessor()
        
        # Test 1: Initialization
        logger.info("Test 1: Initializing availability manager processor...")
        success = await processor.initialize()
        if not success:
            logger.error("❌ Failed to initialize availability manager processor")
            return False
        
        logger.info("✅ Availability manager processor initialized successfully")
        
        # Test 2: Check configuration
        logger.info("Test 2: Checking configuration...")
        logger.info(f"   Enable reassignments: {processor.enable_reassignments}")
        logger.info(f"   Max files per run: {processor.max_files_per_run}")
        logger.info(f"   Min replicas: {processor.rules.min_replicas}")
        logger.info(f"   Max replicas: {processor.rules.max_replicas}")
        logger.info(f"   Min availability score: {processor.rules.min_availability_score}")
        logger.info(f"   Max consecutive failures: {processor.rules.max_consecutive_failures}")
        logger.info(f"   Failure window: {processor.rules.failure_window_hours}h")
        logger.info(f"   Reassignment cooldown: {processor.rules.reassignment_cooldown_hours}h")
        
        # Test 3: Check empty assignments (dry run)
        logger.info("Test 3: Checking for empty assignments...")
        empty_stats = await processor.check_empty_assignments()
        logger.info(f"   Files checked: {empty_stats['files_checked']}")
        logger.info(f"   Files fixed: {empty_stats['files_fixed']}")
        logger.info(f"   Files failed: {empty_stats['files_failed']}")
        
        # Test 4: Check availability failures (dry run)
        if processor.enable_reassignments:
            logger.info("Test 4: Checking availability-based reassignments...")
            availability_stats = await processor.process_availability_failures()
            logger.info(f"   Files checked: {availability_stats['files_checked']}")
            logger.info(f"   Files reassigned: {availability_stats['files_reassigned']}")
            logger.info(f"   Reassignment failures: {availability_stats['reassignment_failures']}")
        else:
            logger.info("Test 4: Skipped (automatic reassignments disabled)")
        
        # Test 5: Generate availability report
        logger.info("Test 5: Generating availability report...")
        report_success = await processor.generate_availability_report()
        if report_success:
            logger.info("✅ Availability report generated successfully")
        else:
            logger.warning("⚠️ Availability report generation failed")
        
        # Test 6: Full maintenance cycle (dry run)
        logger.info("Test 6: Running full maintenance cycle...")
        maintenance_stats = await processor.run_maintenance()
        logger.info(f"   Empty assignments fixed: {maintenance_stats['empty_assignments_fixed']}")
        logger.info(f"   Availability reassignments: {maintenance_stats['availability_reassignments']}")
        logger.info(f"   Total failures: {maintenance_stats['total_failures']}")
        logger.info(f"   Execution time: {maintenance_stats['execution_time_seconds']:.1f}s")
        
        # Cleanup
        await processor.cleanup()
        
        logger.info("✅ All availability manager tests completed successfully")
        
        # Summary
        total_issues_found = empty_stats['files_checked']
        total_fixes = empty_stats['files_fixed'] + maintenance_stats['availability_reassignments']
        total_failures = maintenance_stats['total_failures']
        
        logger.info("\n📊 Test Summary:")
        logger.info(f"   Issues found: {total_issues_found}")
        logger.info(f"   Issues fixed: {total_fixes}")
        logger.info(f"   Failures: {total_failures}")
        
        if total_failures == 0:
            logger.info("🎉 Availability manager is working correctly!")
            return True
        else:
            logger.warning(f"⚠️ Availability manager completed with {total_failures} failures")
            return False
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        logger.exception("Full traceback:")
        return False


async def check_prerequisites():
    """Check if all prerequisites are met for testing."""
    logger.info("🔍 Checking prerequisites...")
    
    # Check environment variables
    required_vars = ['DATABASE_URL', 'VALIDATOR_ACCOUNT_ID']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    # Check optional availability configuration
    availability_vars = [
        'ENABLE_AUTOMATIC_REASSIGNMENTS',
        'MAX_AVAILABILITY_FILES_PER_RUN', 
        'MIN_REPLICAS_PER_FILE',
        'MAX_REPLICAS_PER_FILE',
        'MIN_AVAILABILITY_SCORE',
        'MAX_CONSECUTIVE_FAILURES',
        'FAILURE_WINDOW_HOURS',
        'REASSIGNMENT_COOLDOWN_HOURS'
    ]
    
    set_vars = []
    for var in availability_vars:
        if os.getenv(var):
            set_vars.append(var)
    
    logger.info(f"✅ Required variables: {len(required_vars)}/{len(required_vars)} set")
    logger.info(f"✅ Availability variables: {len(set_vars)}/{len(availability_vars)} set")
    
    if len(set_vars) < len(availability_vars):
        logger.info("💡 Some availability variables not set - will use defaults")
    
    return True


async def main():
    """Main test runner."""
    start_time = datetime.utcnow()
    logger.info("🚀 Starting Availability Manager Test Suite")
    
    try:
        # Check prerequisites
        prereq_success = await check_prerequisites()
        if not prereq_success:
            logger.error("❌ Prerequisites not met")
            return 1
        
        # Run tests
        test_success = await test_availability_manager()
        
        # Calculate execution time
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        if test_success:
            logger.info(f"✅ All tests passed in {execution_time:.1f} seconds")
            return 0
        else:
            logger.error(f"❌ Some tests failed in {execution_time:.1f} seconds")
            return 1
            
    except KeyboardInterrupt:
        logger.info("🛑 Test interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Fatal error during testing: {e}")
        logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 