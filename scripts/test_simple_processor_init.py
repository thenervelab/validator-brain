#!/usr/bin/env python3
"""
Test Simple Processor Initialization

Quick test to verify the simple file assignment processor can initialize properly.
"""

import asyncio
import logging
import os
import sys

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


async def test_processor_initialization():
    """Test that the simple file assignment processor can initialize."""
    try:
        # Import the processor
        from rabbitmq.simple_file_assignment_processor import SimpleFileAssignmentProcessor
        
        logger.info("🧪 Testing Simple File Assignment Processor Initialization")
        logger.info("=" * 60)
        
        # Create processor instance
        processor = SimpleFileAssignmentProcessor()
        
        # Test initialization
        logger.info("🚀 Testing processor initialization...")
        success = await processor.initialize()
        
        if success:
            logger.info("✅ Processor initialized successfully!")
            
            # Test getting reliable miners
            logger.info("⛏️ Testing reliable miners retrieval...")
            reliable_miners = await processor.get_reliable_miners()
            logger.info(f"✅ Found {len(reliable_miners)} reliable miners")
            
            # Test cleanup
            logger.info("🧹 Testing processor cleanup...")
            await processor.cleanup()
            logger.info("✅ Processor cleanup successful!")
            
            logger.info("✅ All processor tests passed!")
            return True
        else:
            logger.error("❌ Processor initialization failed!")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error during processor test: {e}")
        logger.exception("Full traceback:")
        return False


async def main():
    """Main entry point."""
    success = await test_processor_initialization()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 