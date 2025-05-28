#!/usr/bin/env python3
"""
Queue Status Checker

This script checks the status of RabbitMQ queues to determine if they are empty
and processing is complete.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def check_queue_status(queue_names: List[str]) -> Dict[str, int]:
    """
    Check the message count for specified queues.
    
    Args:
        queue_names: List of queue names to check
        
    Returns:
        Dictionary mapping queue names to message counts
    """
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
    
    try:
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        
        queue_status = {}
        
        for queue_name in queue_names:
            try:
                # Declare queue (passive=True means don't create if it doesn't exist)
                queue = await channel.declare_queue(queue_name, durable=True, passive=True)
                message_count = queue.declaration_result.message_count
                queue_status[queue_name] = message_count
                
                logger.info(f"Queue '{queue_name}': {message_count} messages")
                
            except Exception as e:
                logger.warning(f"Could not check queue '{queue_name}': {e}")
                queue_status[queue_name] = -1  # -1 indicates error
        
        await connection.close()
        return queue_status
        
    except Exception as e:
        logger.error(f"Error connecting to RabbitMQ: {e}")
        return {queue: -1 for queue in queue_names}


async def wait_for_queues_empty(queue_names: List[str], timeout: int = 300, check_interval: int = 10) -> bool:
    """
    Wait for all specified queues to be empty.
    
    Args:
        queue_names: List of queue names to monitor
        timeout: Maximum time to wait in seconds
        check_interval: How often to check in seconds
        
    Returns:
        True if all queues are empty, False if timeout
    """
    logger.info(f"Waiting for queues to be empty: {', '.join(queue_names)}")
    
    start_time = asyncio.get_event_loop().time()
    
    while True:
        current_time = asyncio.get_event_loop().time()
        elapsed = current_time - start_time
        
        if elapsed > timeout:
            logger.warning(f"Timeout after {timeout} seconds waiting for queues to be empty")
            return False
        
        # Check queue status
        queue_status = await check_queue_status(queue_names)
        
        # Check if all queues are empty (0 messages) or have errors (-1)
        all_empty = True
        for queue_name, message_count in queue_status.items():
            if message_count > 0:
                all_empty = False
                logger.info(f"Queue '{queue_name}' still has {message_count} messages")
            elif message_count == -1:
                logger.warning(f"Could not check queue '{queue_name}' - assuming empty")
        
        if all_empty:
            logger.info(f"✅ All queues are empty after {elapsed:.1f} seconds")
            return True
        
        # Wait before next check
        await asyncio.sleep(check_interval)


async def main():
    """Main entry point for command line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Check RabbitMQ queue status')
    parser.add_argument('queues', nargs='+', help='Queue names to check')
    parser.add_argument('--wait', action='store_true', help='Wait for queues to be empty')
    parser.add_argument('--timeout', type=int, default=300, help='Timeout in seconds (default: 300)')
    parser.add_argument('--interval', type=int, default=10, help='Check interval in seconds (default: 10)')
    
    args = parser.parse_args()
    
    if args.wait:
        success = await wait_for_queues_empty(args.queues, args.timeout, args.interval)
        sys.exit(0 if success else 1)
    else:
        queue_status = await check_queue_status(args.queues)
        
        print(json.dumps(queue_status, indent=2))
        
        # Exit with non-zero if any queue has messages
        has_messages = any(count > 0 for count in queue_status.values())
        sys.exit(1 if has_messages else 0)


if __name__ == "__main__":
    asyncio.run(main()) 