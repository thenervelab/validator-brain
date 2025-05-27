"""
Utility script to inspect the miner health check queue without consuming messages.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def inspect_health_queue():
    """Inspect the miner health check queue."""
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
    queue_name = 'miner_health_check'
    
    try:
        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        
        # Declare the queue (passive=True means don't create if it doesn't exist)
        try:
            queue = await channel.declare_queue(queue_name, passive=True)
            message_count = queue.declaration_result.message_count
            consumer_count = queue.declaration_result.consumer_count
            
            print(f"Queue: {queue_name}")
            print(f"Messages in queue: {message_count}")
            print(f"Active consumers: {consumer_count}")
            print("-" * 50)
            
            if message_count > 0:
                print("Sample messages (without consuming):")
                
                # Get a few messages to inspect (without consuming them)
                sample_count = min(5, message_count)
                for i in range(sample_count):
                    try:
                        message = await queue.get(no_ack=False)
                        if message:
                            # Parse and display message content
                            try:
                                data = json.loads(message.body.decode())
                                print(f"\nMessage {i+1}:")
                                print(f"  Node ID: {data.get('node_id', 'N/A')}")
                                print(f"  IPFS Peer ID: {data.get('ipfs_peer_id', 'N/A')}")
                                print(f"  Epoch: {data.get('epoch', 'N/A')}")
                                print(f"  Files count: {len(data.get('files', []))}")
                                print(f"  Timestamp: {data.get('timestamp', 'N/A')}")
                                print(f"  Block number: {data.get('block_number', 'N/A')}")
                                
                                # Show first few files if any
                                files = data.get('files', [])
                                if files:
                                    print(f"  Sample files:")
                                    for j, file_cid in enumerate(files[:3]):
                                        print(f"    {j+1}. {file_cid}")
                                    if len(files) > 3:
                                        print(f"    ... and {len(files) - 3} more files")
                                
                            except json.JSONDecodeError:
                                print(f"  Raw message: {message.body.decode()[:100]}...")
                            
                            # Reject the message to put it back in the queue
                            await message.reject(requeue=True)
                        else:
                            break
                    except Exception as e:
                        logger.error(f"Error getting message {i+1}: {e}")
                        break
            else:
                print("No messages in queue")
                
        except aio_pika.exceptions.ChannelNotFoundError:
            print(f"Queue '{queue_name}' does not exist")
        
        await connection.close()
        
    except Exception as e:
        logger.error(f"Error inspecting queue: {e}")
        raise


async def main():
    """Main function."""
    try:
        await inspect_health_queue()
    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    asyncio.run(main()) 