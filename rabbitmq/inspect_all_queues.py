"""Inspect all RabbitMQ queues to see message counts and contents."""

import asyncio
import json
import os

import aio_pika
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


async def inspect_queue(queue_name: str):
    """Inspect a RabbitMQ queue and show its contents."""
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
    
    try:
        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        
        try:
            # Declare the queue (passive=True to just check it exists)
            queue = await channel.declare_queue(queue_name, durable=True, passive=True)
            
            print(f"\nQueue: {queue_name}")
            print(f"Message count: {queue.declaration_result.message_count}")
            print(f"Consumer count: {queue.declaration_result.consumer_count}")
            print("-" * 60)
            
            # Get a few messages without consuming them (just peek)
            if queue.declaration_result.message_count > 0:
                print("Peeking at first few messages (not consuming):")
                print("-" * 60)
                
                # Note: We'll get messages without auto-ack to peek at them
                count = 0
                async for message in queue:
                    # Decode the message
                    data = json.loads(message.body.decode())
                    print(f"\nMessage {count + 1}:")
                    print(json.dumps(data, indent=2))
                    
                    # Reject the message to put it back in queue
                    await message.reject(requeue=True)
                    
                    count += 1
                    if count >= 3:  # Only show first 3
                        break
            else:
                print("Queue is empty")
                
        except aio_pika.exceptions.ChannelNotFoundEntity:
            print(f"\nQueue '{queue_name}' does not exist yet")
        
        await connection.close()
        
    except Exception as e:
        print(f"Error inspecting queue '{queue_name}': {e}")


async def main():
    """Main entry point."""
    print("=" * 60)
    print("RABBITMQ QUEUE INSPECTOR")
    print("=" * 60)
    
    # List of queues to inspect
    queues = ['user_profile', 'miner_profile']
    
    for queue_name in queues:
        await inspect_queue(queue_name)
    
    print("\n" + "=" * 60)
    print("Inspection complete")


if __name__ == "__main__":
    asyncio.run(main()) 