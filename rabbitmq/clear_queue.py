"""Clear a RabbitMQ queue."""

import asyncio
import sys
import os

import aio_pika
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


async def clear_queue(queue_name: str):
    """Clear a RabbitMQ queue."""
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
    
    try:
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        
        await channel.queue_delete(queue_name)
        print(f"Queue '{queue_name}' deleted successfully")
        
        await connection.close()
        
    except Exception as e:
        print(f"Error: {e}")


async def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python clear_queue.py <queue_name>")
        sys.exit(1)
    
    queue_name = sys.argv[1]
    await clear_queue(queue_name)


if __name__ == "__main__":
    asyncio.run(main()) 