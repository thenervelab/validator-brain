"""Test the node metrics consumer with one message."""

import asyncio
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set DATABASE_URL if not set
if not os.getenv('DATABASE_URL'):
    os.environ['DATABASE_URL'] = 'postgresql://user:password@localhost:5432/substrate_fetcher'

# Import after setting env vars
from rabbitmq.node_metrics_consumer import NodeMetricsConsumer


async def test_consumer():
    """Test processing one message."""
    consumer = NodeMetricsConsumer()
    
    try:
        # Initialize connections
        await consumer.init_database()
        await consumer.connect_rabbitmq()
        
        # Get the queue
        queue = await consumer.rabbitmq_channel.declare_queue(
            consumer.queue_name,
            durable=True
        )
        
        print(f"Queue has {queue.declaration_result.message_count} messages")
        
        # Get one message
        message = await queue.get(timeout=5)
        if message:
            print(f"Processing one message...")
            await consumer.process_message(message)
            print("Done!")
        else:
            print("No messages in queue")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(test_consumer()) 