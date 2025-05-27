"""Test script to verify connections to Substrate and RabbitMQ."""

import asyncio
import os
import sys

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from substrateinterface import SubstrateInterface
import aio_pika

from app.utils.config import NODE_URL

# Load environment variables
load_dotenv()


def test_substrate_connection():
    """Test connection to Substrate node."""
    print(f"\nTesting Substrate connection to: {NODE_URL}")
    print("-" * 60)
    
    try:
        substrate = SubstrateInterface(url=NODE_URL)
        
        # Get chain info
        chain = substrate.chain
        print(f"✓ Connected to chain: {chain}")
        
        # Get latest block
        block = substrate.get_block_number(substrate.get_chain_head())
        print(f"✓ Latest block number: {block}")
        
        # Test query for UserProfile storage
        print("\nTesting UserProfile storage query...")
        result = substrate.query_map(
            module='IpfsPallet',
            storage_function='UserProfile'
        )
        
        count = 0
        for key, value in result:
            count += 1
            if count <= 3:  # Show first 3 entries
                print(f"  Account: {key.value}")
                print(f"  CID: {value.value}")
                print()
        
        print(f"✓ Found {count} user profiles in storage")
        
        substrate.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to connect to Substrate: {e}")
        return False


async def test_rabbitmq_connection():
    """Test connection to RabbitMQ."""
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
    
    print(f"\nTesting RabbitMQ connection to: {rabbitmq_url}")
    print("-" * 60)
    
    try:
        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()
        
        print("✓ Connected to RabbitMQ")
        
        # Declare test queue
        queue = await channel.declare_queue('test_queue', durable=True)
        print(f"✓ Declared test queue: {queue.name}")
        
        # Clean up
        await connection.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to connect to RabbitMQ: {e}")
        print("\nMake sure RabbitMQ is running:")
        print("  docker-compose up -d rabbitmq")
        return False


async def main():
    """Run all connection tests."""
    print("=" * 60)
    print("CONNECTION TESTS")
    print("=" * 60)
    
    # Test Substrate
    substrate_ok = test_substrate_connection()
    
    # Test RabbitMQ
    rabbitmq_ok = await test_rabbitmq_connection()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Substrate: {'✓ OK' if substrate_ok else '✗ FAILED'}")
    print(f"RabbitMQ:  {'✓ OK' if rabbitmq_ok else '✗ FAILED'}")
    
    if not substrate_ok or not rabbitmq_ok:
        print("\nPlease fix the connection issues before running the processor.")
        return False
    
    print("\nAll connections successful! You can run the processor.")
    return True


if __name__ == "__main__":
    asyncio.run(main()) 