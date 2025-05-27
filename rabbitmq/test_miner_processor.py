"""Test script to verify the miner profile processor functionality."""

import asyncio
import os
import sys

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.utils.config import NODE_URL

# Load environment variables
load_dotenv()


def test_miner_profile_parsing():
    """Test parsing of miner profile data."""
    print("\nTesting Miner Profile Parsing")
    print("-" * 60)
    
    # Sample data from the user's example
    sample_data = [
        [
            ["12D3KooWKnhGPbTtCgEPWRxGJhtFFcbMTEerfSKMpVnbpLQzByPx"],
            "bafkreihdcfmbfzxeirzz2ajuyplgjfjmadlav2qa54wycl26n5gmzyzgg4"
        ],
        [
            ["12D3KooWNLehimWAQ1z3AdtmDkUZsUJgJR4RbP5puGgxKtBpp1LK"],
            "bafkreiehp2mweh2v4e3sxsjzra4uxvw7z6weompr6npku5x4k3row7wycm"
        ]
    ]
    
    from rabbitmq.miner_profile_processor import MinerProfileProcessor
    
    processor = MinerProfileProcessor()
    parsed = processor.parse_miner_profile_data(sample_data)
    
    print(f"Parsed {len(parsed)} profiles:")
    for profile in parsed:
        print(f"  Node ID: {profile['node_id']}")
        print(f"  CID: {profile['cid']}")
        print()
    
    return True


def test_substrate_miner_profiles():
    """Test fetching miner profiles from substrate."""
    print("\nTesting Substrate Miner Profile Query")
    print("-" * 60)
    
    try:
        substrate = SubstrateInterface(url=NODE_URL)
        
        # Query miner profiles
        result = substrate.query_map(
            module='IpfsPallet',
            storage_function='MinerProfile'
        )
        
        count = 0
        for key, value in result:
            count += 1
            if count <= 3:  # Show first 3 entries
                print(f"  Node ID: {key.value}")
                print(f"  CID: {value.value}")
                print()
        
        print(f"✓ Found {count} miner profiles in storage")
        
        substrate.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to query miner profiles: {e}")
        return False


async def test_full_processor():
    """Test the full miner profile processor."""
    print("\nTesting Full Miner Profile Processor")
    print("-" * 60)
    
    from rabbitmq.miner_profile_processor import MinerProfileProcessor
    
    try:
        processor = MinerProfileProcessor()
        
        # Just test the connections
        processor.connect_substrate()
        print("✓ Connected to substrate")
        
        await processor.connect_rabbitmq()
        print("✓ Connected to RabbitMQ")
        print(f"✓ Queue '{processor.queue_name}' declared")
        
        # Clean up
        if processor.rabbitmq_connection:
            await processor.rabbitmq_connection.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Error in processor test: {e}")
        return False


async def main():
    """Run all tests."""
    print("=" * 60)
    print("MINER PROFILE PROCESSOR TESTS")
    print("=" * 60)
    
    # Test parsing
    parsing_ok = test_miner_profile_parsing()
    
    # Test substrate query
    substrate_ok = test_substrate_miner_profiles()
    
    # Test full processor
    processor_ok = await test_full_processor()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Parsing:   {'✓ OK' if parsing_ok else '✗ FAILED'}")
    print(f"Substrate: {'✓ OK' if substrate_ok else '✗ FAILED'}")
    print(f"Processor: {'✓ OK' if processor_ok else '✗ FAILED'}")
    
    if parsing_ok and substrate_ok and processor_ok:
        print("\nAll tests passed! You can run the miner profile processor.")
        return True
    else:
        print("\nSome tests failed. Please check the errors above.")
        return False


if __name__ == "__main__":
    asyncio.run(main()) 