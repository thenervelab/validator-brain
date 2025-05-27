"""Test script for user profile consumer functionality."""

import asyncio
import json
import os
import sys

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from rabbitmq.user_profile_consumer import UserProfileConsumer
from substrate_fetcher.ipfs_profile_parser import parse_user_profile_files

# Load environment variables
load_dotenv()


async def test_ipfs_fetch():
    """Test fetching from IPFS."""
    print("\nTesting IPFS Fetch")
    print("-" * 60)
    
    consumer = UserProfileConsumer()
    
    # Test with a known CID (you may need to replace with a valid one)
    test_cid = "bafkreicx4yjeyjgazxz7tvzuyhvl565wka2oqa566pbsc5u5tfvnlk3vzq"
    
    print(f"Fetching CID: {test_cid}")
    content = await consumer.fetch_from_ipfs(test_cid)
    
    if content:
        print(f"✓ Fetched {len(content)} bytes")
        print(f"  Content preview: {content[:100]}...")
        return True
    else:
        print("✗ Failed to fetch content")
        return False


async def test_profile_parsing():
    """Test parsing a sample user profile."""
    print("\nTesting Profile Parsing")
    print("-" * 60)
    
    # Sample user profile data (simulated)
    sample_profile = [
        {
            "file_hash": [98, 97, 102, 107, 114, 101, 105, 104, 100, 99, 102, 109, 98, 102, 122, 120, 101, 105, 114, 122, 122, 50, 97, 106, 117, 121, 112, 108, 103, 106, 102, 106, 109, 97, 100, 108, 97, 118, 50, 113, 97, 53, 52, 119, 121, 99, 108, 50, 54, 110, 53, 103, 109, 122, 121, 122, 103, 103, 52],
            "file_name": "test_file.txt",
            "file_size_in_bytes": 1024,
            "is_assigned": True,
            "last_charged_at": 1234567890,
            "main_req_hash": "0x1234567890abcdef",
            "miner_ids": [
                "12D3KooWKnhGPbTtCgEPWRxGJhtFFcbMTEerfSKMpVnbpLQzByPx",
                "12D3KooWNLehimWAQ1z3AdtmDkUZsUJgJR4RbP5puGgxKtBpp1LK"
            ],
            "total_replicas": 2
        }
    ]
    
    try:
        files = parse_user_profile_files(sample_profile)
        print(f"✓ Parsed {len(files)} files")
        for file in files:
            print(f"  File: {file['file_name']}")
            print(f"  CID: {file['file_hash']}")
            print(f"  Size: {file['file_size_in_bytes']} bytes")
            print(f"  Miners: {file['miner_ids']}")
        return True
    except Exception as e:
        print(f"✗ Failed to parse: {e}")
        return False


async def test_database_operations():
    """Test database operations."""
    print("\nTesting Database Operations")
    print("-" * 60)
    
    from app.db.connection import get_db_pool, init_db_pool, close_db_pool
    
    try:
        # Initialize database
        await init_db_pool()
        pool = get_db_pool()
        
        async with pool.acquire() as conn:
            # Check if tables exist
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('files', 'file_assignments')
            """)
            
            print(f"✓ Found {len(tables)} required tables")
            for table in tables:
                print(f"  - {table['table_name']}")
        
        await close_db_pool()
        return True
        
    except Exception as e:
        print(f"✗ Database error: {e}")
        return False


async def test_message_processing():
    """Test processing a sample message."""
    print("\nTesting Message Processing")
    print("-" * 60)
    
    consumer = UserProfileConsumer()
    
    # Mock IPFS fetch to avoid actual network calls
    async def mock_fetch(cid):
        # Return sample profile data
        return b"""[
            {
                "file_hash": [98, 97, 102, 107, 114, 101, 105, 116, 101, 115, 116, 99, 105, 100, 49, 50, 51],
                "file_name": "mock_file.txt",
                "file_size_in_bytes": 2048,
                "is_assigned": true,
                "last_charged_at": 1234567890,
                "main_req_hash": "0x1234567890abcdef",
                "miner_ids": ["12D3KooWKnhGPbTtCgEPWRxGJhtFFcbMTEerfSKMpVnbpLQzByPx"],
                "total_replicas": 1
            }
        ]"""
    
    # Replace fetch method temporarily
    original_fetch = consumer.fetch_from_ipfs
    consumer.fetch_from_ipfs = mock_fetch
    
    try:
        await consumer.connect()
        
        # Process a test profile
        account = "5GdqPwn1Ley9jmxSk52cHFqWKFc864XfaBMffXo1jUtwNmx3"
        cid = "bafkreitestcid123"
        
        processed = await consumer.process_user_profile(account, cid)
        print(f"✓ Processed {processed} files")
        
        await consumer.close()
        return True
        
    except Exception as e:
        print(f"✗ Processing error: {e}")
        consumer.fetch_from_ipfs = original_fetch
        return False


async def main():
    """Run all tests."""
    print("=" * 60)
    print("USER PROFILE CONSUMER TESTS")
    print("=" * 60)
    
    # Run tests
    ipfs_ok = await test_ipfs_fetch()
    parsing_ok = await test_profile_parsing()
    db_ok = await test_database_operations()
    processing_ok = await test_message_processing()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"IPFS Fetch:    {'✓ OK' if ipfs_ok else '✗ FAILED'}")
    print(f"Parsing:       {'✓ OK' if parsing_ok else '✗ FAILED'}")
    print(f"Database:      {'✓ OK' if db_ok else '✗ FAILED'}")
    print(f"Processing:    {'✓ OK' if processing_ok else '✗ FAILED'}")
    
    if ipfs_ok and parsing_ok and db_ok and processing_ok:
        print("\nAll tests passed! The consumer is ready to run.")
        print("\nTo start the consumer, run:")
        print("  python rabbitmq/user_profile_consumer.py")
    else:
        print("\nSome tests failed. Please check the errors above.")


if __name__ == "__main__":
    asyncio.run(main()) 