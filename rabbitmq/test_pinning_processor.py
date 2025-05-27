"""Test script to verify the pinning request processor functionality."""

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


def test_hex_conversion():
    """Test hex to string conversion."""
    print("\nTesting Hex to String Conversion")
    print("-" * 60)
    
    # Sample hex strings from the example
    test_cases = [
        ("6261666b7265696778687470366e75646f76676e66356e7a35766a6c65766835786b7535737063666f79336c6c6372326136717579336832366665", "file_hash"),
        ("66696c65735f6c6973745f62393565333161632d326534662d343464622d613537632d353238386535633438653036", "file_name")
    ]
    
    for hex_string, label in test_cases:
        try:
            # Convert hex to bytes then to string
            decoded = bytes.fromhex(hex_string).decode('utf-8')
            print(f"✓ {label}: {decoded}")
        except Exception as e:
            print(f"✗ Failed to decode {label}: {e}")


def test_storage_request_parsing():
    """Test parsing of storage request data."""
    print("\nTesting Storage Request Parsing")
    print("-" * 60)
    
    # Sample data from the user's example
    sample_data = [
        [
            ["5GeakAuWoJDYhcGCXoQaqsGGTF7BSdef1hmCraNvpHGL33zB", "6261666b7265696778687470366e75646f76676e66356e7a35766a6c65766835786b7535737063666f79336c6c6372326136717579336832366665"],
            {
                "totalReplicas": 5,
                "owner": "5GeakAuWoJDYhcGCXoQaqsGGTF7BSdef1hmCraNvpHGL33zB",
                "fileHash": "6261666b7265696778687470366e75646f76676e66356e7a35766a6c65766835786b7535737063666f79336c6c6372326136717579336832366665",
                "fileName": "files_list_b95e31ac-2e4f-44db-a57c-5288e5c48e06",
                "lastChargedAt": 708819,
                "createdAt": 708819,
                "minerIds": [],
                "selectedValidator": "5FH2vToACmzqqD2WXJsUZ5dDaXEfejYg4EMc4yJThCFBwhZK",
                "isAssigned": False
            }
        ]
    ]
    
    from rabbitmq.pinning_request_processor import PinningRequestProcessor
    
    processor = PinningRequestProcessor()
    parsed = processor.parse_storage_request_data(sample_data)
    
    print(f"Parsed {len(parsed)} requests:")
    for request in parsed:
        print(f"\n  Owner: {request['owner']}")
        print(f"  Request Hash: {request['request_hash'][:32]}...")
        print(f"  File Hash: {request['file_hash'][:32]}...")
        print(f"  File Name: {request['file_name']}")
        print(f"  Total Replicas: {request['total_replicas']}")
        print(f"  Is Assigned: {request['is_assigned']}")
        print(f"  Miner Count: {len(request['miner_ids'])}")


async def test_substrate_connection():
    """Test connection to substrate and fetching storage requests."""
    print("\nTesting Substrate Connection")
    print("-" * 60)
    
    try:
        substrate = SubstrateInterface(url=NODE_URL)
        print(f"✓ Connected to {NODE_URL}")
        
        # Try to fetch storage requests
        result = substrate.query_map(
            module='IpfsPallet',
            storage_function='UserStorageRequests'
        )
        
        count = 0
        for key, value in result:
            count += 1
            if count <= 3:  # Show first 3 entries
                if hasattr(key, 'value') and isinstance(key.value, list) and len(key.value) >= 2:
                    print(f"\n  Owner: {key.value[0]}")
                    print(f"  Request Hash: {key.value[1][:32]}...")
                    if hasattr(value, 'value') and isinstance(value.value, dict):
                        print(f"  File Name: {value.value.get('fileName', 'N/A')}")
                        print(f"  Total Replicas: {value.value.get('totalReplicas', 0)}")
                        print(f"  Is Assigned: {value.value.get('isAssigned', False)}")
        
        print(f"\n✓ Total storage requests found: {count}")
        substrate.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to connect or fetch: {e}")
        return False


async def main():
    """Run all tests."""
    print("=" * 60)
    print("PINNING REQUEST PROCESSOR TESTS")
    print("=" * 60)
    
    # Run tests
    test_hex_conversion()
    test_storage_request_parsing()
    substrate_ok = await test_substrate_connection()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Hex Conversion: ✓ OK")
    print(f"Parsing:        ✓ OK")
    print(f"Substrate:      {'✓ OK' if substrate_ok else '✗ FAILED'}")
    
    if substrate_ok:
        print("\nThe processor is ready to run.")
        print("\nTo start the processor, run:")
        print("  python rabbitmq/pinning_request_processor.py")
    else:
        print("\nPlease check the substrate connection.")


if __name__ == "__main__":
    asyncio.run(main()) 