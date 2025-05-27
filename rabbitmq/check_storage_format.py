"""Check the actual format of UserStorageRequests."""

import os
import sys

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.utils.config import NODE_URL

# Load environment variables
load_dotenv()


def main():
    """Check UserStorageRequests format."""
    print(f"Connecting to {NODE_URL}...")
    
    try:
        substrate = SubstrateInterface(url=NODE_URL)
        print("Connected successfully!\n")
        
        # Get a specific example account to check
        test_account = "5DydLmGijznP1RhH3uaojj8CDjTLdbZXoyZLBNXC9CN68Vsv"
        
        print(f"Checking storage for account: {test_account}")
        print("-" * 60)
        
        # Try to query with specific parameters
        try:
            # First, let's see what RequestsCount returns for this account
            count_result = substrate.query(
                module='IpfsPallet',
                storage_function='RequestsCount',
                params=[test_account]
            )
            print(f"RequestsCount for account: {count_result}")
            
        except Exception as e:
            print(f"Error querying RequestsCount: {e}")
        
        # Check the raw query_map data more carefully
        print("\n\nChecking UserStorageRequests raw data:")
        print("-" * 60)
        
        result = substrate.query_map(
            module='IpfsPallet',
            storage_function='UserStorageRequests'
        )
        
        count = 0
        for key, value in result:
            count += 1
            if count <= 5:
                print(f"\nEntry {count}:")
                
                # Examine the key structure
                if hasattr(key, '__iter__') and len(key) == 2:
                    print(f"  Key[0] (account): {key[0]}")
                    print(f"  Key[1] (hash): {key[1]}")
                    
                    # Try to decode the hash
                    if hasattr(key[1], 'value'):
                        hash_value = str(key[1].value)
                        print(f"  Hash value: {hash_value}")
                        try:
                            # Try to decode as hex
                            decoded = bytes.fromhex(hash_value).decode('utf-8', errors='ignore')
                            print(f"  Hash decoded: {decoded}")
                        except:
                            pass
                
                # Check the value
                print(f"  Value: {value}")
                print(f"  Value type: {type(value)}")
                
                # If value is None, it might mean the entry was deleted
                if value is None or (hasattr(value, 'value') and value.value is None):
                    print("  -> Entry exists but value is None (possibly deleted)")
        
        print(f"\n\nTotal UserStorageRequests entries: {count}")
        
        # Let's also check if there's a different storage item for active requests
        print("\n\nChecking other storage items that might contain request data:")
        print("-" * 60)
        
        # Get all storage items for IpfsPallet
        metadata = substrate.get_metadata()
        for pallet in metadata.pallets:
            if pallet.name == 'IpfsPallet':
                print("\nAll IpfsPallet storage items:")
                for storage in pallet.storage:
                    print(f"  - {storage.name}")
        
        substrate.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 