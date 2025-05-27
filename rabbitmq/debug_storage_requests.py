"""Debug script to investigate UserStorageRequests query."""

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
    """Debug UserStorageRequests query."""
    print(f"Connecting to {NODE_URL}...")
    
    try:
        substrate = SubstrateInterface(url=NODE_URL)
        print("Connected successfully!\n")
        
        # First, let's check the exact storage function details
        print("Checking IpfsPallet storage functions:")
        print("-" * 60)
        
        metadata = substrate.get_metadata()
        for pallet in metadata.pallets:
            if pallet.name == 'IpfsPallet':
                for storage in pallet.storage:
                    if 'Storage' in storage.name or 'Request' in storage.name:
                        print(f"Storage: {storage.name}")
                        print(f"  Type: {storage.type}")
                        print(f"  Modifier: {storage.modifier}")
                        if hasattr(storage, 'documentation'):
                            print(f"  Docs: {storage.documentation}")
                        print()
        
        # Try different query methods
        print("\nTrying query_map for UserStorageRequests:")
        print("-" * 60)
        try:
            result = substrate.query_map(
                module='IpfsPallet',
                storage_function='UserStorageRequests'
            )
            
            count = 0
            for key, value in result:
                count += 1
                print(f"\nEntry {count}:")
                print(f"  Key type: {type(key)}")
                print(f"  Key: {key}")
                if hasattr(key, 'value'):
                    print(f"  Key value: {key.value}")
                print(f"  Value type: {type(value)}")
                print(f"  Value: {value}")
                if hasattr(value, 'value'):
                    print(f"  Value value: {value.value}")
                
                if count >= 3:
                    break
            
            print(f"\nTotal entries found: {count}")
            
        except Exception as e:
            print(f"Error with query_map: {e}")
        
        # Try a direct query
        print("\n\nTrying direct query:")
        print("-" * 60)
        try:
            # Try to get the storage function info
            storage_function = substrate.get_metadata_storage_function(
                "IpfsPallet", 
                "UserStorageRequests"
            )
            print(f"Storage function info: {storage_function}")
            
        except Exception as e:
            print(f"Error getting storage function: {e}")
        
        # Try to check if there's data using a different approach
        print("\n\nChecking all IpfsPallet storage items:")
        print("-" * 60)
        
        # Query each storage item to see which ones have data
        storage_items = [
            'RequestsCount',
            'UserTotalFilesSize',
            'MinerTotalFilesSize',
            'UserStorageRequests',
            'UserUnpinRequests'
        ]
        
        for item in storage_items:
            try:
                result = substrate.query(
                    module='IpfsPallet',
                    storage_function=item
                )
                print(f"{item}: {result}")
            except Exception as e:
                print(f"{item}: Error - {e}")
        
        substrate.close()
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main() 