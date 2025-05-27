"""Check the active storage request details."""

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
    """Check active storage request."""
    print(f"Connecting to {NODE_URL}...")
    
    try:
        substrate = SubstrateInterface(url=NODE_URL)
        print("Connected successfully!\n")
        
        # Query UserStorageRequests
        result = substrate.query_map(
            module='IpfsPallet',
            storage_function='UserStorageRequests'
        )
        
        print("Active UserStorageRequests:")
        print("-" * 60)
        
        for key, value in result:
            if value is not None and (not hasattr(value, 'value') or value.value is not None):
                print(f"\nActive entry found!")
                print(f"Key: {key}")
                print(f"Key type: {type(key)}")
                
                if hasattr(key, '__iter__') and len(key) >= 2:
                    account = str(key[0])
                    request_hash = str(key[1])
                    
                    print(f"Account: {account}")
                    print(f"Request hash: {request_hash}")
                    
                    # Decode the request hash
                    try:
                        if hasattr(key[1], 'value'):
                            hash_hex = str(key[1].value)
                        else:
                            hash_hex = str(request_hash)
                        decoded = bytes.fromhex(hash_hex).decode('utf-8')
                        print(f"Request hash decoded: {decoded}")
                    except:
                        pass
                
                print(f"\nValue: {value}")
                print(f"Value type: {type(value)}")
                
                if hasattr(value, 'value'):
                    print(f"Value.value: {value.value}")
                    print(f"Value.value type: {type(value.value)}")
                    
                    # If it's a dict, show the structure
                    if isinstance(value.value, dict):
                        print("\nValue structure:")
                        for k, v in value.value.items():
                            print(f"  {k}: {v}")
        
        # Let's also check the metadata for the exact type
        print("\n\nChecking UserStorageRequests metadata:")
        print("-" * 60)
        
        storage_function = substrate.get_metadata_storage_function(
            "IpfsPallet", 
            "UserStorageRequests"
        )
        print(f"Storage type info: {storage_function}")
        
        # Check if the value type 691 has a definition
        print("\n\nLooking for type definition 691:")
        metadata = substrate.get_metadata()
        
        # The type registry might have the definition
        if hasattr(substrate, 'type_registry'):
            try:
                type_def = substrate.type_registry.get_type_definition(691)
                print(f"Type 691 definition: {type_def}")
            except:
                pass
        
        substrate.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 