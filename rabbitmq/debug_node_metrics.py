"""Debug script to examine node metrics data structure."""

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
    """Debug node metrics data structure."""
    print(f"Connecting to {NODE_URL}...")
    
    try:
        substrate = SubstrateInterface(url=NODE_URL)
        print("Connected successfully!\n")
        
        # Query node metrics
        result = substrate.query_map(
            module='ExecutionUnit',
            storage_function='NodeMetrics'
        )
        
        print("Examining first 5 node metrics entries:")
        print("-" * 80)
        
        count = 0
        for key, value in result:
            count += 1
            if count > 5:
                break
                
            print(f"\nEntry {count}:")
            print(f"Key type: {type(key)}")
            print(f"Key: {key}")
            
            if hasattr(key, '__iter__'):
                print(f"Key length: {len(key)}")
                for i, k in enumerate(key):
                    print(f"  Key[{i}]: {k} (type: {type(k)})")
                    if hasattr(k, 'value'):
                        print(f"    Key[{i}].value: {k.value}")
            
            print(f"\nValue type: {type(value)}")
            if value is None:
                print("Value is None")
                continue
                
            if hasattr(value, 'value'):
                print(f"Value.value type: {type(value.value)}")
                actual_value = value.value
            else:
                actual_value = value
            
            if isinstance(actual_value, dict):
                print("\nValue fields:")
                for field_name, field_value in list(actual_value.items())[:10]:
                    print(f"  {field_name}: {field_value}")
                    if field_name in ['minerId', 'miner_id']:
                        print(f"    -> This is the miner ID we want!")
        
        print(f"\n\nTotal entries found: {count}")
        
        substrate.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 