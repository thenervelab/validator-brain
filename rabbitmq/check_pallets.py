"""Check available pallets on the substrate chain."""

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
    """Check available pallets."""
    print(f"Connecting to {NODE_URL}...")
    
    try:
        substrate = SubstrateInterface(url=NODE_URL)
        print("Connected successfully!\n")
        
        # Get metadata
        metadata = substrate.get_metadata()
        
        print("Available Pallets:")
        print("-" * 60)
        
        # List all pallets
        for pallet in metadata.pallets:
            print(f"- {pallet.name}")
            
            # Check if it's IPFS related
            if 'ipfs' in pallet.name.lower() or 'storage' in pallet.name.lower():
                print(f"  Storage functions:")
                for storage in pallet.storage:
                    print(f"    - {storage.name}")
        
        substrate.close()
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main() 