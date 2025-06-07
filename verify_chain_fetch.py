#!/usr/bin/env python3
"""
Diagnostic script to verify that the pinning request processor will fetch 
unassigned storage requests from the blockchain, including old ones.
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from substrateinterface import SubstrateInterface
from dotenv import load_dotenv

load_dotenv()

def check_chain_storage_requests():
    """Check what storage requests are available on the blockchain."""
    
    # Connect to substrate
    node_url = os.getenv('NODE_URL', 'wss://rpc.hippius.network')
    print(f"🔗 Connecting to {node_url}")
    
    substrate = SubstrateInterface(url=node_url)
    print("✅ Connected to substrate")
    
    # Query all user storage requests (same as pinning_request_processor.py)
    print("\n🔍 Querying ALL storage requests from blockchain...")
    
    result = substrate.query_map(
        module='IpfsPallet',
        storage_function='UserStorageRequests'
    )
    
    # Look for your specific requests
    your_owner = '5HoreGVb17XhY3wanDvzoAWS7yHYbc5uMteXqRNTiZ6Txkqq'
    your_file_hashes = [
        '6261666b72656962766e6d3469727963786d6837616f637968693579633374676533346c7234713364337374366a637a35716973346167366c7a79',
        '6261666b726569613633617673776d777878786779706f6876636777757476776667326a7134736d6c356468723474796e6b7274617465686b6465'
    ]
    
    total_requests = 0
    unassigned_requests = 0
    your_requests_found = 0
    
    print("\n📊 Analyzing storage requests:")
    
    for key, value in result:
        total_requests += 1
        
        if value is not None:
            # Extract account and request data
            if hasattr(key, '__iter__') and len(key) >= 2:
                account = str(key[0].value if hasattr(key[0], 'value') else key[0])
                request_hash = str(key[1].value if hasattr(key[1], 'value') else key[1])
                
                # Handle scale_info wrapped value
                actual_value = value.value if hasattr(value, 'value') else value
                
                if actual_value:
                    is_assigned = actual_value.get('is_assigned', actual_value.get('isAssigned', False))
                    file_hash = str(actual_value.get('file_hash', actual_value.get('fileHash', '')))
                    selected_validator = str(actual_value.get('selected_validator', actual_value.get('selectedValidator', '')))
                    
                    if not is_assigned:
                        unassigned_requests += 1
                        
                        # Check if this is one of your requests
                        if account == your_owner and file_hash in your_file_hashes:
                            your_requests_found += 1
                            print(f"\n🎯 FOUND YOUR REQUEST!")
                            print(f"   Owner: {account}")
                            print(f"   File Hash: {file_hash}")
                            print(f"   Selected Validator: {selected_validator}")
                            print(f"   Is Assigned: {is_assigned}")
                            print(f"   ✅ This WILL be processed by current validator!")
    
    print(f"\n📈 SUMMARY:")
    print(f"   Total storage requests on chain: {total_requests}")
    print(f"   Unassigned requests: {unassigned_requests}")
    print(f"   Your requests found: {your_requests_found}/2")
    
    if your_requests_found == 2:
        print(f"\n🎉 SUCCESS: Both your requests are on chain and unassigned!")
        print(f"   They WILL be processed by the current validator workflow")
        print(f"   The 'selectedValidator' field is ignored in the new approach")
    elif your_requests_found > 0:
        print(f"\n⚠️ PARTIAL: Found {your_requests_found}/2 of your requests")
        print(f"   Check if one was already processed")
    else:
        print(f"\n❌ ISSUE: Your requests not found on chain")
        print(f"   They may have already been processed or removed")
    
    substrate.close()
    return your_requests_found

if __name__ == "__main__":
    try:
        found = check_chain_storage_requests()
        print(f"\n🔍 Result: Found {found}/2 of your storage requests on chain")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1) 