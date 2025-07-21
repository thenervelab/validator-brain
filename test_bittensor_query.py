#!/usr/bin/env python3
"""
Simple test script to query Bittensor substrate and print the results.
"""

import sys
sys.path.append('.')

from rabbitmq.network_self_healing_processor import connect_to_node, query_storage_double_map

def test_bittensor_query():
    """Query Bittensor substrate and print results."""
    print("🔗 Connecting to Bittensor network...")
    
    try:
        substrate = connect_to_node("wss://entrypoint-finney.opentensor.ai:443")
        print("✅ Connected successfully!")
        
        print("\n📋 Querying Uids storage for netuid 75...")
        current_uids = query_storage_double_map(substrate, "SubtensorModule", "Uids", 75)
        
        print(f"✅ Found {len(current_uids)} registered miners")
        print("\n📊 Sample entries:")
        
        # Show first 10 entries
        for i, (hotkey, uid) in enumerate(list(current_uids.items())[:10]):
            print(f"   {i+1}. {hotkey} -> UID {uid}")
        
        if len(current_uids) > 10:
            print(f"   ... and {len(current_uids) - 10} more")
            
        print(f"\n🎯 Total registered miners on Bittensor netuid 75: {len(current_uids)}")
        
        substrate.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bittensor_query()