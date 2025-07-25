#!/usr/bin/env python3
"""
Analyze coldkey registrations between Bittensor and local registration data.

This script:
1. Fetches all registered coldkeys from Bittensor SubtensorModule.Uids (netuid=75)
2. Fetches primary nodes from Registration.ColdkeyNodeRegistration
3. Fetches linked nodes from Registration.LinkedNodes
4. Creates a mapping: coldkey -> [primary_node, linked_node1, linked_node2, ...]

Usage:
    python analyze_deregistrations.py
"""

import json
import logging
import os
import sys
from typing import Dict, List, Set

# Add the project root to Python path to import existing modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

try:
    from substrateinterface import SubstrateInterface
    from substrateinterface.exceptions import SubstrateRequestException
except ImportError:
    print("substrateinterface not available. Install with: pip install substrate-interface")
    sys.exit(1)

# Configuration
BITTENSOR_WS_URL = "wss://entrypoint-finney.opentensor.ai:443"  # Bittensor for Uids
REGISTRATION_WS_URL = "wss://hippius-testnet.starkleytech.com"  # Hippius for registration data
NETUID = 75

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def connect_to_node(ws_url: str) -> SubstrateInterface:
    """Establish connection to a Substrate node."""
    try:
        logger.info(f"Connecting to {ws_url}...")
        substrate = SubstrateInterface(
            url=ws_url,
            ss58_format=42,
        )
        logger.info(f"Connected to chain: {substrate.chain}")
        logger.info(f"Runtime version: {substrate.runtime_version}")
        return substrate
    except Exception as e:
        logger.error(f"Connection failed to {ws_url}: {str(e)}")
        raise


def query_storage_map(substrate: SubstrateInterface, module: str, storage_function: str) -> Dict:
    """Query all entries in a storage map."""
    try:
        logger.info(f"Querying {module}.{storage_function}...")
        result = substrate.query_map(module=module, storage_function=storage_function)
        data = {entry[0].value: entry[1].value for entry in result}
        logger.info(f"Found {len(data)} entries in {module}.{storage_function}")
        return data
    except SubstrateRequestException as e:
        logger.error(f"Failed to query {module}.{storage_function}: {str(e)}")
        return {}


def query_storage_double_map(substrate: SubstrateInterface, module: str, storage_function: str, netuid: int) -> Dict:
    """Query all entries in a storage double map for a specific netuid."""
    try:
        logger.info(f"Querying {module}.{storage_function} for netuid={netuid}...")
        result = substrate.query_map(module=module, storage_function=storage_function, params=[netuid])
        # Format as { hotkey: uid }
        data = {entry[0].value: entry[1].value for entry in result}
        logger.info(f"Found {len(data)} entries in {module}.{storage_function} for netuid={netuid}")
        return data
    except SubstrateRequestException as e:
        logger.error(f"Failed to query {module}.{storage_function} for netuid={netuid}: {str(e)}")
        return {}


def fetch_bittensor_coldkeys(substrate: SubstrateInterface) -> Set[str]:
    """Fetch all registered coldkeys (hotkeys) from Bittensor."""
    logger.info("=== Fetching Bittensor Coldkeys ===")
    uids_data = query_storage_double_map(substrate, "SubtensorModule", "Uids", NETUID)

    # The uids_data is { hotkey: uid }, we want the hotkeys (coldkeys)
    coldkeys = set(uids_data.keys())
    logger.info(f"Found {len(coldkeys)} registered coldkeys on Bittensor")

    # Log some examples
    for i, coldkey in enumerate(list(coldkeys)[:3]):
        logger.info(f"  Example {i+1}: {coldkey}")
    if len(coldkeys) > 3:
        logger.info(f"  ... and {len(coldkeys) - 3} more coldkeys")

    return coldkeys


def fetch_primary_nodes(substrate: SubstrateInterface) -> Dict[str, str]:
    """Fetch primary nodes from Registration.ColdkeyNodeRegistration.

    Returns:
        Dict mapping node_id -> owner (coldkey)
    """
    logger.info("=== Fetching Primary Nodes ===")
    coldkey_reg_data = query_storage_map(substrate, "Registration", "ColdkeyNodeRegistration")

    primary_nodes = {}
    for node_id, node_info in coldkey_reg_data.items():
        if not node_info or not isinstance(node_info, dict) or "owner" not in node_info:
            logger.warning(f"Skipping node {node_id}: Invalid or missing NodeInfo")
            continue

        owner = node_info["owner"]
        primary_nodes[node_id] = owner
        logger.debug(f"Primary node: {node_id} -> owner: {owner}")

    logger.info(f"Found {len(primary_nodes)} primary nodes")

    # Log some examples
    for i, (node_id, owner) in enumerate(list(primary_nodes.items())[:3]):
        logger.info(f"  Example {i+1}: {node_id} -> {owner}")
    if len(primary_nodes) > 3:
        logger.info(f"  ... and {len(primary_nodes) - 3} more primary nodes")

    return primary_nodes


def fetch_linked_nodes(substrate: SubstrateInterface) -> Dict[str, List[str]]:
    """Fetch linked nodes from Registration.LinkedNodes.
    
    CRITICAL ISSUE: The substrate-interface library incorrectly aggregates LinkedNodes data.
    
    Based on polkadot.js raw data, the correct structure should be:
    [
        [[primary_node1], [linked_node1, linked_node2, ...]],
        [[primary_node2], [linked_node3]],
        ...
    ]
    
    But substrate-interface returns one massive aggregation:
    primary_node1 -> [all_286_linked_nodes_mixed_together]
    
    This function documents the aggregation bug and warns about data limitations.

    Returns:
        Dict mapping primary_node_id -> [linked_node1, linked_node2, ...]
    """
    logger.info("=== Fetching Linked Nodes (SUBSTRATE-INTERFACE AGGREGATION BUG) ===")

    try:
        logger.info("Fetching LinkedNodes raw data...")
        result = substrate.query_map(module="Registration", storage_function="LinkedNodes")
        result_list = list(result)

        logger.info(f"Raw LinkedNodes result entries: {len(result_list)}")
        
        # Expected: ~200+ individual entries like [[primary], [linked...]]
        # Actual: 1 aggregated entry with all data mixed together
        if len(result_list) == 1:
            logger.warning("*** SUBSTRATE-INTERFACE AGGREGATION BUG DETECTED ***")
            logger.warning("Expected: Multiple separate [[primary], [linked...]] entries")
            logger.warning("Actual: One massive aggregated entry with incorrect relationships")
            logger.warning("This means we cannot determine correct primary->linked mappings!")
            
            single_entry = result_list[0]
            if isinstance(single_entry, (list, tuple)) and len(single_entry) == 2:
                key, value = single_entry
                
                primary_key = key.value if hasattr(key, "value") else key
                linked_array = value.value if hasattr(value, "value") else value
                
                logger.warning(f"Aggregated primary node: {primary_key}")
                if isinstance(linked_array, list):
                    logger.warning(f"Aggregated linked nodes count: {len(linked_array)}")
                    logger.warning("These linked nodes should be distributed across ~200+ primary nodes!")
                    logger.warning("But we cannot determine the correct distribution.")
                    
                    # Return the aggregated data with clear warning
                    logger.info("Returning aggregated data with WARNING: relationships are incorrect")
                    return {str(primary_key): [str(node) for node in linked_array]}
                
            return {}
        
        # This would be the correct case (multiple entries)
        logger.info("Processing multiple LinkedNodes entries (would be correct structure)")
        linked_nodes = {}
        
        for i, entry in enumerate(result_list):
            try:
                if isinstance(entry, (list, tuple)) and len(entry) == 2:
                    key, value = entry
                    
                    primary_key = key.value if hasattr(key, "value") else key
                    linked_array = value.value if hasattr(value, "value") else value
                    
                    primary_str = str(primary_key)
                    
                    if isinstance(linked_array, list):
                        linked_nodes[primary_str] = [str(node) for node in linked_array]
                        logger.debug(f"Entry {i+1}: {primary_str} -> {len(linked_array)} linked nodes")
                    else:
                        linked_nodes[primary_str] = [str(linked_array)] if linked_array else []
                        logger.debug(f"Entry {i+1}: {primary_str} -> 1 linked node")
                        
            except Exception as e:
                logger.warning(f"Error processing LinkedNodes entry {i+1}: {e}")
                continue
        
        # Log statistics
        total_linked = sum(len(links) for links in linked_nodes.values())
        nodes_with_links = sum(1 for links in linked_nodes.values() if len(links) > 0)
        
        logger.info(f"LinkedNodes Statistics:")
        logger.info(f"  Total primary nodes: {len(linked_nodes)}")
        logger.info(f"  Primary nodes with linked nodes: {nodes_with_links}")
        logger.info(f"  Primary nodes without linked nodes: {len(linked_nodes) - nodes_with_links}")
        logger.info(f"  Total linked nodes: {total_linked}")
        
        # Log examples
        if nodes_with_links > 0:
            logger.info("=== Examples of nodes WITH linked nodes ===")
            count = 0
            for primary_id, links in linked_nodes.items():
                if links:
                    logger.info(f"  {primary_id} -> {len(links)} linked: {links[:3]}{'...' if len(links) > 3 else ''}")
                    count += 1
                    if count >= 3:
                        break
        
        return linked_nodes
        
    except Exception as e:
        logger.error(f"Error fetching LinkedNodes: {e}")
        return {}


def build_coldkey_node_mapping(
    bittensor_coldkeys: Set[str], primary_nodes: Dict[str, str], linked_nodes: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    """Build mapping of coldkey -> [primary_node, linked_node1, linked_node2, ...].

    Args:
        bittensor_coldkeys: Set of coldkeys registered on Bittensor
        primary_nodes: Dict mapping node_id -> owner (coldkey)
        linked_nodes: Dict mapping primary_node_id -> [linked_nodes]

    Returns:
        Dict mapping coldkey -> [primary_node, linked_node1, linked_node2, ...]
    """
    logger.info("=== Building Coldkey -> Node Mapping ===")

    # Reverse the primary_nodes mapping: owner -> node_id
    owner_to_primary = {}
    for node_id, owner in primary_nodes.items():
        if owner in owner_to_primary:
            logger.warning(f"Owner {owner} has multiple primary nodes: {owner_to_primary[owner]} and {node_id}")
        owner_to_primary[owner] = node_id

    coldkey_mapping = {}

    # Only process coldkeys that are registered on Bittensor
    for coldkey in bittensor_coldkeys:
        if coldkey not in owner_to_primary:
            # This coldkey is on Bittensor but has no primary node in our registration
            logger.debug(f"Coldkey {coldkey} is on Bittensor but has no primary node")
            coldkey_mapping[coldkey] = []
            continue

        primary_node = owner_to_primary[coldkey]
        nodes = [primary_node]

        # Add linked nodes if they exist
        if primary_node in linked_nodes:
            nodes.extend(linked_nodes[primary_node])

        coldkey_mapping[coldkey] = nodes
        logger.debug(f"Coldkey {coldkey} -> {len(nodes)} nodes: {nodes}")

    logger.info(f"Built mapping for {len(coldkey_mapping)} coldkeys")

    # Statistics
    total_nodes = sum(len(nodes) for nodes in coldkey_mapping.values())
    empty_mappings = sum(1 for nodes in coldkey_mapping.values() if not nodes)

    logger.info(f"Statistics:")
    logger.info(f"  Total coldkeys mapped: {len(coldkey_mapping)}")
    logger.info(f"  Total nodes across all coldkeys: {total_nodes}")
    logger.info(f"  Coldkeys with no nodes: {empty_mappings}")
    logger.info(f"  Average nodes per coldkey: {total_nodes / len(coldkey_mapping) if coldkey_mapping else 0:.2f}")

    return coldkey_mapping


def analyze_discrepancies(
    bittensor_coldkeys: Set[str], primary_nodes: Dict[str, str], coldkey_mapping: Dict[str, List[str]]
):
    """Analyze discrepancies between Bittensor and registration data."""
    logger.info("=== Analyzing Discrepancies ===")

    # Coldkeys in registration but not on Bittensor
    registration_owners = set(primary_nodes.values())
    missing_from_bittensor = registration_owners - bittensor_coldkeys

    # Coldkeys on Bittensor but not in registration
    missing_from_registration = bittensor_coldkeys - registration_owners

    logger.info(f"Discrepancy Analysis:")
    logger.info(f"  Coldkeys in registration but NOT on Bittensor: {len(missing_from_bittensor)}")
    logger.info(f"  Coldkeys on Bittensor but NOT in registration: {len(missing_from_registration)}")

    if missing_from_bittensor:
        logger.info("  Coldkeys in registration but missing from Bittensor (candidates for deregistration):")
        for i, coldkey in enumerate(list(missing_from_bittensor)[:5]):
            logger.info(f"    {i+1}: {coldkey}")
        if len(missing_from_bittensor) > 5:
            logger.info(f"    ... and {len(missing_from_bittensor) - 5} more")

    if missing_from_registration:
        logger.info("  Coldkeys on Bittensor but missing from registration (new registrations?):")
        for i, coldkey in enumerate(list(missing_from_registration)[:5]):
            logger.info(f"    {i+1}: {coldkey}")
        if len(missing_from_registration) > 5:
            logger.info(f"    ... and {len(missing_from_registration) - 5} more")

    return {
        "missing_from_bittensor": list(missing_from_bittensor),
        "missing_from_registration": list(missing_from_registration),
    }


def save_results(
    coldkey_mapping: Dict[str, List[str]], discrepancies: Dict, output_file: str = "deregistration_analysis.json"
):
    """Save results to JSON file."""
    logger.info(f"=== Saving Results to {output_file} ===")

    results = {
        "timestamp": "",
        "summary": {
            "total_coldkeys_bittensor": len([k for k in coldkey_mapping.keys()]),
            "total_coldkeys_with_nodes": len([k for k, v in coldkey_mapping.items() if v]),
            "total_coldkeys_without_nodes": len([k for k, v in coldkey_mapping.items() if not v]),
            "total_nodes": sum(len(nodes) for nodes in coldkey_mapping.values()),
            "missing_from_bittensor": len(discrepancies["missing_from_bittensor"]),
            "missing_from_registration": len(discrepancies["missing_from_registration"]),
        },
        "coldkey_node_mapping": coldkey_mapping,
        "discrepancies": discrepancies,
    }

    # Add timestamp
    from datetime import datetime

    results["timestamp"] = datetime.now().isoformat()

    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Results saved to {output_file}")
    logger.info(f"Summary: {results['summary']}")


def print_top_coldkeys_by_nodes(coldkey_mapping: Dict[str, List[str]], top_n: int = 10):
    """Print the coldkeys with the most linked node IDs."""
    logger.info("=== Top Coldkeys by Node Count ===")
    
    # Sort coldkeys by number of nodes (descending)
    sorted_coldkeys = sorted(
        coldkey_mapping.items(), 
        key=lambda x: len(x[1]), 
        reverse=True
    )
    
    # Filter out coldkeys with no nodes for cleaner output
    coldkeys_with_nodes = [(coldkey, nodes) for coldkey, nodes in sorted_coldkeys if len(nodes) > 0]
    
    if not coldkeys_with_nodes:
        logger.info("No coldkeys found with any nodes")
        return
    
    logger.info(f"Showing top {min(top_n, len(coldkeys_with_nodes))} coldkeys with most nodes:")
    logger.info("")
    
    for i, (coldkey, nodes) in enumerate(coldkeys_with_nodes[:top_n]):
        primary_node = nodes[0] if nodes else "None"
        linked_nodes = nodes[1:] if len(nodes) > 1 else []
        
        logger.info(f"#{i+1}. Coldkey: {coldkey}")
        logger.info(f"     Total nodes: {len(nodes)}")
        logger.info(f"     Primary node: {primary_node}")
        if linked_nodes:
            logger.info(f"     Linked nodes ({len(linked_nodes)}): {linked_nodes[:5]}{'...' if len(linked_nodes) > 5 else ''}")
        else:
            logger.info(f"     Linked nodes: None")
        logger.info("")
    
    # Summary statistics
    max_nodes = len(coldkeys_with_nodes[0][1]) if coldkeys_with_nodes else 0
    total_with_nodes = len(coldkeys_with_nodes)
    total_coldkeys = len(coldkey_mapping)
    
    logger.info(f"Summary:")
    logger.info(f"  Maximum nodes per coldkey: {max_nodes}")
    logger.info(f"  Coldkeys with nodes: {total_with_nodes}")
    logger.info(f"  Coldkeys without nodes: {total_coldkeys - total_with_nodes}")
    logger.info(f"  Total coldkeys: {total_coldkeys}")


def main():
    """Main function to analyze deregistrations."""
    logger.info("Starting deregistration analysis...")

    try:
        # Connect to both networks
        logger.info("=== Connecting to Networks ===")
        bittensor_substrate = connect_to_node(BITTENSOR_WS_URL)
        registration_substrate = connect_to_node(REGISTRATION_WS_URL)

        # Fetch data from Bittensor
        bittensor_coldkeys = fetch_bittensor_coldkeys(bittensor_substrate)

        # Fetch data from registration network
        primary_nodes = fetch_primary_nodes(registration_substrate)
        linked_nodes = fetch_linked_nodes(registration_substrate)

        # Build the mapping
        coldkey_mapping = build_coldkey_node_mapping(bittensor_coldkeys, primary_nodes, linked_nodes)

        # Analyze discrepancies
        discrepancies = analyze_discrepancies(bittensor_coldkeys, primary_nodes, coldkey_mapping)

        # Save results
        save_results(coldkey_mapping, discrepancies)

        # Print coldkeys with most linked nodes
        print_top_coldkeys_by_nodes(coldkey_mapping)

        logger.info("=== Analysis Complete ===")

    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise
    finally:
        logger.info("Script completed")


if __name__ == "__main__":
    main()
