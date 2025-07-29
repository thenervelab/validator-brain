#!/usr/bin/env python3
"""
Module to analyze coldkey deregistrations between Bittensor and local registration data.

This module provides a function to identify coldkeys that are registered locally
but have been deregistered from Bittensor, along with their associated node IDs.
"""

from typing import Dict, List, Set

from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException


def _query_storage_map(substrate: SubstrateInterface, module: str, storage_function: str) -> Dict:
    """Query all entries in a storage map."""
    try:
        result = substrate.query_map(module=module, storage_function=storage_function)
        return {entry[0].value: entry[1].value for entry in result}
    except SubstrateRequestException:
        return {}


def _query_storage_double_map(
    substrate: SubstrateInterface, module: str, storage_function: str, netuid: int
) -> Dict:
    """Query all entries in a storage double map for a specific netuid."""
    try:
        result = substrate.query_map(
            module=module, storage_function=storage_function, params=[netuid]
        )
        return {entry[0].value: entry[1].value for entry in result}
    except SubstrateRequestException:
        return {}


def _fetch_bittensor_coldkeys(
    bittensor_substrate: SubstrateInterface, netuid: int = 75
) -> Set[str]:
    """Fetch all registered coldkeys (hotkeys) from Bittensor."""
    uids_data = _query_storage_double_map(bittensor_substrate, "SubtensorModule", "Uids", netuid)
    return set(uids_data.keys())


def _fetch_primary_nodes(registration_substrate: SubstrateInterface) -> Dict[str, str]:
    """Fetch primary nodes from Registration.ColdkeyNodeRegistration.

    Returns:
        Dict mapping node_id -> owner (coldkey)
    """
    coldkey_reg_data = _query_storage_map(
        registration_substrate, "Registration", "ColdkeyNodeRegistration"
    )

    primary_nodes = {}
    for node_id, node_info in coldkey_reg_data.items():
        if node_info and isinstance(node_info, dict) and "owner" in node_info:
            primary_nodes[node_id] = node_info["owner"]

    return primary_nodes


def _fetch_linked_nodes(
    registration_substrate: SubstrateInterface,
) -> Dict[str, List[str]]:
    """Fetch linked nodes from Registration.LinkedNodes.

    Returns:
        Dict mapping primary_node_id -> [linked_node1, linked_node2, ...]
    """
    try:
        result = registration_substrate.query_map(
            module="Registration", storage_function="LinkedNodes"
        )
        linked_nodes = {}

        for entry in result:
            if isinstance(entry, (list, tuple)) and len(entry) == 2:
                key, value = entry
                primary_key = key.value if hasattr(key, "value") else key
                linked_array = value.value if hasattr(value, "value") else value
                primary_str = str(primary_key)

                if isinstance(linked_array, list):
                    linked_nodes[primary_str] = [str(node) for node in linked_array]
                else:
                    linked_nodes[primary_str] = [str(linked_array)] if linked_array else []

        return linked_nodes
    except Exception:
        return {}


def get_deregistered_coldkeys(
    bittensor_substrate: SubstrateInterface,
    registration_substrate: SubstrateInterface,
    netuid: int = 75,
) -> Dict[str, List[str]]:
    """Get coldkeys that are deregistered from Bittensor with their associated node IDs.

    Args:
        bittensor_substrate: Connected SubstrateInterface to Bittensor network
        registration_substrate: Connected SubstrateInterface to registration network
        netuid: Network UID to query on Bittensor (default: 75)

    Returns:
        Dict mapping coldkey -> [primary_node, linked_node1, linked_node2, ...]
        Only includes coldkeys that are in local registration but NOT on Bittensor
    """
    # Fetch data from both networks
    bittensor_coldkeys = _fetch_bittensor_coldkeys(bittensor_substrate, netuid)
    primary_nodes = _fetch_primary_nodes(registration_substrate)
    linked_nodes = _fetch_linked_nodes(registration_substrate)

    # Find coldkeys in registration but not on Bittensor (deregistered)
    registration_owners = set(primary_nodes.values())
    deregistered_coldkeys = registration_owners - bittensor_coldkeys

    # Build reverse mapping: owner -> primary_node_id
    owner_to_primary = {owner: node_id for node_id, owner in primary_nodes.items()}

    # Build result mapping for deregistered coldkeys only
    result = {}
    for coldkey in deregistered_coldkeys:
        if coldkey in owner_to_primary:
            primary_node = owner_to_primary[coldkey]
            nodes = [primary_node]

            # Add linked nodes if they exist
            if primary_node in linked_nodes:
                nodes.extend(linked_nodes[primary_node])

            result[coldkey] = nodes

    return result
