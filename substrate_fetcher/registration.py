#!/usr/bin/env python3
"""
Module to analyze coldkey deregistrations between Bittensor and local registration data.

This module provides a function to identify coldkeys that are registered locally
but have been deregistered from Bittensor, along with their associated node IDs.
"""

import logging
import os

from pydantic import BaseModel
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

from app.db.connection import get_db_pool

logger = logging.getLogger(__name__)


async def grace(cold_keys):
    """
    Grace period check for deregistered cold_keys.
    Increments unsuccessful_registration_checks counter and removes nodes
    that have been checked less than 10 times from the processing list.
    """
    if not cold_keys:
        return

    db_pool = get_db_pool()
    coldkeys_to_remove = []

    async with db_pool.acquire() as conn:
        for cold_key in cold_keys:
            # Insert or update the deregistered node record
            result = await conn.fetchrow(
                """
                INSERT INTO deregistered_node_ids (node_id, unsuccessful_registration_checks, updated_at)
                VALUES ($1, 1, CURRENT_TIMESTAMP)
                ON CONFLICT (node_id) 
                DO UPDATE SET 
                    unsuccessful_registration_checks = deregistered_node_ids.unsuccessful_registration_checks + 1,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING unsuccessful_registration_checks
                """,
                cold_key,
            )

            check_count = result["unsuccessful_registration_checks"]

            # If less than 5 checks, remove from processing list (grace period)
            if check_count < 5:
                coldkeys_to_remove.append(cold_key)
                logger.info(f"🕐 Gracing deregistered {cold_key=} (check #{check_count}/10)")

    if coldkeys_to_remove:
        logger.info(f"🕐 Graced {len(coldkeys_to_remove)} nodes, {len(cold_keys)} remaining for processing")

    # Remove graced nodes from the processing list
    return list(set(cold_keys) - set(coldkeys_to_remove))


def _query_storage_map(substrate: SubstrateInterface, module: str, storage_function: str) -> dict:
    """Query all entries in a storage map."""
    try:
        result = substrate.query_map(module=module, storage_function=storage_function)
        return {entry[0].value: entry[1].value for entry in result}
    except SubstrateRequestException:
        return {}


def _query_storage_double_map(substrate: SubstrateInterface, module: str, storage_function: str, netuid: int) -> dict:
    """Query all entries in a storage double map for a specific netuid."""
    try:
        result = substrate.query_map(module=module, storage_function=storage_function, params=[netuid])
        return {entry[0].value: entry[1].value for entry in result}
    except SubstrateRequestException:
        return {}


def _fetch_bittensor_coldkeys(bittensor_substrate: SubstrateInterface, netuid: int = 75) -> set[str]:
    """Fetch all registered coldkeys (hotkeys) from Bittensor."""
    uids_data = _query_storage_double_map(bittensor_substrate, "SubtensorModule", "Uids", netuid)
    return set(uids_data.keys())


def _fetch_primary_nodes(registration_substrate: SubstrateInterface) -> dict:
    """Fetch primary nodes from Registration.ColdkeyNodeRegistration.

    Returns:
        Dict mapping node_id -> owner (coldkey)
    """
    coldkey_reg_data = _query_storage_map(
        registration_substrate,
        "Registration",
        "ColdkeyNodeRegistration",
    )

    primary_nodes = {}
    for node_id, node_info in coldkey_reg_data.items():
        if node_info and isinstance(node_info, dict) and "owner" in node_info:
            logger.info(f"Primary node {node_id=} {node_info=}")

            primary_nodes[node_id] = {
                "owner": node_info["owner"],
                "ipfs_peer_id": node_info["ipfs_node_id"],
            }

    return primary_nodes


def _fetch_secondary_nodes(registration_substrate: SubstrateInterface) -> dict:
    """Fetch secondary nodes from Registration.NodeRegistration.

    Returns:
        Dict mapping node_id -> owner (coldkey)
    """
    hotkey_reg_data = _query_storage_map(
        registration_substrate,
        "Registration",
        "NodeRegistration",
    )

    primary_nodes = {}
    for node_id, node_info in hotkey_reg_data.items():
        if node_info and isinstance(node_info, dict) and "owner" in node_info:
            logger.info(f"Secondary node {node_id=} {node_info=}")

            primary_nodes[node_id] = {
                "owner": node_info["owner"],
                "ipfs_peer_id": node_info["ipfs_node_id"],
            }

    return primary_nodes


def _fetch_node_relationships(
    registration_substrate: SubstrateInterface,
) -> dict[str, list[str]]:
    """Fetch linked nodes from Registration.LinkedNodes.

    Returns:
        Dict mapping primary_node_id -> [linked_node1, linked_node2, ...]
    """
    try:
        result = registration_substrate.query_map(
            module="Registration",
            storage_function="LinkedNodes",
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


class ColdKey(BaseModel):
    id: str


class Node(BaseModel):
    id: str
    ipfs_peer_id: str
    owner: ColdKey
    hierarchy: str  # primary / secondary


class DeregistrationReport(BaseModel):
    coldkeys: list[ColdKey]
    primary_nodes: list[Node]
    linked_nodes: list[Node]


async def compute_deregistration_report(
    netuid: int = 75,
) -> DeregistrationReport:
    """Get coldkeys that are deregistered from Bittensor with their associated node IDs.

    Args:
        netuid: Network UID to query on Bittensor (default: 75)

    Returns:
        Dict mapping coldkey -> [primary_node, linked_node1, linked_node2, ...]
        Only includes coldkeys that are in local registration but NOT on Bittensor
    """
    bittensor_substrate = SubstrateInterface(
        url="wss://entrypoint-finney.opentensor.ai:443",
        ss58_format=42,
    )

    # Connect to registration network (from NODE_URL env var)
    registration_url = os.getenv("NODE_URL", "wss://rpc.hippius.network")
    registration_substrate = SubstrateInterface(
        url=registration_url,
        use_remote_preset=True,
    )

    # Fetch data from both networks
    bittensor_coldkeys = _fetch_bittensor_coldkeys(bittensor_substrate, netuid)
    primary_nodes_pallet = _fetch_primary_nodes(registration_substrate)
    secondary_nodes_pallet = _fetch_secondary_nodes(registration_substrate)
    links_pallet = _fetch_node_relationships(registration_substrate)

    hippius_to_deregister_coldkeys = set()
    hippius_registered_coldkeys = [item["owner"] for item in primary_nodes_pallet.values()]
    for key in hippius_registered_coldkeys:
        if key not in bittensor_coldkeys:
            hippius_to_deregister_coldkeys.add(key)

    # grace the keys for a period
    hippius_to_deregister_coldkeys = await grace(hippius_to_deregister_coldkeys)

    primary_nodes = {}
    secondary_nodes = []
    for node_id, details in primary_nodes_pallet:
        if node_id["owner"] in hippius_to_deregister_coldkeys:  # prepare to deregister
            logger.info(f"Found primary {node_id=} ({details['owner']}) to deregister...")
            primary_nodes[node_id] = Node(
                id=node_id,
                owner=details["owner"],
                ipfs_peer_id=details["ipfs_peer_id"],
                hierarchy="main",
            )

    for node_id, details in secondary_nodes_pallet.items():
        for parent_node_id, linked_secondary_nodes in links_pallet.items():
            if node_id in linked_secondary_nodes:
                logger.info(f"Found primary {node_id=} ({details['owner']}) {parent_node_id=} to deregister...")
                secondary_nodes.append(
                    Node(
                        id=node_id,
                        owner=details["owner"],
                        ipfs_peer_id=details["ipfs_peer_id"],
                        hierarchy="linked",
                    )
                )
                break
        else:
            logger.warning(f"No primary node detected for secondary (linked) {node_id=} {details}")

    logger.info("Computed deregistration report:")
    logger.info(f"{len(hippius_to_deregister_coldkeys)=}")
    logger.info(f"{len(primary_nodes)=}")
    logger.info(f"{len(secondary_nodes)=}")

    return DeregistrationReport(
        coldkeys=hippius_to_deregister_coldkeys,
        primary_nodes=primary_nodes.values(),
        linked_nodes=secondary_nodes,
    )
