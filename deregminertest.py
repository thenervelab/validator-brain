import asyncio

from rabbitmq.network_self_healing_processor import (
    query_storage_double_map,
    connect_to_node,
)


async def find_deregistered_miners():
    """Find miners that are no longer registered on Bittensor."""
    substrate = connect_to_node("wss://entrypoint-finney.opentensor.ai:443")
    current_uids = query_storage_double_map(substrate, "SubtensorModule", "Uids", 75)

    return current_uids


if __name__ == "__main__":
    result = asyncio.run(find_deregistered_miners())

    for item in result:
        print(item)
