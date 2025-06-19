import logging
from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException

# Configuration
REGISTRATION_WS_URL = "wss://hippius-testnet.starkleytech.com"  # For ColdkeyNodeRegistration
UIDS_WS_URL = "http://127.0.0.1:9945"  # Local Bittensor Node for Uids
OUTPUT_FILE = "node_registration_data.json"
NETUID = 75
MNEMONIC = "your_seed_phrase_here"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def connect_to_node(ws_url):
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


def query_storage_map(substrate, module, storage_function):
    """Query all entries in a storage map."""
    try:
        result = substrate.query_map(
            module=module,
            storage_function=storage_function
        )
        return {entry[0].value: entry[1].value for entry in result}
    except SubstrateRequestException as e:
        logger.error(f"Failed to query {module}.{storage_function}: {str(e)}")
        return {}


def query_storage_double_map(substrate, module, storage_function, netuid):
    """Query all entries in a storage double map for a specific netuid."""
    try:
        result = substrate.query_map(
            module=module,
            storage_function=storage_function,
            params=[netuid]
        )
        # Format as { hotkey: uid }
        return {entry[0].value: entry[1].value for entry in result}
    except SubstrateRequestException as e:
        logger.error(f"Failed to query {module}.{storage_function} for netuid={netuid}: {str(e)}")
        return {}


def find_unregistered_node_owners(coldkey_reg_data, uids_data):
    """Find node owners in ColdkeyNodeRegistration not present as hotkeys in Uids."""
    unregistered_owners = []
    for node_id, node_info in coldkey_reg_data.items():
        if not node_info or not isinstance(node_info, dict) or 'owner' not in node_info:
            logger.warning(f"Skipping node {node_id}: Invalid or missing NodeInfo")
            continue
        account_id = node_info['owner']
        if account_id not in uids_data:
            unregistered_owners.append({
                'node_id': node_id,
                'owner': account_id
            })
    return unregistered_owners


def submit_deregistration_report(substrate, keypair, node_ids):
    """Submit deregistration report transaction."""
    try:
        # Convert node_ids to Vec<Vec<u8>> format (node_id is already a string, encode to bytes)
        formatted_node_ids = [node_id.encode() for node_id in node_ids]

        # Compose the call
        call = substrate.compose_call(
            call_module='Registration',
            call_function='submit_deregistration_report',
            call_params={
                'node_ids': formatted_node_ids
            }
        )

        # Create extrinsic
        extrinsic = substrate.create_signed_extrinsic(
            call=call,
            keypair=keypair
        )

        # Submit and watch
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True
        )

        if receipt.is_success:
            logger.info(f"Transaction successful: Hash {receipt.extrinsic_hash}")
        else:
            logger.error(f"Transaction failed: {receipt.error_message}")

        return receipt
    except Exception as e:
        logger.error(f"Failed to submit deregistration report: {str(e)}")
        return None


def main():
    try:
        # Connect to nodes
        registration_substrate = connect_to_node(REGISTRATION_WS_URL)
        uids_substrate = connect_to_node(UIDS_WS_URL)

        # Query ColdkeyNodeRegistration entries
        logger.info("Querying all ColdkeyNodeRegistration entries...")
        coldkey_reg_data = query_storage_map(registration_substrate, "Registration",
                                             "ColdkeyNodeRegistration")

        # Query Uids entries for netuid=75
        logger.info(f"Querying Uids entries for netuid={NETUID}...")
        uids_data = query_storage_double_map(uids_substrate, "SubtensorModule", "Uids", NETUID)

        # Find unregistered node owners
        logger.info("Checking for node owners not registered as hotkeys in Uids...")
        unregistered_owners = find_unregistered_node_owners(coldkey_reg_data, uids_data)

        # Log summary
        logger.info(f"ColdkeyNodeRegistration entries: {len(coldkey_reg_data)}")
        logger.info(f"Uids entries for netuid={NETUID}: {len(uids_data)}")
        logger.info(f"Unregistered node owners: {len(unregistered_owners)}")

        # Print entries for verification
        logger.info("Unregistered node owners:")
        for owner in unregistered_owners:
            logger.info(f"  Node ID: {owner['node_id']}, Account ID: {owner['owner']}")

        # Submit deregistration report if there are unregistered nodes
        if unregistered_owners:
            logger.info("Preparing to submit deregistration report...")
            # Create keypair from mnemonic
            keypair = Keypair.create_from_mnemonic(MNEMONIC, ss58_format=42)
            logger.info(f"Using account: {keypair.ss58_address}")

            # Extract node IDs
            node_ids = [owner['node_id'] for owner in unregistered_owners]

            # Submit transaction
            receipt = submit_deregistration_report(registration_substrate, keypair, node_ids)
            if receipt and receipt.is_success:
                logger.info("Deregistration report submitted successfully")
            else:
                logger.error("Failed to submit deregistration report")

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        logger.info("Script completed")


if __name__ == "__main__":
    logger.info("Starting node registration query script")
    main()