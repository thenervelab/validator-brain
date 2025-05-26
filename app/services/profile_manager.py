"""Profile manager service."""

from app.services.ipfs_api import upload_json_to_ipfs
from app.utils.logging import logger


async def prepare_miner_profile_updates(miner_profiles):
    """
    Prepare miner profile updates for blockchain submission.

    Args:
        miner_profiles: Dictionary of miner profiles

    Returns:
        List of miner profile updates for blockchain submission
    """
    try:
        miner_updates = []

        for miner_id, entries in miner_profiles.items():
            if not entries:
                continue

            # Calculate total files and size
            total_files = len(entries)
            total_size = sum(entry.get("file_size_in_bytes", 0) for entry in entries)

            # Encode entries for blockchain
            encoded_entries = []
            for entry in entries:
                file_hash = entry.get("file_hash", "")
                file_hash_encoded = None

                if file_hash:
                    file_hash_encoded = list(bytes.fromhex(file_hash.encode("utf-8").hex()))

                encoded_entry = entry.copy()
                encoded_entry["file_hash"] = file_hash_encoded
                encoded_entries.append(encoded_entry)

            # Upload to IPFS
            result = await upload_json_to_ipfs(data=encoded_entries)
            if result["success"]:
                cid = result["cid"]
                logger.info(f"Uploaded miner profile for {miner_id} to IPFS: {cid}")

                miner_update = {
                    "miner_node_id": miner_id,
                    "cid": cid,
                    "files_count": total_files,
                    "files_size": total_size,
                }
                miner_updates.append(miner_update)
            else:
                logger.error(f"Failed to upload miner profile for {miner_id}: {result['error']}")

        logger.info(
            f"Prepared {len(miner_updates)} miner profile updates for blockchain submission",
        )
        return miner_updates

    except Exception as e:
        logger.error(f"Error preparing miner profile updates: {e}")
        return []
