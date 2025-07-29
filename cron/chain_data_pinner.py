#!/usr/bin/env python3
"""
Standalone cron script to fetch user profiles and storage requests from substrate chain
and pin them to local IPFS node.

This script runs independently every 3 minutes to:
1. Fetch all user profiles from IpfsPallet::UserProfile
2. Fetch all user storage requests from IpfsPallet::UserStorageRequests
3. Decode CIDs and pin them to local IPFS node
4. Skip files already pinned locally

Usage:
    python chain_data_pinner.py
"""

import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any, Set

import base58
import httpx
from substrateinterface import SubstrateInterface

# Configure logging with rotating file handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Create rotating file handler (50MB per file, keep 3 previous files = 200MB total)
file_handler = RotatingFileHandler(
    filename="chain_data_pinner.log",
    maxBytes=50 * 1024 * 1024,
    backupCount=3,  # 50MB  # Keep 3 previous files
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


class ChainDataPinner:
    """Fetches chain data and pins to local IPFS node."""

    def __init__(self):
        self.substrate_url = os.getenv("NODE_URL", "wss://rpc.hippius.network")
        self.ipfs_api_url = os.getenv("IPFS_API_URL", "http://127.0.0.1:5001")
        from app.utils.config import get_ipfs_node_url

        self.ipfs_gateway_url = get_ipfs_node_url()
        self.db_path = os.getenv("PINNING_DB_PATH", "/home/ubuntu/hippius/pinning_status.db")
        self.substrate = None
        self.http_client = None
        self.db_conn = None

    def init_database(self):
        """Initialize SQLite database with pinning status table."""
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pinning_status (
                owner TEXT NOT NULL,
                cid TEXT NOT NULL,
                pinned_status TEXT NOT NULL CHECK (pinned_status IN ('success', 'fail')),
                processed_at TEXT NOT NULL,
                PRIMARY KEY (owner, cid)
            )
        """
        )
        self.db_conn.commit()

    async def connect(self):
        """Initialize connections to substrate, HTTP client, and database."""
        try:
            # Connect to substrate
            self.substrate = SubstrateInterface(url=self.substrate_url, use_remote_preset=True)
            logger.info(f"Connected to substrate at {self.substrate_url}")

            # Initialize HTTP client for IPFS API with higher connection limits for parallel pinning
            limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
            self.http_client = httpx.AsyncClient(timeout=5, limits=limits)
            logger.info(f"HTTP client initialized for IPFS API at {self.ipfs_api_url}")

            # Initialize SQLite database
            self.init_database()
            logger.info(f"SQLite database initialized at {self.db_path}")

        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    async def close(self):
        """Close all connections."""
        if self.substrate:
            self.substrate.close()
        if self.http_client:
            await self.http_client.aclose()
        if self.db_conn:
            self.db_conn.close()

    def should_retry_pin(self, owner: str, cid: str) -> bool:
        """Check if we should retry pinning this CID based on database status."""
        cursor = self.db_conn.execute(
            "SELECT pinned_status, processed_at, attempts FROM pinning_status WHERE owner = ? AND cid = ?",
            (owner, cid),
        )
        result = cursor.fetchone()

        if result is None:
            # Never tried before, should pin
            return True

        status, processed_at_str, attempts = result

        if status == "success":
            # Already successfully pinned, skip
            return False

        # Check attempts limit first - if >= 3 attempts, stop trying
        if attempts >= 3:
            return False

        # Status is 'fail', check if enough time has passed (2 hours)
        processed_at = datetime.fromisoformat(processed_at_str)
        two_hours_ago = datetime.now() - timedelta(hours=2)

        return processed_at < two_hours_ago

    def update_pin_status(self, owner: str, cid: str, status: str):
        """Update the pinning status in database."""
        now = datetime.now().isoformat()

        # Get current attempts count
        cursor = self.db_conn.execute(
            "SELECT attempts FROM pinning_status WHERE owner = ? AND cid = ?",
            (owner, cid),
        )
        result = cursor.fetchone()

        if result is None:
            # First attempt
            attempts = 1 if status == "fail" else 0
        else:
            # Increment attempts only on failure
            attempts = result[0] + 1 if status == "fail" else result[0]

        self.db_conn.execute(
            "INSERT OR REPLACE INTO pinning_status (owner, cid, pinned_status, processed_at, attempts) VALUES (?, ?, ?, ?, ?)",
            (owner, cid, status, now, attempts),
        )
        self.db_conn.commit()

    def fetch_user_profiles(self) -> List[Dict[str, str]]:
        """Fetch user profiles from substrate UserProfile storage."""
        try:
            logger.info("Fetching user profiles from IpfsPallet::UserProfile...")

            # Query the storage map
            result = self.substrate.query_map(module="IpfsPallet", storage_function="UserProfile")

            profiles = []
            for key, value in result:
                # Extract account and CID
                account = str(key.value) if hasattr(key, "value") else str(key)
                cid = str(value.value) if hasattr(value, "value") else str(value)

                if account and cid:
                    profiles.append({"type": "user_profile", "account": account, "cid": cid})

            logger.info(f"Fetched {len(profiles)} user profiles")
            return profiles

        except Exception as e:
            logger.error(f"Error fetching user profiles: {e}")
            return []

    def fetch_user_storage_requests(self) -> List[Dict[str, str]]:
        """Fetch user storage requests from substrate UserStorageRequests storage."""
        try:
            logger.info("Fetching user storage requests from IpfsPallet::UserStorageRequests...")

            # Query the storage double map
            result = self.substrate.query_map(
                module="IpfsPallet", storage_function="UserStorageRequests"
            )

            storage_requests = []
            for key, value in result:
                try:
                    # Handle double map key (owner_account_id, file_hash)
                    if isinstance(key, (tuple, list)) and len(key) >= 2:
                        account = str(key[0].value) if hasattr(key[0], "value") else str(key[0])
                        file_hash_hex = (
                            str(key[1].value) if hasattr(key[1], "value") else str(key[1])
                        )

                        # Convert hex-encoded file_hash to CID string
                        cid = self.hex_to_cid(file_hash_hex) if file_hash_hex else None

                        # Also extract data from the value if needed
                        value_data = value.value if hasattr(value, "value") else value
                        file_name = None
                        if isinstance(value_data, dict):
                            file_name = value_data.get("fileName", "unknown")

                        if account and cid:
                            storage_requests.append(
                                {
                                    "type": "storage_request",
                                    "account": account,
                                    "cid": cid,
                                    "file_hash": file_hash_hex,
                                    "file_name": file_name,
                                }
                            )
                            logger.debug(f"Storage request: {account} -> {cid} ({file_name})")

                except Exception as e:
                    logger.warning(f"Error processing storage request key {key}: {e}")
                    continue

            logger.info(f"Fetched {len(storage_requests)} user storage requests")
            return storage_requests

        except Exception as e:
            logger.error(f"Error fetching user storage requests: {e}")
            return []

    def hex_to_cid(self, hex_string: str) -> str:
        """Convert hex-encoded string to IPFS CID."""
        try:
            # If it's already a valid CID string, return as-is
            if isinstance(hex_string, str) and (
                hex_string.startswith("Qm") or hex_string.startswith("b")
            ):
                return hex_string

            # Convert hex string to bytes then to ASCII string (CID)
            if isinstance(hex_string, str) and len(hex_string) % 2 == 0:
                try:
                    # Decode hex to bytes using binascii for better compatibility
                    import binascii

                    byte_data = binascii.unhexlify(hex_string)
                    # Convert bytes to string (this should give us the CID)
                    cid_string = byte_data.decode("utf-8", errors="ignore")

                    # Validate it looks like a CID
                    if cid_string and (cid_string.startswith("Qm") or cid_string.startswith("b")):
                        logger.debug(f"Successfully converted hex to CID: {cid_string}")
                        return cid_string
                    else:
                        logger.warning(f"Hex decoded to non-CID string: {cid_string}")
                        return cid_string  # Return anyway, might still be valid

                except Exception as decode_error:
                    logger.warning(f"Error decoding hex {hex_string}: {decode_error}")
                    pass

            # Fallback: return original data as string
            return str(hex_string)

        except Exception as e:
            logger.warning(f"Error converting hex to CID: {hex_string}, error: {e}")
            return str(hex_string)

    def bytes_to_cid(self, data: str) -> str:
        """Convert various data formats to IPFS CID string."""
        try:
            # If it's already a valid CID string, return as-is
            if isinstance(data, str) and (data.startswith("Qm") or data.startswith("b")):
                return data

            # If it's hex-encoded bytes, decode first
            if isinstance(data, str) and len(data) % 2 == 0:
                try:
                    byte_data = bytes.fromhex(data)
                    # Try to decode as base58 (CIDv0)
                    return base58.b58encode(byte_data).decode("ascii")
                except:
                    pass

            # Fallback: return original data as string
            return str(data)

        except Exception as e:
            logger.warning(f"Error converting to CID: {data}, error: {e}")
            return str(data)

    async def fetch_profile_content(self, cid: str, timeout: float = 15.0) -> Dict[str, Any]:
        """Fetch user profile JSON content from IPFS gateway with timeout."""
        try:
            url = f"{self.ipfs_gateway_url}/ipfs/{cid}"
            response = await self.http_client.get(url, timeout=timeout)

            if response.status_code == 200:
                profile_data = response.json()
                logger.debug(f"Successfully fetched profile content for CID {cid}")
                return profile_data
            else:
                logger.warning(f"Failed to fetch profile {cid}: HTTP {response.status_code}")
                return {}

        except Exception as e:
            logger.error(f"Error fetching profile content for CID {cid}: {e}")
            return {}

    def extract_file_cids_from_profile(self, profile_data: Dict[str, Any]) -> List[str]:
        """Extract individual file CIDs from user profile JSON."""
        file_cids = []

        try:
            # Profile data is an array of file objects
            if isinstance(profile_data, list):
                for file_entry in profile_data:
                    if isinstance(file_entry, dict):
                        # Extract file_hash which contains the CID as byte array
                        file_hash = file_entry.get("file_hash", [])

                        if isinstance(file_hash, list) and file_hash:
                            # Convert byte array to CID string
                            try:
                                # Convert list of integers to bytes then to string
                                cid_bytes = bytes(file_hash)
                                cid_string = cid_bytes.decode("utf-8", errors="ignore")

                                # Validate it looks like a CID
                                if cid_string and (
                                    cid_string.startswith("Qm") or cid_string.startswith("b")
                                ):
                                    file_cids.append(cid_string)
                                    logger.debug(f"Extracted file CID: {cid_string}")
                            except Exception as e:
                                logger.warning(
                                    f"Error converting file_hash to CID: {file_hash}, error: {e}"
                                )

                        elif isinstance(file_hash, str) and file_hash:
                            # Already a string CID
                            file_cids.append(file_hash)
                            logger.debug(f"Extracted file CID: {file_hash}")

            logger.info(f"Extracted {len(file_cids)} file CIDs from profile")
            return file_cids

        except Exception as e:
            logger.error(f"Error extracting file CIDs from profile: {e}")
            return []

    async def get_pinned_cids(self) -> Set[str]:
        """Get list of already pinned CIDs from local IPFS node."""
        try:
            response = await self.http_client.post(f"{self.ipfs_api_url}/api/v0/pin/ls")
            if response.status_code == 200:
                result = response.json()
                pinned_cids = set()

                # Handle different response formats
                if isinstance(result, dict):
                    if "Keys" in result:
                        pinned_cids = set(result["Keys"].keys())
                    else:
                        pinned_cids = set(result.keys())

                logger.info(f"Found {len(pinned_cids)} already pinned CIDs")
                return pinned_cids
            else:
                logger.warning(f"Failed to get pinned CIDs: {response.status_code}")
                return set()

        except Exception as e:
            logger.warning(f"Error getting pinned CIDs: {e}")
            return set()

    async def pin_cid(self, cid: str, timeout: float = 5) -> bool:
        """Pin a single CID to local IPFS node with timeout. Streams from gateway if needed."""
        try:
            # Database already checked in should_retry_pin(), so proceed directly to pinning

            # Try direct pin first (attempt to pin from IPFS network)
            try:
                response = await self.http_client.post(
                    f"{self.ipfs_api_url}/api/v0/pin/add",
                    params={"arg": cid},
                    timeout=timeout,
                )
                if response.status_code == 200:
                    response_data = response.json()
                    pinned_cid = response_data.get("Pins", [])
                    logger.info(
                        f"Successfully pinned CID from network: {cid} - Response: {response.text}"
                    )
                    return True
                else:
                    logger.debug(
                        f"Direct pin failed for {cid}: {response.status_code} - {response.text}"
                    )
            except Exception as e:
                logger.debug(f"Exception during direct pin for {cid}: {e}")

            # Direct pin failed, try streaming from gateways as fallback
            gateways = [
                self.ipfs_gateway_url,
            ]

            for i, gateway_base_url in enumerate(gateways, 1):
                logger.info(
                    f"Direct pin failed, trying gateway {i}/{len(gateways)} ({gateway_base_url}) for CID: {cid}"
                )

                try:
                    gateway_url = f"{gateway_base_url}/ipfs/{cid}"

                    # Stream from gateway and add to IPFS simultaneously (reduced timeout)
                    async with self.http_client.stream(
                        "GET", gateway_url, timeout=timeout
                    ) as gateway_stream:
                        if gateway_stream.status_code != 200:
                            logger.warning(
                                f"Gateway {i} failed for CID {cid}: {gateway_stream.status_code}"
                            )
                            continue  # Try next gateway

                        # Prepare multipart form data for IPFS add API
                        boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"

                        async def stream_generator():
                            # Start multipart
                            yield f"--{boundary}\r\n".encode()
                            yield f'Content-Disposition: form-data; name="file"; filename="{cid}"\r\n'.encode()
                            yield f"Content-Type: application/octet-stream\r\n\r\n".encode()

                            # Stream content
                            async for chunk in gateway_stream.aiter_bytes(chunk_size=64 * 1024):
                                yield chunk

                            # End multipart
                            yield f"\r\n--{boundary}--\r\n".encode()

                        headers = {"Content-Type": f"multipart/form-data; boundary={boundary}"}

                        # Upload to IPFS with pinning (reduced timeout)
                        add_response = await self.http_client.post(
                            f"{self.ipfs_api_url}/api/v0/add?pin=true&cid-version=1",
                            headers=headers,
                            content=stream_generator(),
                            timeout=timeout,
                        )

                        if add_response.status_code == 200:
                            result = add_response.json()
                            added_cid = result.get("Hash")
                            if added_cid == cid:
                                logger.info(
                                    f"Successfully streamed and pinned CID from gateway {i}: {cid} - Response: {add_response.text}"
                                )
                                return True
                            else:
                                logger.warning(
                                    f"CID mismatch from gateway {i}: expected {cid}, got {added_cid}"
                                )
                                continue  # Try next gateway
                        else:
                            logger.warning(
                                f"Gateway {i} add failed for CID {cid}: {add_response.status_code} - {add_response.text}"
                            )
                            continue  # Try next gateway

                except Exception as e:
                    logger.warning(f"Gateway {i} exception for CID {cid}: {e}")
                    continue  # Try next gateway

            # All gateways failed
            logger.error(f"All pin attempts failed for CID {cid}")
            return False

        except Exception as e:
            logger.error(f"Error pinning CID {cid}: {e}")
            return False

    async def pin_cid_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        item: Dict[str, str],
        progress_counter: dict,
        total_count: int,
    ) -> Dict[str, Any]:
        """Pin a single CID with semaphore concurrency control."""
        async with semaphore:
            cid = item["cid"]
            account = item["account"]
            item_type = item["type"]

            # Check if we should retry this pin based on database
            if not self.should_retry_pin(account, cid):
                logger.debug(
                    f"Skipping CID {cid} for {account} - already processed or too recent failure"
                )

                # Update progress counter and log
                progress_counter["count"] += 1
                remaining = total_count - progress_counter["count"]
                remaining_pct = (remaining / total_count) * 100
                logger.debug(
                    f"Skipped {item_type} CID {cid} for {account} - Remaining: {remaining} ({remaining_pct:.1f}%)"
                )

                return {
                    "cid": cid,
                    "account": account,
                    "type": item_type,
                    "success": False,
                    "skipped": True,
                }

            success = await self.pin_cid(cid, timeout=10)

            # Update database with result
            status = "success" if success else "fail"
            self.update_pin_status(account, cid, status)

            # Update progress counter and log
            progress_counter["count"] += 1
            remaining = total_count - progress_counter["count"]
            remaining_pct = (remaining / total_count) * 100

            if success:
                if item_type == "profile_file":
                    profile_cid = item.get("profile_cid", "unknown")
                    logger.info(
                        f"Pinned {item_type} CID {cid} from profile {profile_cid} for {account} - Remaining: {remaining} ({remaining_pct:.1f}%)"
                    )
                elif item_type == "storage_request":
                    file_name = item.get("file_name", "unknown")
                    logger.info(
                        f"Pinned {item_type} CID {cid} ({file_name}) for {account} - Remaining: {remaining} ({remaining_pct:.1f}%)"
                    )
                else:
                    logger.info(
                        f"Pinned {item_type} CID {cid} for {account} - Remaining: {remaining} ({remaining_pct:.1f}%)"
                    )
            else:
                if item_type == "profile_file":
                    profile_cid = item.get("profile_cid", "unknown")
                    logger.warning(
                        f"Failed to pin {item_type} CID {cid} from profile {profile_cid} for {account} - Remaining: {remaining} ({remaining_pct:.1f}%)"
                    )
                elif item_type == "storage_request":
                    file_name = item.get("file_name", "unknown")
                    logger.warning(
                        f"Failed to pin {item_type} CID {cid} ({file_name}) for {account} - Remaining: {remaining} ({remaining_pct:.1f}%)"
                    )
                else:
                    logger.warning(
                        f"Failed to pin {item_type} CID {cid} for {account} - Remaining: {remaining} ({remaining_pct:.1f}%)"
                    )

            result = {
                "cid": cid,
                "account": account,
                "type": item_type,
                "success": success,
                "skipped": False,
            }

            # Add extra info for profile files
            if item_type == "profile_file" and "profile_cid" in item:
                result["profile_cid"] = item["profile_cid"]

            if item_type == "storage_request" and "file_name" in item:
                result["file_name"] = item["file_name"]

            return result

    async def process_chain_data(self):
        """Main processing function - fetch data and pin to IPFS."""
        start_time = time.time()

        try:
            # Fetch all data from chain
            user_profiles = self.fetch_user_profiles()
            storage_requests = self.fetch_user_storage_requests()

            logger.info(
                f"Found {len(user_profiles)} user profiles and {len(storage_requests)} storage requests from chain"
            )

            # Get already pinned CIDs to avoid redundant work
            pinned_cids = await self.get_pinned_cids()

            # Process user profiles and extract file CIDs
            all_cids_to_pin = []

            # First, add profile CIDs themselves
            for profile in user_profiles:
                profile_cid = profile["cid"]
                if profile_cid not in pinned_cids:
                    all_cids_to_pin.append(
                        {
                            "cid": profile_cid,
                            "type": "user_profile",
                            "account": profile["account"],
                        }
                    )

            # Then fetch profile contents and extract individual file CIDs in parallel
            logger.info("Fetching user profile contents to extract file CIDs...")
            profile_semaphore = asyncio.Semaphore(50)  # Limit to 10 concurrent profile fetches

            async def process_profile_with_semaphore(profile):
                """Process a single user profile with semaphore control."""
                async with profile_semaphore:
                    try:
                        profile_cid = profile["cid"]
                        account = profile["account"]

                        # Fetch profile JSON content with timeout
                        profile_data = await self.fetch_profile_content(profile_cid, timeout=15.0)

                        if profile_data:
                            # Extract file CIDs from profile
                            file_cids = self.extract_file_cids_from_profile(profile_data)

                            # Return file CIDs for this profile
                            profile_files = []
                            for file_cid in file_cids:
                                if file_cid not in pinned_cids:
                                    profile_files.append(
                                        {
                                            "cid": file_cid,
                                            "type": "profile_file",
                                            "account": account,
                                            "profile_cid": profile_cid,
                                        }
                                    )

                            logger.info(
                                f"Profile {profile_cid} for {account}: extracted {len(file_cids)} file CIDs"
                            )
                            return profile_files
                        else:
                            logger.warning(
                                f"No data fetched for profile {profile_cid} for {account}"
                            )
                            return []

                    except Exception as e:
                        logger.warning(
                            f"Error processing profile {profile['cid']} for {profile['account']}: {e}"
                        )
                        return []

            # Process all profiles in parallel
            if user_profiles:
                profile_tasks = [
                    process_profile_with_semaphore(profile) for profile in user_profiles
                ]
                profile_results = await asyncio.gather(*profile_tasks, return_exceptions=True)

                # Flatten results and add to pin list
                for result in profile_results:
                    if isinstance(result, Exception):
                        logger.error(f"Profile processing task failed with exception: {result}")
                        continue
                    elif isinstance(result, list):
                        all_cids_to_pin.extend(result)

                total_profile_files = sum(
                    len(result) for result in profile_results if isinstance(result, list)
                )
                logger.info(
                    f"Extracted {total_profile_files} file CIDs from {len(user_profiles)} user profiles"
                )

            # Add storage request CIDs
            for request in storage_requests:
                request_cid = request["cid"]
                if request_cid not in pinned_cids:
                    all_cids_to_pin.append(
                        {
                            "cid": request_cid,
                            "type": "storage_request",
                            "account": request["account"],
                        }
                    )

            total_cids_to_pin = len(all_cids_to_pin)

            if total_cids_to_pin == 0:
                logger.info("No new CIDs found to pin")
                return

            logger.info(f"Found {total_cids_to_pin} total new CIDs to pin")

            # Pin all CIDs in parallel with concurrency limit
            logger.info("Starting parallel pinning with 15 concurrent operations...")
            semaphore = asyncio.Semaphore(15)  # Limit to 15 concurrent pins
            progress_counter = {"count": 0}  # Shared counter for progress tracking

            # Create tasks for parallel pinning
            pin_tasks = [
                self.pin_cid_with_semaphore(semaphore, item, progress_counter, total_cids_to_pin)
                for item in all_cids_to_pin
            ]

            # Execute all pinning operations in parallel
            pin_results = await asyncio.gather(*pin_tasks, return_exceptions=True)

            # Process results for summary counts only (progress logging happens in real-time)
            successful_pins = 0
            failed_pins = 0
            skipped_pins = 0

            for result in pin_results:
                if isinstance(result, Exception):
                    logger.error(f"Pin task failed with exception: {result}")
                    failed_pins += 1
                    continue

                success = result["success"]
                skipped = result.get("skipped", False)

                if skipped:
                    skipped_pins += 1
                elif success:
                    successful_pins += 1
                else:
                    failed_pins += 1

            # Summary
            elapsed_time = time.time() - start_time
            logger.info(f"Processing completed in {elapsed_time:.2f}s:")
            logger.info(f"  User profiles from chain: {len(user_profiles)}")
            logger.info(f"  Storage requests from chain: {len(storage_requests)}")
            logger.info(f"  Total CIDs to pin: {total_cids_to_pin}")
            logger.info(f"  Successfully pinned: {successful_pins}")
            logger.info(f"  Failed to pin: {failed_pins}")
            logger.info(f"  Skipped (already processed): {skipped_pins}")
            if successful_pins > 0:
                logger.info(f"  Average pins per second: {successful_pins / elapsed_time:.1f}")

        except Exception as e:
            logger.error(f"Error in process_chain_data: {e}")
            raise

    async def run(self):
        """Run the complete chain data pinning process."""
        try:
            await self.connect()
            await self.process_chain_data()
        finally:
            await self.close()


async def main():
    """Main entry point for the cron script."""
    logger.info("Starting chain data pinner cron job")

    try:
        pinner = ChainDataPinner()
        await pinner.run()
        logger.info("Chain data pinner completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Chain data pinner failed: {e}")
        return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))
