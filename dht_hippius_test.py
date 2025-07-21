#!/usr/bin/env python3
import json

import httpx

# Configuration
IPFS_URL = "https://dht.hippius.com"
ROOT_CID = "bafybeifsu6sz4zstpevzg6so5t2oswrmtnqilhvc2fkd2hkeqsr7pzgezu"
MINER_NODES = [
    "12D3KooWH83Hrc3sRFk6UFF6MXxMLSdmNtHdjFpLXbak9df4FnyY",
    "12D3KooWPNby2pYYT97xTwii9ME9qefTrXZ6NWTmHhTmFm9p2732",
    "12D3KooWH4WdjC9FbeVRMVTc231eAsewRoGPEPmRCZpnxA97wbCj",
]


def get_all_refs(root_cid):
    """Get all blocks (refs) from the root CID"""
    print(f"Getting all refs for CID: {root_cid}")

    refs_url = f"{IPFS_URL}/api/v0/refs"
    refs_params = {"arg": root_cid, "recursive": "true", "unique": "true"}

    try:
        with httpx.Client() as client:
            response = client.post(refs_url, params=refs_params)
            response.raise_for_status()

            refs = []
            for line in response.text.strip().split("\n"):
                if line.strip():
                    ref_data = json.loads(line)
                    if "Ref" in ref_data:
                        refs.append(ref_data["Ref"])

            print(f"Found {len(refs)} blocks")
            return refs

    except Exception as e:
        print(f"Error getting refs: {e}")
        return []


def check_provider(cid, miner_id):
    """Check if a miner node is hosting a specific block"""
    dht_url = f"{IPFS_URL}/api/v0/routing/findprovs"
    dht_params = {"arg": cid}

    try:
        with httpx.Client() as client:
            response = client.post(dht_url, params=dht_params)
            response.raise_for_status()

            for line in response.text.strip().split("\n"):
                if line.strip():
                    try:
                        data = json.loads(line)
                        # Type 4 indicates a provider
                        if data.get("Type") == 4 and data.get("ID") == miner_id:
                            return True
                        # Also check Responses list for nested provider messages
                        if data.get("Type") == 4 and data.get("Responses"):
                            for resp in data.get("Responses", []):
                                if resp.get("ID") == miner_id:
                                    return True
                    except json.JSONDecodeError:
                        continue

    except Exception as e:
        print(f"Error checking provider for {cid}: {e}")

    return False


def main():
    print("IPFS DHT Test Script")
    print("=" * 50)

    # Get all refs
    refs = get_all_refs(ROOT_CID)
    if not refs:
        print("No refs found, exiting.")
        return

    # Test each miner for each block
    results = {}
    for miner_id in MINER_NODES:
        results[miner_id] = {"hosting": [], "not_hosting": []}
        print(f"\nChecking miner: {miner_id}")

        for i, cid in enumerate(refs[:5]):  # Limit to first 5 for testing
            print(f"  Checking block {i+1}/5: {cid[:20]}...")

            if check_provider(cid, miner_id):
                results[miner_id]["hosting"].append(cid)
                print(f"    ✓ HOSTING")
            else:
                results[miner_id]["not_hosting"].append(cid)
                print(f"    ✗ NOT HOSTING")

    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    for miner_id in MINER_NODES:
        hosting_count = len(results[miner_id]["hosting"])
        total_checked = hosting_count + len(results[miner_id]["not_hosting"])
        print(f"\nMiner: {miner_id}")
        print(f"  Hosting: {hosting_count}/{total_checked} blocks")

        if results[miner_id]["hosting"]:
            print("  Hosted blocks:")
            for cid in results[miner_id]["hosting"]:
                print(f"    - {cid}")


if __name__ == "__main__":
    main()
