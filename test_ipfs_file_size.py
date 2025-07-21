#!/usr/bin/env python3
"""
Test script to verify IPFS file size fetching works correctly.
Tests both object/stat and files/stat endpoints with real CIDs.
"""

import asyncio
import json
import sys
import subprocess
import httpx

# Test CIDs
TEST_CIDS = [
    "QmXrZQdLBKhqabxSik5HJ1vwMDvZGt6F7hD9NFBvAbxarV",
    "bafybeifl2vpxs5kxbla44zhxsndkni4t2vatszlkklwyz2eqnig7lenzr4"
]

# IPFS node URL
IPFS_NODE_URL = "http://127.0.0.1:5001"

async def test_object_stat(cid: str) -> dict:
    """Test /api/v0/object/stat endpoint"""
    print(f"\n=== Testing object/stat for {cid} ===")
    
    stat_url = f"{IPFS_NODE_URL}/api/v0/object/stat"
    params = {"arg": cid}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(stat_url, params=params, timeout=10.0)
            response.raise_for_status()
            stats = response.json()
            
            print(f"✅ Response: {json.dumps(stats, indent=2)}")
            
            size = stats.get("CumulativeSize")
            if size is not None:
                print(f"✅ CumulativeSize: {size:,} bytes")
                return {"success": True, "size": size, "method": "object/stat"}
            else:
                print("❌ No CumulativeSize found")
                return {"success": False, "error": "No CumulativeSize", "method": "object/stat"}
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return {"success": False, "error": str(e), "method": "object/stat"}

async def test_files_stat(cid: str) -> dict:
    """Test /api/v0/files/stat endpoint"""
    print(f"\n=== Testing files/stat for {cid} ===")
    
    stat_url = f"{IPFS_NODE_URL}/api/v0/files/stat"
    params = {"arg": f"/ipfs/{cid}"}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(stat_url, params=params, timeout=10.0)
            response.raise_for_status()
            stats = response.json()
            
            print(f"✅ Response: {json.dumps(stats, indent=2)}")
            
            size = stats.get("CumulativeSize") or stats.get("Size")
            if size is not None:
                print(f"✅ Size: {size:,} bytes")
                return {"success": True, "size": size, "method": "files/stat"}
            else:
                print("❌ No Size found")
                return {"success": False, "error": "No Size", "method": "files/stat"}
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return {"success": False, "error": str(e), "method": "files/stat"}

async def test_block_stat(cid: str) -> dict:
    """Test /api/v0/block/stat endpoint"""
    print(f"\n=== Testing block/stat for {cid} ===")
    
    stat_url = f"{IPFS_NODE_URL}/api/v0/block/stat"
    params = {"arg": cid}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(stat_url, params=params, timeout=10.0)
            response.raise_for_status()
            stats = response.json()
            
            print(f"✅ Response: {json.dumps(stats, indent=2)}")
            
            size = stats.get("Size")
            if size is not None:
                print(f"✅ Size: {size:,} bytes")
                return {"success": True, "size": size, "method": "block/stat"}
            else:
                print("❌ No Size found")
                return {"success": False, "error": "No Size", "method": "block/stat"}
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return {"success": False, "error": str(e), "method": "block/stat"}

def test_curl_object_stat(cid: str) -> dict:
    """Test using curl command for object/stat"""
    print(f"\n=== Testing curl object/stat for {cid} ===")
    
    cmd = [
        "curl", "-X", "POST", 
        f"{IPFS_NODE_URL}/api/v0/object/stat?arg={cid}",
        "-s"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            stats = json.loads(result.stdout)
            print(f"✅ Response: {json.dumps(stats, indent=2)}")
            
            size = stats.get("CumulativeSize")
            if size is not None:
                print(f"✅ CumulativeSize: {size:,} bytes")
                return {"success": True, "size": size, "method": "curl object/stat"}
            else:
                print("❌ No CumulativeSize found")
                return {"success": False, "error": "No CumulativeSize", "method": "curl object/stat"}
        else:
            print(f"❌ Curl failed: {result.stderr}")
            return {"success": False, "error": result.stderr, "method": "curl object/stat"}
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"success": False, "error": str(e), "method": "curl object/stat"}

def test_curl_files_stat(cid: str) -> dict:
    """Test using curl command for files/stat"""
    print(f"\n=== Testing curl files/stat for {cid} ===")
    
    cmd = [
        "curl", "-X", "POST", 
        f"{IPFS_NODE_URL}/api/v0/files/stat?arg=/ipfs/{cid}",
        "-s"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            stats = json.loads(result.stdout)
            print(f"✅ Response: {json.dumps(stats, indent=2)}")
            
            size = stats.get("CumulativeSize") or stats.get("Size")
            if size is not None:
                print(f"✅ Size: {size:,} bytes")
                return {"success": True, "size": size, "method": "curl files/stat"}
            else:
                print("❌ No Size found")
                return {"success": False, "error": "No Size", "method": "curl files/stat"}
        else:
            print(f"❌ Curl failed: {result.stderr}")
            return {"success": False, "error": result.stderr, "method": "curl files/stat"}
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"success": False, "error": str(e), "method": "curl files/stat"}

async def main():
    """Run all tests"""
    print("🧪 Testing IPFS file size fetching endpoints")
    print(f"IPFS Node URL: {IPFS_NODE_URL}")
    
    results = []
    
    for cid in TEST_CIDS:
        print(f"\n{'='*60}")
        print(f"Testing CID: {cid}")
        print(f"{'='*60}")
        
        # Test all endpoints
        results.append(await test_object_stat(cid))
        results.append(await test_files_stat(cid))
        results.append(await test_block_stat(cid))
        results.append(test_curl_object_stat(cid))
        results.append(test_curl_files_stat(cid))
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    
    if successful:
        print("\n✅ Successful methods:")
        for result in successful:
            print(f"  - {result['method']}: {result['size']:,} bytes")
    
    if failed:
        print("\n❌ Failed methods:")
        for result in failed:
            print(f"  - {result['method']}: {result['error']}")
    
    # Recommendation
    print(f"\n{'='*60}")
    print("RECOMMENDATION")
    print(f"{'='*60}")
    
    if any(r["method"] == "object/stat" and r["success"] for r in results):
        print("✅ Use /api/v0/object/stat endpoint - it works!")
    elif any(r["method"] == "files/stat" and r["success"] for r in results):
        print("✅ Use /api/v0/files/stat endpoint - it works!")
    elif any(r["method"] == "block/stat" and r["success"] for r in results):
        print("✅ Use /api/v0/block/stat endpoint - it works!")
    else:
        print("❌ None of the endpoints worked. Check IPFS node connection.")

if __name__ == "__main__":
    asyncio.run(main())