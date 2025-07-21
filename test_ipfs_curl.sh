#!/bin/bash

# Test script to verify IPFS file size fetching works correctly
# Tests both object/stat and files/stat endpoints with real CIDs

# Test CIDs
CID1="QmXrZQdLBKhqabxSik5HJ1vwMDvZGt6F7hD9NFBvAbxarV"
CID2="bafybeifl2vpxs5kxbla44zhxsndkni4t2vatszlkklwyz2eqnig7lenzr4"

# IPFS node URL
IPFS_NODE_URL="https://ipfs.hippius.com"

echo "🧪 Testing IPFS file size fetching endpoints"
echo "IPFS Node URL: $IPFS_NODE_URL"
echo ""

test_endpoint() {
    local endpoint="$1"
    local cid="$2"
    local arg="$3"
    
    echo "=== Testing $endpoint for $cid ==="
    
    local url="$IPFS_NODE_URL/api/v0/$endpoint?arg=$arg"
    echo "URL: $url"
    
    response=$(curl -s -X POST "$url" 2>/dev/null)
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "✅ Response: $response"
        
        # Try to extract size using different field names
        cumulative_size=$(echo "$response" | python3 -c "import json, sys; data=json.load(sys.stdin); print(data.get('CumulativeSize', 'N/A'))" 2>/dev/null)
        size=$(echo "$response" | python3 -c "import json, sys; data=json.load(sys.stdin); print(data.get('Size', 'N/A'))" 2>/dev/null)
        
        if [ "$cumulative_size" != "N/A" ] && [ "$cumulative_size" != "None" ]; then
            echo "✅ CumulativeSize: $cumulative_size bytes"
        elif [ "$size" != "N/A" ] && [ "$size" != "None" ]; then
            echo "✅ Size: $size bytes"
        else
            echo "❌ No size field found"
        fi
    else
        echo "❌ Request failed with exit code $exit_code"
    fi
    
    echo ""
}

# Test both CIDs with different endpoints
for cid in "$CID1" "$CID2"; do
    echo "=============================================="
    echo "Testing CID: $cid"
    echo "=============================================="
    
    # Test object/stat
    test_endpoint "object/stat" "$cid" "$cid"
    
    # Test files/stat
    test_endpoint "files/stat" "$cid" "/ipfs/$cid"
    
    # Test block/stat
    test_endpoint "block/stat" "$cid" "$cid"
    
    echo ""
done

echo "=============================================="
echo "Summary"
echo "=============================================="
echo "If any endpoint returned size data, that's the one to use in the code."
echo "object/stat is typically best for regular IPFS content."
echo "files/stat is for IPFS filesystem (MFS) content."
echo "block/stat is for raw blocks."