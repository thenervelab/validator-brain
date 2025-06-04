#!/bin/bash

# Show Consumer Configuration Differences
# This script shows how consumers.yaml is modified when using external IPFS

echo "🔍 Consumer Configuration Differences for External IPFS"
echo "========================================================"
echo ""

echo "📋 Original consumers.yaml (with IPFS checks):"
echo "----------------------------------------------"
grep -n "nc -z.*ipfs-service" consumers.yaml || echo "No IPFS service checks found"

echo ""
echo "📋 Modified consumers.yaml (without IPFS checks):"
echo "------------------------------------------------"

# Create the modified version in temp file
temp_file="/tmp/consumers-test.yaml"
sed -e 's/ && nc -z ipfs-service 5001//g' consumers.yaml > "$temp_file"

grep -n "nc -z.*ipfs-service" "$temp_file" || echo "✅ All IPFS service checks successfully removed"

echo ""
echo "🔍 Specific changes made:"
echo "------------------------"

# Show side-by-side diff of the relevant lines
echo "BEFORE (lines with IPFS checks):"
grep -n "nc -z.*ipfs-service" consumers.yaml | head -5

echo ""
echo "AFTER (same lines, IPFS checks removed):"
grep -B1 -A1 "until nc -z postgres-service.*rabbitmq-service.*; do" "$temp_file" | grep "until nc" | head -5

echo ""
echo "💡 Summary:"
echo "----------"

original_ipfs_checks=$(grep -c "nc -z ipfs-service" consumers.yaml)
modified_ipfs_checks=$(grep -c "nc -z ipfs-service" "$temp_file")

echo "  Original consumers.yaml: $original_ipfs_checks IPFS service checks"
echo "  Modified consumers.yaml: $modified_ipfs_checks IPFS service checks"
echo "  Change: Removed $(($original_ipfs_checks - $modified_ipfs_checks)) IPFS service dependency checks"

echo ""
echo "✅ Affected consumers:"
echo "  - user-profile-consumer"  
echo "  - pinning-file-consumer"
echo "  - miner-health-consumer"
echo "  - epoch-health-consumer"

echo ""
echo "🎯 Result: These consumers will no longer wait for local ipfs-service"
echo "       and can start immediately when using external IPFS endpoints!"

# Clean up
rm -f "$temp_file" 