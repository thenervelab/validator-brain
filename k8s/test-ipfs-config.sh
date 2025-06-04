#!/bin/bash

# Test IPFS Configuration Script
# Verifies that custom IPFS endpoints are properly configured

NAMESPACE="${1:-validator-1}"
EXPECTED_ENDPOINT="${2:-}"

if [[ -z "$EXPECTED_ENDPOINT" ]]; then
    echo "Usage: $0 <namespace> <expected-ipfs-endpoint>"
    echo "Example: $0 validator-1 http://ipfs-service.ipfs.svc.cluster.local:5001"
    exit 1
fi

echo "🧪 Testing IPFS Configuration"
echo "==============================="
echo "Namespace: $NAMESPACE"
echo "Expected Endpoint: $EXPECTED_ENDPOINT"
echo ""

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" > /dev/null 2>&1; then
    echo "❌ ERROR: Namespace '$NAMESPACE' does not exist"
    exit 1
fi

echo "📋 Step 1: Checking ConfigMap..."
ACTUAL_ENDPOINT=$(kubectl get configmap ipfs-validator-config -n "$NAMESPACE" -o jsonpath='{.data.IPFS_NODE_URL}' 2>/dev/null || echo "")

if [[ -z "$ACTUAL_ENDPOINT" ]]; then
    echo "❌ ConfigMap not found or IPFS_NODE_URL not set"
    exit 1
fi

echo "   Configured endpoint: $ACTUAL_ENDPOINT"

if [[ "$ACTUAL_ENDPOINT" == "$EXPECTED_ENDPOINT" ]]; then
    echo "✅ ConfigMap has correct IPFS endpoint"
else
    echo "❌ ConfigMap endpoint mismatch!"
    echo "   Expected: $EXPECTED_ENDPOINT"
    echo "   Actual:   $ACTUAL_ENDPOINT"
    exit 1
fi

echo ""
echo "🌐 Step 2: Testing IPFS connectivity..."

# Test connectivity from within the cluster
echo "   Testing from within cluster..."
kubectl run test-ipfs-connectivity \
    --image=curlimages/curl \
    --restart=Never \
    --rm -i --quiet \
    -n "$NAMESPACE" \
    --timeout=30s \
    -- curl -f -s "$EXPECTED_ENDPOINT/api/v0/id" > /dev/null 2>&1

if [[ $? -eq 0 ]]; then
    echo "✅ IPFS endpoint is accessible from cluster"
else
    echo "⚠️  IPFS endpoint test failed (may be expected if service is not running)"
fi

echo ""
echo "📊 Step 3: Checking consumer pods..."
CONSUMER_PODS=$(kubectl get pods -l app.kubernetes.io/component=consumer -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)

if [[ "$CONSUMER_PODS" -gt 0 ]]; then
    echo "✅ Found $CONSUMER_PODS consumer pods that will use the configured endpoint"
    
    # Check if any pods are running and can access the config
    RUNNING_PODS=$(kubectl get pods -l app.kubernetes.io/component=consumer -n "$NAMESPACE" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    
    if [[ "$RUNNING_PODS" -gt 0 ]]; then
        echo "   $RUNNING_PODS pods are currently running"
        
        # Get the first running pod to test config
        FIRST_POD=$(kubectl get pods -l app.kubernetes.io/component=consumer -n "$NAMESPACE" --field-selector=status.phase=Running --no-headers -o custom-columns=":metadata.name" | head -1)
        
        if [[ -n "$FIRST_POD" ]]; then
            echo "   Testing config access from pod: $FIRST_POD"
            POD_ENDPOINT=$(kubectl exec "$FIRST_POD" -n "$NAMESPACE" -- env | grep IPFS_NODE_URL | cut -d'=' -f2 2>/dev/null || echo "")
            
            if [[ "$POD_ENDPOINT" == "$EXPECTED_ENDPOINT" ]]; then
                echo "✅ Pod has correct IPFS endpoint configured"
            else
                echo "⚠️  Pod may need restart to pick up new config"
                echo "   Pod endpoint: $POD_ENDPOINT"
            fi
        fi
    else
        echo "   No pods currently running (may be expected during deployment)"
    fi
else
    echo "⚠️  No consumer pods found (deployment may be in progress)"
fi

echo ""
echo "✅ IPFS Configuration Test Complete!"
echo ""
echo "💡 Next Steps:"
echo "  # View full ConfigMap:"
echo "  kubectl get configmap ipfs-validator-config -n $NAMESPACE -o yaml"
echo ""
echo "  # Restart consumers to pick up config changes:"
echo "  kubectl rollout restart deployment -l app.kubernetes.io/component=consumer -n $NAMESPACE"
echo ""
echo "  # Monitor consumer logs:"
echo "  kubectl logs -l app.kubernetes.io/component=consumer -n $NAMESPACE --tail=10" 