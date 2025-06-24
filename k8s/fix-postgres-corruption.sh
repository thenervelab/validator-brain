#!/bin/bash

# Fix PostgreSQL Corruption Script
# This script cleans corrupted PostgreSQL data and redeploys

set -e

NAMESPACE="${1:-validator-1}"

echo "🩺 Fixing PostgreSQL Corruption in namespace: $NAMESPACE"
echo "========================================================="

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" > /dev/null 2>&1; then
    echo "❌ ERROR: Namespace '$NAMESPACE' does not exist"
    exit 1
fi

echo "⚠️  This will DELETE all PostgreSQL data and start fresh!"
read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Operation cancelled"
    exit 0
fi

echo "🧹 Step 1: Deleting PostgreSQL deployment..."
kubectl delete deployment postgres -n "$NAMESPACE" --ignore-not-found=true

echo "🧹 Step 2: Deleting PostgreSQL service..."
kubectl delete service postgres-service -n "$NAMESPACE" --ignore-not-found=true

echo "🧹 Step 3: Deleting corrupted PostgreSQL PVC..."
kubectl delete pvc postgres-pvc -n "$NAMESPACE" --ignore-not-found=true

echo "⏳ Step 4: Waiting for PVC to be fully deleted..."
while kubectl get pvc postgres-pvc -n "$NAMESPACE" > /dev/null 2>&1; do
    echo "   Still deleting PVC..."
    sleep 5
done

echo "🐘 Step 5: Redeploying PostgreSQL with fresh data..."
kubectl apply -f postgres.yaml -n "$NAMESPACE"

echo "⏳ Step 6: Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=available deployment/postgres -n "$NAMESPACE" --timeout=300s

echo "🔄 Step 7: Running database migrations..."
kubectl delete job dbmate-migrations -n "$NAMESPACE" --ignore-not-found=true
kubectl apply -f dbmate-job.yaml -n "$NAMESPACE"

echo "⏳ Step 8: Waiting for migrations to complete..."
sleep 10
for i in {1..20}; do
    STATUS=$(kubectl get jobs dbmate-migrations -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
    if [[ "$STATUS" == "True" ]]; then
        echo "✅ Database migrations completed!"
        break
    fi
    
    if [[ $i -eq 20 ]]; then
        echo "⚠️  Migrations taking longer than expected, check manually"
        break
    fi
    
    echo "   Waiting for migrations... ($i/20)"
    sleep 15
done

echo ""
echo "✅ PostgreSQL Recovery Complete!"
echo "================================="
echo ""
echo "📋 Verification Commands:"
echo "  # Check PostgreSQL status:"
echo "  kubectl get pods -l app=postgres -n $NAMESPACE"
echo ""
echo "  # Check migration job:"
echo "  kubectl get jobs dbmate-migrations -n $NAMESPACE"
echo ""
echo "  # Test database connection:"
echo "  kubectl exec -it deployment/postgres -n $NAMESPACE -- psql -U user -d substrate_fetcher -c \"SELECT version();\""
echo ""
echo "🎯 You can now restart your failed processor jobs!" 