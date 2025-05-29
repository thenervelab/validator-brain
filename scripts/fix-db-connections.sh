#!/bin/bash

# Database Connection Fix Script - High Scale Edition
# This script applies high-scale database connection settings and provides monitoring tools
# Supports up to 1000 concurrent connections

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

log "🚀 Applying HIGH-SCALE PostgreSQL configuration (1000 max connections)..."

# Apply the updated ConfigMap
log "📝 Applying updated ConfigMap with high-scale database settings..."
kubectl apply -f k8s/configmap.yaml

# Apply the updated PostgreSQL deployment
log "🐘 Applying updated PostgreSQL deployment with 1000 max_connections..."
kubectl apply -f k8s/postgres.yaml

# Wait for PostgreSQL to restart
log "⏳ Waiting for PostgreSQL to restart with new configuration..."
kubectl rollout status deployment/postgres --timeout=600s

# Restart all consumer deployments to pick up new connection pool settings
log "🔄 Restarting consumer deployments to apply new connection pool settings..."

CONSUMERS=(
    "user-profile-consumer"
    "pinning-request-consumer"
    "node-metrics-consumer"
    "registration-consumer"
    "miner-profile-reconstruction-consumer"
    "user-profile-reconstruction-consumer"
    "pinning-file-consumer"
    "miner-health-consumer"
    "epoch-health-consumer"
    "file-assignment-consumer"
)

for consumer in "${CONSUMERS[@]}"; do
    log "🔄 Restarting $consumer..."
    kubectl rollout restart deployment/$consumer
done

# Restart epoch orchestrator
log "🔄 Restarting epoch orchestrator..."
kubectl rollout restart deployment/epoch-orchestrator

# Wait for all deployments to be ready
log "⏳ Waiting for all deployments to be ready..."
for consumer in "${CONSUMERS[@]}"; do
    kubectl rollout status deployment/$consumer --timeout=120s
done
kubectl rollout status deployment/epoch-orchestrator --timeout=120s

log "✅ All deployments restarted successfully!"

# Show connection monitoring commands
echo ""
echo -e "${BLUE}📊 High-Scale Database Connection Monitoring Commands:${NC}"
echo ""
echo "1. Check current PostgreSQL connections:"
echo "   kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SELECT count(*) as active_connections FROM pg_stat_activity WHERE state = 'active';\""
echo ""
echo "2. Check total connections:"
echo "   kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SELECT count(*) as total_connections FROM pg_stat_activity;\""
echo ""
echo "3. Check max_connections setting:"
echo "   kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SHOW max_connections;\""
echo ""
echo "4. Monitor connections by application:"
echo "   kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SELECT application_name, count(*) FROM pg_stat_activity GROUP BY application_name ORDER BY count(*) DESC;\""
echo ""
echo "5. Check connection states:"
echo "   kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SELECT state, count(*) FROM pg_stat_activity GROUP BY state;\""
echo ""
echo "6. Monitor connection usage over time:"
echo "   kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SELECT now(), count(*) as total_connections, count(*) filter (where state = 'active') as active_connections FROM pg_stat_activity;\""
echo ""
echo "7. Check for connection errors in consumer logs:"
echo "   kubectl logs -l app=user-profile-consumer --tail=50 | grep -i connection"
echo ""
echo "8. Monitor PostgreSQL performance:"
echo "   kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c \"SELECT * FROM pg_stat_database WHERE datname = 'substrate_fetcher';\""
echo ""

# Function to check current connections
check_connections() {
    echo -e "${BLUE}📊 Current High-Scale Database Connection Status:${NC}"
    
    echo "Max connections:"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "SHOW max_connections;" 2>/dev/null || echo "Could not connect to database"
    
    echo ""
    echo "Shared buffers:"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "SHOW shared_buffers;" 2>/dev/null || echo "Could not connect to database"
    
    echo ""
    echo "Current active connections:"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "SELECT count(*) as active_connections FROM pg_stat_activity WHERE state = 'active';" 2>/dev/null || echo "Could not connect to database"
    
    echo ""
    echo "Total connections:"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "SELECT count(*) as total_connections FROM pg_stat_activity;" 2>/dev/null || echo "Could not connect to database"
    
    echo ""
    echo "Connection breakdown by state:"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "SELECT state, count(*) FROM pg_stat_activity GROUP BY state ORDER BY count(*) DESC;" 2>/dev/null || echo "Could not connect to database"
    
    echo ""
    echo "Top applications by connection count:"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "SELECT application_name, count(*) FROM pg_stat_activity WHERE application_name IS NOT NULL GROUP BY application_name ORDER BY count(*) DESC LIMIT 10;" 2>/dev/null || echo "Could not connect to database"
}

# Function to monitor connections continuously
monitor_connections() {
    echo -e "${BLUE}📊 Monitoring database connections (press Ctrl+C to stop)...${NC}"
    while true; do
        echo "$(date): $(kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -t -c "SELECT count(*) FROM pg_stat_activity;" 2>/dev/null | tr -d ' \n') total connections"
        sleep 5
    done
}

# Check connections after restart
log "📊 Checking database connections after restart..."
sleep 15
check_connections

echo ""
echo -e "${GREEN}🎉 High-Scale Database connection optimization complete!${NC}"
echo ""
echo -e "${YELLOW}📋 Summary of HIGH-SCALE changes:${NC}"
echo "- PostgreSQL max_connections: 1000 (was 200)"
echo "- Shared buffers: 512MB (was 256MB)"
echo "- Effective cache size: 2GB (was 1GB)"
echo "- Work memory: 8MB (was 4MB)"
echo "- WAL buffers: 32MB (was 16MB)"
echo "- Max WAL size: 8GB (was 4GB)"
echo "- Worker processes: 16 (was 8)"
echo "- Memory limits: 4Gi (was 2Gi)"
echo "- CPU limits: 2000m (was 1000m)"
echo ""
echo -e "${BLUE}💡 Expected HIGH-SCALE connection capacity:${NC}"
echo "- 9 consumers × 8 max connections = 72 connections"
echo "- 1 epoch orchestrator × 8 max connections = 8 connections"
echo "- Reserve for scaling: ~920 connections available"
echo "- Total capacity: 1000 connections"
echo ""
echo "Available commands:"
echo "  bash scripts/fix-db-connections.sh check     # Check current status"
echo "  bash scripts/fix-db-connections.sh monitor   # Monitor continuously"

# Handle command line arguments
case "$1" in
    "check")
        check_connections
        exit 0
        ;;
    "monitor")
        monitor_connections
        exit 0
        ;;
    *)
        # Default behavior - already executed above
        ;;
esac 