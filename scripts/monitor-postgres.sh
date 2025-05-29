#!/bin/bash

# PostgreSQL High-Scale Monitoring Script
# Provides comprehensive monitoring for PostgreSQL with 1000+ connection capacity

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to execute PostgreSQL queries
pg_exec() {
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -t -c "$1" 2>/dev/null | tr -d '\r' | sed 's/^[ \t]*//;s/[ \t]*$//'
}

# Function to display header
header() {
    echo -e "${CYAN}================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}================================${NC}"
}

# Function to show connection overview
show_connection_overview() {
    header "📊 CONNECTION OVERVIEW"
    
    echo -e "${GREEN}Max Connections:${NC} $(pg_exec "SHOW max_connections;")"
    echo -e "${GREEN}Current Total:${NC} $(pg_exec "SELECT count(*) FROM pg_stat_activity;")"
    echo -e "${GREEN}Active Connections:${NC} $(pg_exec "SELECT count(*) FROM pg_stat_activity WHERE state = 'active';")"
    echo -e "${GREEN}Idle Connections:${NC} $(pg_exec "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle';")"
    echo -e "${GREEN}Idle in Transaction:${NC} $(pg_exec "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction';")"
    
    # Calculate usage percentage
    max_conn=$(pg_exec "SHOW max_connections;")
    current_conn=$(pg_exec "SELECT count(*) FROM pg_stat_activity;")
    if [[ -n "$max_conn" && -n "$current_conn" ]]; then
        usage_pct=$(echo "scale=1; $current_conn * 100 / $max_conn" | bc -l 2>/dev/null || echo "N/A")
        echo -e "${GREEN}Usage:${NC} ${usage_pct}% (${current_conn}/${max_conn})"
    fi
    echo ""
}

# Function to show connection states
show_connection_states() {
    header "🔄 CONNECTION STATES"
    
    echo -e "${BLUE}State breakdown:${NC}"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "
        SELECT 
            COALESCE(state, 'unknown') as state,
            count(*) as count,
            round(count(*) * 100.0 / sum(count(*)) over(), 1) as percentage
        FROM pg_stat_activity 
        GROUP BY state 
        ORDER BY count(*) DESC;
    " 2>/dev/null || echo "Could not retrieve connection states"
    echo ""
}

# Function to show applications using connections
show_applications() {
    header "📱 APPLICATIONS"
    
    echo -e "${BLUE}Top applications by connection count:${NC}"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "
        SELECT 
            COALESCE(application_name, 'unknown') as application,
            count(*) as connections,
            round(count(*) * 100.0 / sum(count(*)) over(), 1) as percentage
        FROM pg_stat_activity 
        WHERE application_name IS NOT NULL OR application_name = ''
        GROUP BY application_name 
        ORDER BY count(*) DESC 
        LIMIT 15;
    " 2>/dev/null || echo "Could not retrieve application data"
    echo ""
}

# Function to show long-running queries
show_long_queries() {
    header "⏱️  LONG-RUNNING QUERIES"
    
    echo -e "${BLUE}Queries running longer than 30 seconds:${NC}"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "
        SELECT 
            pid,
            application_name,
            state,
            round(extract(epoch from (now() - query_start))::numeric, 2) as duration_seconds,
            left(query, 100) as query_preview
        FROM pg_stat_activity 
        WHERE state != 'idle' 
        AND query_start < now() - interval '30 seconds'
        AND query NOT LIKE '%pg_stat_activity%'
        ORDER BY query_start;
    " 2>/dev/null || echo "Could not retrieve long-running queries"
    echo ""
}

# Function to show database statistics
show_db_stats() {
    header "📈 DATABASE STATISTICS"
    
    echo -e "${BLUE}Database performance metrics:${NC}"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "
        SELECT 
            numbackends as active_connections,
            xact_commit as transactions_committed,
            xact_rollback as transactions_rolled_back,
            blks_read as blocks_read,
            blks_hit as blocks_hit,
            round((blks_hit * 100.0 / NULLIF(blks_hit + blks_read, 0))::numeric, 2) as cache_hit_ratio,
            tup_returned as tuples_returned,
            tup_fetched as tuples_fetched,
            tup_inserted as tuples_inserted,
            tup_updated as tuples_updated,
            tup_deleted as tuples_deleted
        FROM pg_stat_database 
        WHERE datname = 'substrate_fetcher';
    " 2>/dev/null || echo "Could not retrieve database statistics"
    echo ""
}

# Function to show memory and configuration
show_config() {
    header "⚙️  CONFIGURATION"
    
    echo -e "${GREEN}Memory Settings:${NC}"
    echo "  Shared Buffers: $(pg_exec "SHOW shared_buffers;")"
    echo "  Work Mem: $(pg_exec "SHOW work_mem;")"
    echo "  Maintenance Work Mem: $(pg_exec "SHOW maintenance_work_mem;")"
    echo "  Effective Cache Size: $(pg_exec "SHOW effective_cache_size;")"
    echo ""
    
    echo -e "${GREEN}Connection Settings:${NC}"
    echo "  Max Connections: $(pg_exec "SHOW max_connections;")"
    echo "  Idle in Transaction Timeout: $(pg_exec "SHOW idle_in_transaction_session_timeout;")"
    echo "  Lock Timeout: $(pg_exec "SHOW lock_timeout;")"
    echo ""
    
    echo -e "${GREEN}WAL Settings:${NC}"
    echo "  WAL Buffers: $(pg_exec "SHOW wal_buffers;")"
    echo "  Max WAL Size: $(pg_exec "SHOW max_wal_size;")"
    echo "  Min WAL Size: $(pg_exec "SHOW min_wal_size;")"
    echo ""
}

# Function to show locks
show_locks() {
    header "🔒 LOCKS"
    
    echo -e "${BLUE}Current locks:${NC}"
    kubectl exec -it deployment/postgres -- psql -U user -d substrate_fetcher -c "
        SELECT 
            mode,
            count(*) as lock_count
        FROM pg_locks 
        GROUP BY mode 
        ORDER BY count(*) DESC;
    " 2>/dev/null || echo "Could not retrieve lock information"
    echo ""
}

# Function to monitor continuously
monitor_continuous() {
    echo -e "${CYAN}🔄 Continuous PostgreSQL Monitoring (press Ctrl+C to stop)${NC}"
    echo ""
    
    while true; do
        clear
        echo -e "${CYAN}PostgreSQL High-Scale Monitor - $(date)${NC}"
        echo ""
        
        show_connection_overview
        show_connection_states
        show_applications
        
        echo -e "${YELLOW}Refreshing in 10 seconds...${NC}"
        sleep 10
    done
}

# Function to show help
show_help() {
    echo -e "${CYAN}PostgreSQL High-Scale Monitoring Script${NC}"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  overview    - Show connection overview (default)"
    echo "  states      - Show connection states"
    echo "  apps        - Show applications using connections"
    echo "  queries     - Show long-running queries"
    echo "  stats       - Show database statistics"
    echo "  config      - Show PostgreSQL configuration"
    echo "  locks       - Show current locks"
    echo "  monitor     - Continuous monitoring"
    echo "  all         - Show all information"
    echo "  help        - Show this help"
    echo ""
}

# Main script logic
case "${1:-overview}" in
    "overview")
        show_connection_overview
        ;;
    "states")
        show_connection_states
        ;;
    "apps")
        show_applications
        ;;
    "queries")
        show_long_queries
        ;;
    "stats")
        show_db_stats
        ;;
    "config")
        show_config
        ;;
    "locks")
        show_locks
        ;;
    "monitor")
        monitor_continuous
        ;;
    "all")
        show_connection_overview
        show_connection_states
        show_applications
        show_long_queries
        show_db_stats
        show_config
        show_locks
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac 