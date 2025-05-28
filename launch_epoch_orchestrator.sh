#!/bin/bash

# IPFS Service Validator - Epoch Orchestrator Launcher
# This script launches the epoch orchestrator with proper environment setup

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ✅ $1"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ⚠️  $1"
}

print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} ❌ $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check environment variables
check_env_vars() {
    print_status "Checking required environment variables..."
    
    local missing_vars=()
    
    # Required variables
    if [[ -z "${VALIDATOR_ACCOUNT_ID:-}" ]]; then
        missing_vars+=("VALIDATOR_ACCOUNT_ID")
    fi
    
    if [[ -z "${NODE_URL:-}" ]]; then
        missing_vars+=("NODE_URL")
    fi
    
    if [[ -z "${DATABASE_URL:-}" ]]; then
        missing_vars+=("DATABASE_URL")
    fi
    
    if [[ -z "${RABBITMQ_URL:-}" ]]; then
        missing_vars+=("RABBITMQ_URL")
    fi
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        print_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        echo ""
        echo "Please set these variables before running the orchestrator."
        echo "Example:"
        echo "  export VALIDATOR_ACCOUNT_ID='5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g'"
        echo "  export NODE_URL='wss://rpc.hippius.network'"
        echo "  export DATABASE_URL='postgresql://user:password@localhost:5432/substrate_fetcher'"
        echo "  export RABBITMQ_URL='amqp://admin:admin@localhost:5672/'"
        exit 1
    fi
    
    print_success "All required environment variables are set"
    
    # Optional variables with warnings
    if [[ -z "${VALIDATOR_SEED:-}" ]]; then
        print_warning "VALIDATOR_SEED not set - transaction signing will be disabled"
    else
        print_success "VALIDATOR_SEED is set - transaction signing enabled"
    fi
}

# Function to check dependencies
check_dependencies() {
    print_status "Checking dependencies..."
    
    # Check Python
    if ! command_exists python; then
        print_error "Python is not installed or not in PATH"
        exit 1
    fi
    
    local python_version=$(python --version 2>&1 | cut -d' ' -f2)
    print_success "Python version: $python_version"
    
    # Check if we're in a virtual environment
    if [[ -z "${VIRTUAL_ENV:-}" ]]; then
        print_warning "Not running in a virtual environment"
        print_status "Consider activating your virtual environment first:"
        print_status "  source .venv/bin/activate"
    else
        print_success "Virtual environment active: $VIRTUAL_ENV"
    fi
    
    # Check if epoch_orchestrator.py exists
    if [[ ! -f "epoch_orchestrator.py" ]]; then
        print_error "epoch_orchestrator.py not found in current directory"
        print_status "Please run this script from the project root directory"
        exit 1
    fi
    
    print_success "epoch_orchestrator.py found"
}

# Function to test blockchain connectivity
test_blockchain_connection() {
    print_status "Testing blockchain connectivity..."
    
    python -c "
import os
import sys
sys.path.append('.')
try:
    from app.utils.epoch_validator import connect_substrate, get_current_epoch_info
    substrate = connect_substrate()
    epoch, block = get_current_epoch_info(substrate)
    print(f'✅ Connected to blockchain - Epoch: {epoch}, Block: {block}')
    substrate.close()
except Exception as e:
    print(f'❌ Blockchain connection failed: {e}')
    sys.exit(1)
" || {
        print_error "Blockchain connectivity test failed"
        exit 1
    }
}

# Function to test database connectivity
test_database_connection() {
    print_status "Testing database connectivity..."
    
    python -c "
import asyncio
import os
import sys
sys.path.append('.')
async def test_db():
    try:
        from app.db.connection import init_db_pool, close_db_pool, get_db_pool
        await init_db_pool()
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval('SELECT 1')
            if result == 1:
                print('✅ Database connection successful')
            else:
                print('❌ Database connection test failed')
                sys.exit(1)
        await close_db_pool()
    except Exception as e:
        print(f'❌ Database connection failed: {e}')
        sys.exit(1)

asyncio.run(test_db())
" || {
        print_error "Database connectivity test failed"
        exit 1
    }
}

# Function to display current status
show_status() {
    print_status "Current blockchain status:"
    
    python -c "
import os
import sys
sys.path.append('.')
try:
    from app.utils.epoch_validator import *
    substrate = connect_substrate()
    epoch, block = get_current_epoch_info(substrate)
    validator_account = get_validator_account_from_env()
    is_val, current_val, epoch_start = is_epoch_validator(substrate, validator_account)
    
    print(f'  Epoch: {epoch}')
    print(f'  Block: {block}')
    print(f'  Position: {block % 100}/99')
    print(f'  Current Validator: {current_val}')
    print(f'  Our Account: {validator_account}')
    print(f'  We are Validator: {\"YES\" if is_val else \"NO\"}')
    
    substrate.close()
except Exception as e:
    print(f'  Error getting status: {e}')
"
}

# Function to launch the orchestrator
launch_orchestrator() {
    print_status "Launching Epoch Orchestrator..."
    print_status "Press Ctrl+C to stop"
    echo ""
    
    # Set default values for optional environment variables
    export BLOCK_CHECK_INTERVAL="${BLOCK_CHECK_INTERVAL:-6}"
    export QUEUE_CHECK_TIMEOUT="${QUEUE_CHECK_TIMEOUT:-300}"
    
    # Launch the orchestrator
    python epoch_orchestrator.py
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --check-only              Only run connectivity checks, don't launch orchestrator"
    echo "  --status                  Show current blockchain status and exit"
    echo "  --update-configmap        Update Kubernetes ConfigMap with validator credentials"
    echo "  --validator-account ID    Set validator account ID (use with --update-configmap)"
    echo "  --validator-seed SEED     Set validator seed phrase (use with --update-configmap)"
    echo "  --apply-k8s              Apply ConfigMap changes to Kubernetes"
    echo "  --help                    Show this help message"
    echo ""
    echo "Environment Variables (Required):"
    echo "  VALIDATOR_ACCOUNT_ID    Your validator account ID"
    echo "  NODE_URL               Blockchain RPC endpoint"
    echo "  DATABASE_URL           PostgreSQL connection string"
    echo "  RABBITMQ_URL           RabbitMQ connection string"
    echo ""
    echo "Environment Variables (Optional):"
    echo "  VALIDATOR_SEED         Your validator seed phrase (for transaction signing)"
    echo "  BLOCK_CHECK_INTERVAL   How often to check blockchain (default: 6 seconds)"
    echo "  QUEUE_CHECK_TIMEOUT    Queue processing timeout (default: 300 seconds)"
    echo ""
    echo "Examples:"
    echo "  # Set environment and launch locally"
    echo "  export VALIDATOR_ACCOUNT_ID='5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g'"
    echo "  export NODE_URL='wss://rpc.hippius.network'"
    echo "  export DATABASE_URL='postgresql://user:password@localhost:5432/substrate_fetcher'"
    echo "  export RABBITMQ_URL='amqp://admin:admin@localhost:5672/'"
    echo "  $0"
    echo ""
    echo "  # Update Kubernetes ConfigMap and deploy"
    echo "  $0 --update-configmap --validator-account '5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g' \\"
    echo "     --validator-seed 'your twelve word seed phrase here' --apply-k8s"
    echo ""
    echo "  # Just check connectivity"
    echo "  $0 --check-only"
    echo ""
    echo "  # Show current status"
    echo "  $0 --status"
}

# Function to update Kubernetes ConfigMap
update_configmap() {
    local validator_account="$1"
    local validator_seed="$2"
    local apply_to_k8s="$3"
    
    print_status "Updating Kubernetes ConfigMap..."
    
    # Check if we're in the right directory or can find the ConfigMap
    local configmap_path=""
    if [[ -f "k8s/configmap.yaml" ]]; then
        configmap_path="k8s/configmap.yaml"
    elif [[ -f "configmap.yaml" ]]; then
        configmap_path="configmap.yaml"
    else
        print_error "ConfigMap file not found. Please run from project root or k8s/ directory."
        return 1
    fi
    
    print_status "Found ConfigMap at: $configmap_path"
    
    # Update validator account if provided
    if [[ -n "$validator_account" ]]; then
        print_status "Setting VALIDATOR_ACCOUNT_ID to: $validator_account"
        
        # Use sed to update the ConfigMap
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            sed -i '' "s/VALIDATOR_ACCOUNT_ID: \".*\"/VALIDATOR_ACCOUNT_ID: \"$validator_account\"/" "$configmap_path"
        else
            # Linux
            sed -i "s/VALIDATOR_ACCOUNT_ID: \".*\"/VALIDATOR_ACCOUNT_ID: \"$validator_account\"/" "$configmap_path"
        fi
        
        print_success "Updated VALIDATOR_ACCOUNT_ID"
    fi
    
    # Update validator seed if provided
    if [[ -n "$validator_seed" ]]; then
        print_status "Setting VALIDATOR_SEED..."
        
        # Check if the VALIDATOR_SEED line is commented out
        if grep -q "# VALIDATOR_SEED:" "$configmap_path"; then
            # Uncomment and update
            if [[ "$OSTYPE" == "darwin"* ]]; then
                # macOS
                sed -i '' "s/# VALIDATOR_SEED: \".*\"/VALIDATOR_SEED: \"$validator_seed\"/" "$configmap_path"
            else
                # Linux
                sed -i "s/# VALIDATOR_SEED: \".*\"/VALIDATOR_SEED: \"$validator_seed\"/" "$configmap_path"
            fi
        else
            # Add the line after VALIDATOR_ACCOUNT_ID
            if [[ "$OSTYPE" == "darwin"* ]]; then
                # macOS
                sed -i '' "/VALIDATOR_ACCOUNT_ID:/a\\
  VALIDATOR_SEED: \"$validator_seed\"" "$configmap_path"
            else
                # Linux
                sed -i "/VALIDATOR_ACCOUNT_ID:/a\\  VALIDATOR_SEED: \"$validator_seed\"" "$configmap_path"
            fi
        fi
        
        print_success "Updated VALIDATOR_SEED"
    fi
    
    # Show what was updated
    if [[ -n "$validator_account" ]] || [[ -n "$validator_seed" ]]; then
        print_status "Updated ConfigMap contents:"
        echo ""
        grep -A 5 -B 5 "VALIDATOR_" "$configmap_path" || true
        echo ""
    fi
    
    # Apply to Kubernetes if requested
    if [[ "$apply_to_k8s" == "true" ]]; then
        print_status "Applying ConfigMap to Kubernetes..."
        
        if command -v kubectl >/dev/null 2>&1; then
            kubectl apply -f "$configmap_path"
            print_success "ConfigMap applied to Kubernetes"
            
            # Restart epoch orchestrator to pick up new config
            print_status "Restarting epoch orchestrator to pick up new configuration..."
            kubectl rollout restart deployment/epoch-orchestrator
            print_success "Epoch orchestrator restarted"
            
            # Show status
            print_status "Checking deployment status..."
            kubectl get pods -l app=epoch-orchestrator
        else
            print_error "kubectl not found. Please install kubectl to apply to Kubernetes."
            print_status "You can manually apply with: kubectl apply -f $configmap_path"
            return 1
        fi
    fi
    
    return 0
}

# Main execution
main() {
    echo ""
    echo "🎯 IPFS Service Validator - Epoch Orchestrator"
    echo "=============================================="
    echo ""
    
    # Parse command line arguments
    local update_configmap=false
    local validator_account=""
    local validator_seed=""
    local apply_k8s=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help|-h)
                show_usage
                exit 0
                ;;
            --status)
                check_env_vars
                show_status
                exit 0
                ;;
            --check-only)
                check_env_vars
                check_dependencies
                test_blockchain_connection
                test_database_connection
                print_success "All connectivity checks passed!"
                exit 0
                ;;
            --update-configmap)
                update_configmap=true
                shift
                ;;
            --validator-account)
                validator_account="$2"
                shift 2
                ;;
            --validator-seed)
                validator_seed="$2"
                shift 2
                ;;
            --apply-k8s)
                apply_k8s=true
                shift
                ;;
            "")
                # Normal launch
                break
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Handle ConfigMap update mode
    if [[ "$update_configmap" == true ]]; then
        if [[ -z "$validator_account" ]] && [[ -z "$validator_seed" ]]; then
            print_error "ConfigMap update requires --validator-account and/or --validator-seed"
            show_usage
            exit 1
        fi
        
        update_configmap "$validator_account" "$validator_seed" "$apply_k8s"
        exit $?
    fi
    
    # Normal launch mode - run all checks
    check_env_vars
    check_dependencies
    test_blockchain_connection
    test_database_connection
    
    print_success "All pre-flight checks passed!"
    echo ""
    
    # Show current status
    show_status
    echo ""
    
    # Launch the orchestrator
    launch_orchestrator
}

# Handle Ctrl+C gracefully
trap 'echo ""; print_status "Shutting down Epoch Orchestrator..."; exit 0' INT

# Run main function
main "$@" 