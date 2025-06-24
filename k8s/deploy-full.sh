#!/bin/bash

# IPFS Service Validator - Full Deployment Script
# Deploys all orchestrator components with proper dependency management
# Usage: ./deploy-full.sh [NAMESPACE] [OPTIONS]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default values
NAMESPACE="ipfs-validator"
BUILD_IMAGE=true
WAIT_TIMEOUT=300
VERBOSE=false
DRY_RUN=false
NAMESPACE_SET=false
DEPLOY_IPFS=true
CUSTOM_IPFS_ENDPOINT=""

# Helper functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

success() {
    echo -e "${CYAN}[$(date +'%Y-%m-%d %H:%M:%S')] ✅ $1${NC}"
}

# Show usage
usage() {
    echo "IPFS Service Validator - Full Deployment Script"
    echo ""
    echo "Usage: $0 [NAMESPACE] [OPTIONS]"
    echo ""
    echo "Parameters:"
    echo "  NAMESPACE     Kubernetes namespace (default: ipfs-validator)"
    echo ""
    echo "Options:"
    echo "  --no-build          Skip Docker image build"
    echo "  --no-ipfs           Skip IPFS deployment (use external IPFS)"
    echo "  --ipfs-endpoint URL Set custom IPFS endpoint (e.g., http://ipfs-service.ipfs.svc.cluster.local:5001)"
    echo "  --timeout N         Wait timeout in seconds (default: 300)"
    echo "  --verbose           Enable verbose logging"
    echo "  --dry-run           Show what would be deployed without applying"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Deploy to default namespace"
    echo "  $0 my-namespace                       # Deploy to custom namespace"
    echo "  $0 prod-validator --no-build         # Deploy without building image"
    echo "  $0 test-env --no-ipfs --verbose      # Deploy without IPFS, with verbose output"
    echo "  $0 external --no-ipfs --ipfs-endpoint http://ipfs-service.ipfs.svc.cluster.local:5001  # Use external IPFS"
    echo "  $0 test-env --timeout 600 --verbose  # Deploy with custom timeout and verbose output"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            usage
            exit 0
            ;;
        --no-build)
            BUILD_IMAGE=false
            shift
            ;;
        --no-ipfs)
            DEPLOY_IPFS=false
            shift
            ;;
        --ipfs-endpoint)
            CUSTOM_IPFS_ENDPOINT="$2"
            DEPLOY_IPFS=false  # Automatically disable local IPFS when using custom endpoint
            shift 2
            ;;
        --timeout)
            WAIT_TIMEOUT="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --*)
            error "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            if [[ "$NAMESPACE_SET" == "false" ]]; then
                NAMESPACE="$1"
                NAMESPACE_SET=true
            else
                error "Multiple namespaces specified"
                usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate namespace name
if [[ ! "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
    error "Invalid namespace name: $NAMESPACE"
    error "Namespace must contain only lowercase letters, numbers, and hyphens"
    exit 1
fi

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    error "kubectl is not installed or not in PATH"
    exit 1
fi

# Check if Docker is available (only if building)
if [[ "$BUILD_IMAGE" == "true" ]] && ! command -v docker &> /dev/null; then
    error "Docker is not installed or not in PATH"
    exit 1
fi

# Function to apply Kubernetes resource
apply_resource() {
    local resource_file=$1
    local description=$2
    
    if [[ ! -f "$resource_file" ]]; then
        error "Resource file not found: $resource_file"
        return 1
    fi
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY RUN: Would apply $description from $resource_file"
        return 0
    fi
    
    info "$description"
    if [[ "$VERBOSE" == "true" ]]; then
        kubectl apply -f "$resource_file" -n "$NAMESPACE"
    else
        kubectl apply -f "$resource_file" -n "$NAMESPACE" > /dev/null
    fi
    success "$description completed"
}

# Function to wait for deployment
wait_for_deployment() {
    local deployment_name=$1
    local description=$2
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY RUN: Would wait for $description"
        return 0
    fi
    
    info "⏳ Waiting for $description to be ready..."
    if kubectl wait --for=condition=available deployment/"$deployment_name" -n "$NAMESPACE" --timeout="${WAIT_TIMEOUT}s" > /dev/null 2>&1; then
        success "$description is ready"
    else
        warn "$description may not be fully ready yet, continuing..."
    fi
}

# Function to wait for StatefulSet
wait_for_statefulset() {
    local statefulset_name=$1
    local description=$2
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY RUN: Would wait for $description"
        return 0
    fi
    
    info "⏳ Waiting for $description to be ready..."
    if kubectl wait --for=condition=ready pod -l app="$statefulset_name" -n "$NAMESPACE" --timeout="${WAIT_TIMEOUT}s" > /dev/null 2>&1; then
        success "$description is ready"
    else
        warn "$description may not be fully ready yet, continuing..."
    fi
}

# Function to wait for pods by label
wait_for_pods() {
    local label_selector=$1
    local description=$2
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY RUN: Would wait for $description"
        return 0
    fi
    
    info "⏳ Waiting for $description to be ready..."
    if kubectl wait --for=condition=ready pod -l "$label_selector" -n "$NAMESPACE" --timeout="${WAIT_TIMEOUT}s" > /dev/null 2>&1; then
        success "$description is ready"
    else
        warn "$description may not be fully ready yet, continuing..."
    fi
}

# Function to check if namespace exists
namespace_exists() {
    kubectl get namespace "$NAMESPACE" > /dev/null 2>&1
}

# Function to create namespace
create_namespace() {
    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY RUN: Would create namespace $NAMESPACE"
        return 0
    fi
    
    info "Creating namespace: $NAMESPACE"
    kubectl create namespace "$NAMESPACE" || {
        warn "Namespace $NAMESPACE may already exist or creation failed"
    }
    success "Namespace $NAMESPACE ready"
}

# Function to update ConfigMap with custom IPFS endpoint
update_configmap_ipfs_endpoint() {
    if [[ -n "$CUSTOM_IPFS_ENDPOINT" ]]; then
        if [[ "$DRY_RUN" == "true" ]]; then
            info "DRY RUN: Would update ConfigMap with custom IPFS endpoint: $CUSTOM_IPFS_ENDPOINT"
            return 0
        fi
        
        info "🔧 Updating ConfigMap with custom IPFS endpoint: $CUSTOM_IPFS_ENDPOINT"
        
        # Create a temporary configmap file with the custom endpoint
        local temp_configmap="/tmp/configmap-custom-ipfs.yaml"
        
        # Replace the IPFS_NODE_URL in the configmap
        sed "s|IPFS_NODE_URL: \"http://ipfs-service:5001\"|IPFS_NODE_URL: \"$CUSTOM_IPFS_ENDPOINT\"|g" configmap.yaml > "$temp_configmap"
        
        # Apply the updated configmap
        kubectl apply -f "$temp_configmap" -n "$NAMESPACE"
        
        # Clean up
        rm -f "$temp_configmap"
        
        success "ConfigMap updated with custom IPFS endpoint"
    fi
}

# Function to create consumers.yaml with conditional IPFS checks
create_consumers_config() {
    if [[ "$DRY_RUN" == "true" ]]; then
        if [[ "$DEPLOY_IPFS" == "false" ]]; then
            info "DRY RUN: Would create consumers.yaml without IPFS service checks"
        else
            info "DRY RUN: Would use consumers.yaml with IPFS service checks"
        fi
        return 0
    fi
    
    local temp_consumers="/tmp/consumers-custom.yaml"
    
    if [[ "$DEPLOY_IPFS" == "false" ]]; then
        info "🔧 Creating consumers config without IPFS service dependency checks..."
        
        # Remove IPFS service checks from init containers
        sed \
            -e 's/ && nc -z ipfs-service 5001//g' \
            consumers.yaml > "$temp_consumers"
        
        success "Consumers config created without IPFS dependency checks"
    else
        info "🔧 Using standard consumers config with IPFS service checks..."
        cp consumers.yaml "$temp_consumers"
        success "Standard consumers config prepared"
    fi
    
    # Apply the appropriate consumers config
    info "📦 Applying consumers configuration..."
    if [[ "$VERBOSE" == "true" ]]; then
        kubectl apply -f "$temp_consumers" -n "$NAMESPACE"
    else
        kubectl apply -f "$temp_consumers" -n "$NAMESPACE" > /dev/null
    fi
    
    # Clean up
    rm -f "$temp_consumers"
    
    success "Consumers configuration applied"
}

# Main deployment script
main() {
    echo -e "${PURPLE}🚀 IPFS Service Validator - Full Deployment${NC}"
    echo -e "${PURPLE}================================================${NC}"
    echo ""
    echo -e "${BLUE}Configuration:${NC}"
    echo "  Namespace: $NAMESPACE"
    echo "  Build Image: $BUILD_IMAGE"
    echo "  Deploy IPFS: $DEPLOY_IPFS"
    if [[ -n "$CUSTOM_IPFS_ENDPOINT" ]]; then
        echo "  Custom IPFS Endpoint: $CUSTOM_IPFS_ENDPOINT"
    fi
    echo "  Timeout: ${WAIT_TIMEOUT}s"
    echo "  Verbose: $VERBOSE"
    echo "  Dry Run: $DRY_RUN"
    echo ""
    
    # Confirm deployment
    if [[ "$DRY_RUN" == "false" ]]; then
        read -p "Proceed with deployment? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            info "Deployment cancelled"
            exit 0
        fi
    fi
    
    # Change to script directory
    cd "$(dirname "$0")"
    
    # Build Docker image if requested
    if [[ "$BUILD_IMAGE" == "true" ]]; then
        log "🏗️  Building Docker image..."
        if [[ "$DRY_RUN" == "false" ]]; then
            cd ..
            docker build -t ipfs-service-validator:latest . || {
                error "Docker build failed"
                exit 1
            }
            cd k8s
        fi
        success "Docker image built"
    fi
    
    # Create namespace if it doesn't exist
    if ! namespace_exists; then
        create_namespace
    else
        info "Using existing namespace: $NAMESPACE"
    fi
    
    log "📦 Phase 1: Core Infrastructure"
    echo "================================"
    
    # 1. Create persistent volumes
    apply_resource "persistent-volumes.yaml" "📁 Creating persistent volumes"
    
    # 2. Apply ConfigMaps and Migrations
    apply_resource "configmap.yaml" "⚙️  Applying application ConfigMap"
    
    # Update ConfigMap with custom IPFS endpoint if provided
    update_configmap_ipfs_endpoint
    
    apply_resource "migrations-configmap.yaml" "📋 Applying database migrations ConfigMap"
    
    log "📦 Phase 2: Database Infrastructure" 
    echo "====================================="
    
    # 3. Deploy PostgreSQL
    apply_resource "postgres.yaml" "🐘 Deploying PostgreSQL with high-scale configuration"
    wait_for_deployment "postgres" "PostgreSQL"
    
    # 4. Run database migrations
    apply_resource "dbmate-job.yaml" "🔄 Running database migrations"
    
    # Wait for migrations to complete
    if [[ "$DRY_RUN" == "false" ]]; then
        info "⏳ Waiting for database migrations to complete..."
        sleep 30  # Give migrations time to start
        
        # Check if migration job completed successfully
        local migration_status
        for i in {1..20}; do  # Check for up to 10 minutes
            migration_status=$(kubectl get jobs dbmate-migrations -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || echo "")
            if [[ "$migration_status" == "True" ]]; then
                success "Database migrations completed"
                break
            fi
            
            if [[ $i -eq 20 ]]; then
                warn "Database migrations taking longer than expected, continuing..."
                break
            fi
            
            sleep 30
        done
    fi
    
    log "📦 Phase 3: Message Queue & Storage"
    echo "===================================="
    
    # 5. Deploy RabbitMQ
    apply_resource "rabbitmq.yaml" "🐰 Deploying RabbitMQ message queue"
    wait_for_deployment "rabbitmq" "RabbitMQ"
    
    # 6. Deploy IPFS
    if [[ "$DEPLOY_IPFS" == "true" ]]; then
        apply_resource "ipfs.yaml" "📦 Deploying IPFS storage nodes"
        wait_for_statefulset "ipfs" "IPFS StatefulSet"
        
        # 7. Deploy IPFS HPA
        apply_resource "ipfs-hpa.yaml" "📈 Deploying IPFS auto-scaling"
    else
        if [[ "$DRY_RUN" == "true" ]]; then
            info "DRY RUN: Would skip IPFS deployment (--no-ipfs specified)"
        else
            info "⏭️  Skipping IPFS deployment (--no-ipfs specified)"
        fi
    fi
    
    log "📦 Phase 4: Processing Infrastructure"
    echo "======================================"
    
    # Create and apply consumers config (with or without IPFS checks)
    create_consumers_config
    
    # Wait for key consumers to be ready
    wait_for_deployment "user-profile-consumer" "User Profile Consumer"
    wait_for_deployment "miner-profile-reconstruction-consumer" "Miner Profile Consumer"
    wait_for_deployment "file-assignment-consumer" "File Assignment Consumer"
    
    log "📦 Phase 5: Job Processors"
    echo "=========================="
    
    # 9. Deploy job processors
    apply_resource "miner-health-processor-job.yaml" "🏥 Deploying miner health processor"
    apply_resource "epoch-health-job.yaml" "📊 Deploying epoch health processor"
    apply_resource "file-assignment-processor-job.yaml" "📋 Deploying file assignment processor"
    apply_resource "pinning-file-processor-job.yaml" "📌 Deploying pinning file processor"
    apply_resource "miner-profile-reconstruction-job.yaml" "⛏️  Deploying miner profile reconstruction"
    apply_resource "user-profile-reconstruction-job.yaml" "👥 Deploying user profile reconstruction"
    
    log "📦 Phase 6: Orchestrator"
    echo "========================"
    
    # 10. Deploy the main epoch orchestrator
    apply_resource "epoch-orchestrator.yaml" "🎭 Deploying epoch orchestrator"
    wait_for_deployment "epoch-orchestrator" "Epoch Orchestrator"
    
    log "📦 Phase 7: Network Services"
    echo "============================"
    
    # 11. Deploy network services (if needed)
    if [[ -f "nodeports.yaml" ]]; then
        apply_resource "nodeports.yaml" "🌐 Deploying network services"
    fi
    
    # Final status check
    if [[ "$DRY_RUN" == "false" ]]; then
        log "🔍 Final Status Check"
        echo "===================="
        
        info "⏳ Waiting for all deployments to be fully ready..."
        kubectl wait --for=condition=available deployment --all -n "$NAMESPACE" --timeout=600s || {
            warn "Some deployments may still be starting up"
        }
        
        echo ""
        echo -e "${BLUE}📊 Deployment Status in namespace '$NAMESPACE':${NC}"
        kubectl get all -n "$NAMESPACE"
        
        echo ""
        echo -e "${GREEN}🎉 Infrastructure Summary:${NC}"
        echo "  ✅ PostgreSQL: High-scale configuration (1000 connections)"
        echo "  ✅ RabbitMQ: Message queue for distributed processing"
        if [[ "$DEPLOY_IPFS" == "true" ]]; then
            echo "  ✅ IPFS: Distributed storage with auto-scaling"
        elif [[ -n "$CUSTOM_IPFS_ENDPOINT" ]]; then
            echo "  ✅ IPFS: External service at $CUSTOM_IPFS_ENDPOINT"
        else
            echo "  ⏭️  IPFS: Skipped (using external IPFS service)"
        fi
        echo "  ✅ Consumers: 9+ background processors"
        echo "  ✅ Orchestrator: Main epoch coordination"
        echo "  ✅ Jobs: Health checks and profile reconstruction"
        
        echo ""
        echo -e "${BLUE}💡 Useful Commands:${NC}"
        echo "  # Check all resources in namespace:"
        echo "  kubectl get all -n $NAMESPACE"
        echo ""
        echo "  # Check orchestrator logs:"
        echo "  kubectl logs -l app=epoch-orchestrator -n $NAMESPACE --tail=50"
        echo ""
        echo "  # Check PostgreSQL status:"
        echo "  kubectl exec -it deployment/postgres -n $NAMESPACE -- psql -U user -d substrate_fetcher -c \"SELECT version();\""
        echo ""
        echo "  # Monitor all pod status:"
        echo "  kubectl get pods -n $NAMESPACE -w"
        echo ""
        if [[ "$DEPLOY_IPFS" == "true" ]]; then
            echo "  # Scale IPFS if needed:"
            echo "  kubectl scale statefulset ipfs --replicas=3 -n $NAMESPACE"
        elif [[ -n "$CUSTOM_IPFS_ENDPOINT" ]]; then
            echo "  # Test custom IPFS endpoint:"
            echo "  curl $CUSTOM_IPFS_ENDPOINT/api/v0/id"
            echo ""
            echo "  # Test from within cluster:"
            echo "  kubectl run -it --rm test-ipfs --image=curlimages/curl --restart=Never -n $NAMESPACE -- curl $CUSTOM_IPFS_ENDPOINT/api/v0/id"
        else
            echo "  # Note: IPFS was skipped - configure external IPFS service"
        fi
        
        echo ""
        echo -e "${CYAN}🚀 Deployment completed successfully in namespace '$NAMESPACE'!${NC}"
        
        # Check for any failed pods
        local failed_pods
        failed_pods=$(kubectl get pods -n "$NAMESPACE" --field-selector=status.phase=Failed --no-headers 2>/dev/null | wc -l)
        if [[ "$failed_pods" -gt 0 ]]; then
            warn "$failed_pods pod(s) are in failed state. Check with: kubectl get pods -n $NAMESPACE"
        fi
    else
        success "Dry run completed - all resources validated"
    fi
}

# Run main function
main "$@" 