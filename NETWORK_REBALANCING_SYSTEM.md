# Network Rebalancing System

## 🎯 **Overview**

The Network Rebalancing System ensures fair distribution of files across storage miners by analyzing both **file count** and **storage size** distribution. It automatically identifies overloaded miners and redistributes files to underutilized miners for optimal cluster utilization.

**🔄 INTEGRATED APPROACH**: Rebalancing is now seamlessly integrated into the **validator workflow** as part of the file assignment phase, ensuring automatic load balancing during regular validator operations.

## 📊 **Current Network Status**

**Analysis Results (Latest):**
- **Total miners**: 484 active miners
- **Miners with files**: 317 miners
- **Balance score**: 37.2/100 (poor distribution)
- **Overloaded miners**: 19 miners with 4-5 files (vs 1.8 average)
- **File size imbalance**: Some miners handle 214.8MB vs 6.4MB average

**❌ Issues Identified:**
- Significant file count imbalance (some miners have 3x more files)
- Storage size concentration (large files clustered on few miners)
- Poor overall balance score indicates need for rebalancing

## 🔧 **System Components**

### 1. **Network Rebalancing Processor** (`rabbitmq/network_rebalancing_processor.py`)

**Features:**
- **Dual-criteria analysis**: Both file count AND storage size distribution
- **Statistical imbalance detection**: Uses standard deviations to identify outliers
- **Capacity-aware selection**: Respects miner storage limits and health scores
- **Gradual rebalancing**: Moves files incrementally to avoid disruption
- **Safety limits**: Max moves per miner to prevent excessive changes

**Configuration:**
```bash
REBALANCE_MAX_FILES_PER_BATCH=20           # Files to move per run
REBALANCE_FILE_COUNT_THRESHOLD=2.0         # File count imbalance threshold (std dev)
REBALANCE_SIZE_THRESHOLD=1.5               # Size imbalance threshold (std dev)
REBALANCE_MAX_UTILIZATION=0.85             # Max storage utilization (85%)
REBALANCE_MIN_AVAILABLE_MB=1000            # Minimum free space (1GB)
REBALANCE_MAX_MOVES_PER_MINER=3            # Max files moved from one miner
REBALANCE_INTERVAL_HOURS=6                 # Hours between rebalancing runs
```

### 2. **Enhanced File Assignment Consumer** (`rabbitmq/file_assignment_consumer.py`)

**New capability**: Handles `rebalancing` message type to move files between miners while maintaining assignment integrity and updating miner statistics.

### 3. **Network Balance Analyzer** (`scripts/analyze_network_balance.py`)

**Analysis features:**
- Comprehensive distribution statistics
- Imbalance detection and scoring
- Specific recommendations for rebalancing
- Balance score calculation (0-100, higher = better)

**Usage:**
```bash
python scripts/analyze_network_balance.py --verbose
python scripts/analyze_network_balance.py --threshold-multiplier 1.5
```

### 4. **Orchestrator Integration** (`epoch_orchestrator.py`)

**🎯 NEW: Seamless Integration**
- **Automatic rebalancing** during validator file assignment phase (blocks 36-60)
- **No separate scheduling** needed - runs as part of normal validator workflow
- **Health-aware timing** - only runs after health checks provide fresh miner data
- **Zero-downtime** - integrated into existing validator operations

## 🚀 **Deployment & Usage**

### **Integrated Validator Workflow** ✅ **RECOMMENDED**

Rebalancing now runs **automatically** as part of the validator workflow:

```bash
# 1. Build and deploy updated orchestrator
docker build -t registry.starkleytech.com/library/ipfs-service-validator:latest .
docker push registry.starkleytech.com/library/ipfs-service-validator:latest

# 2. Restart validator pods to use updated image
kubectl rollout restart deployment epoch-orchestrator

# 3. Rebalancing now runs automatically during validator phases!
# - Phase 2: Health checks (blocks 6-40) ✅
# - Phase 3: File assignment + Rebalancing (blocks 36-60) ✅
```

### **Manual One-time Rebalancing** (if needed)

For immediate rebalancing outside validator workflow:

```bash
# Run standalone rebalancing job
kubectl apply -f k8s/network-rebalancing-job.yaml
kubectl logs job/network-rebalancing-processor -f
```

## 📈 **Expected Results**

**For your current network** (19 overloaded miners):
- **Files to move**: ~20 files from overloaded to underutilized miners
- **Processing time**: 5-10 minutes during validator file assignment phase
- **Balance improvement**: Should improve from 37.2/100 to 60+ score
- **Load distribution**: Reduce max files per miner from 5 to 3-4
- **Frequency**: Automatic rebalancing every time node acts as validator

## 🔍 **How It Works**

### **Integrated Workflow:**

**Validator Phase Sequence:**
1. **Phase 1**: Initialization (blocks 0-15)
2. **Phase 2**: Health Checks (blocks 6-40) + **Pinning Request Processing** 📌
3. **Phase 3**: File Assignment (blocks 36-60) + **Network Rebalancing** 🔄
4. **Phase 4**: Profile Reconstruction (blocks 61-75)
5. **Phase 5**: Blockchain Submission (blocks 76-90)

**Rebalancing Integration:**
- Runs **automatically** after file assignment completes
- Uses **fresh health data** from Phase 2
- Leverages **existing RabbitMQ infrastructure**
- **Non-blocking** - doesn't delay other validator operations

### **Selection Criteria:**

**Overloaded Miners** (any condition triggers):
- File count > (average + 2.0 × std deviation)
- Storage size > (average + 1.5 × std deviation)  
- Storage utilization > 85%

**Target Miners** (all conditions required):
- File count < (average - 1.5 × std deviation)
- Storage size < (average - 1.5 × std deviation)
- Storage utilization < 50%
- Available space > 1GB
- Health score ≥ 70%

### **File Priority** (higher = moved first):
- **Large files** (>100MB): Priority 10.0
- **Medium files** (>10MB): Priority 5.0
- **Small files**: Priority 1.0

## 🛡️ **Safety Features**

- **Validator-only execution**: Only runs when node is validator
- **Health-gated**: Only runs after fresh health checks
- **Interval control**: Respects rebalancing intervals (6h by default)
- **Batch limits**: Max 20 files moved per validator cycle
- **Per-miner limits**: Max 3 files moved from any single miner
- **Capacity validation**: Ensures target miners have sufficient space
- **Transaction safety**: All updates in database transactions

## 📊 **Monitoring**

### **Check current balance:**
```bash
python scripts/analyze_network_balance.py
```

### **Monitor validator logs:**
```bash
# Watch validator orchestrator logs
kubectl logs -l app=epoch-orchestrator -f

# Look for rebalancing activity:
# "🔄 Running network rebalancing as part of validator file assignment..."
# "✅ Network rebalancing completed successfully"
```

### **Monitor rebalancing jobs:**
```bash
# Check recent rebalancing activity
kubectl get jobs -l app=network-rebalancing-processor

# View rebalancing logs
kubectl logs job/network-rebalancing-processor

# Check queue status
kubectl exec rabbitmq-xxx -- rabbitmqctl list_queues
```

### **Database tracking:**
```sql
-- Check system events for rebalancing activity
SELECT * FROM system_events WHERE event_type = 'network_rebalancing' ORDER BY created_at DESC;

-- Verify file assignments
SELECT COUNT(*) FROM file_assignments WHERE miner1 IS NOT NULL;
```

## 🎉 **Benefits**

1. **Seamless Integration**: No separate scheduling or monitoring needed
2. **Automatic Operation**: Runs whenever node acts as validator
3. **Health-Aware**: Always uses fresh miner health data
4. **Validator-Optimized**: Leverages existing validator infrastructure
5. **Production-Ready**: Built into robust orchestrator framework
6. **Zero Downtime**: Non-disruptive to normal validator operations

## 🔄 **Integration Details**

### **Orchestrator Workflow Changes:**

**Phase 2 Enhancement** (Health Checks):
- ✅ Health checks
- 🆕 **Pinning request processing** (fixes new files not being processed!)
- ✅ Network self-healing

**Phase 3 Enhancement** (File Assignment):
- ✅ File assignment (including NULL miner fixes)
- 🆕 **Network rebalancing** (integrated load balancing)
- ✅ Assignment verification

### **No External Dependencies:**
- Reuses existing RabbitMQ queues
- Uses existing file assignment consumer
- Leverages existing health check infrastructure
- Maintains existing safety mechanisms

## 📋 **Fixed Issues**

### **🚨 Critical Fixes Applied:**

1. **✅ Pin Requests Not Processing**:
   - **Root Cause**: `process_pinning_requests()` existed but was never called
   - **Fix**: Added to validator Phase 2 workflow after health checks
   - **Result**: New files now get processed and assigned miners

2. **✅ Rebalancing Integration**:
   - **Old Approach**: Separate CronJob (removed)
   - **New Approach**: Integrated into validator file assignment phase
   - **Result**: Automatic rebalancing during normal validator operations

3. **✅ Health-Aware Timing**:
   - **Issue**: Rebalancing without fresh health data
   - **Fix**: Only runs after Phase 2 health checks complete
   - **Result**: Always uses current miner health scores

## 📋 **Next Steps**

1. **Deploy the updated orchestrator** with integrated rebalancing and pinning fix
2. **Monitor validator logs** for automatic rebalancing activity
3. **Check balance scores** regularly with the analysis script
4. **Verify new files** are being processed via pinning requests
5. **No manual scheduling** needed - everything runs automatically!

This system now provides **seamless, automatic network rebalancing** as part of normal validator operations, ensuring optimal file distribution without any manual intervention! 🎯 