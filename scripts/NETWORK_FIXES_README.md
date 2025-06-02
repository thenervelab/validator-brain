# Network Assignment & Profile Fixes

This directory contains both **emergency fix scripts** and **integrated solutions** to address file assignment and profile reconstruction issues in the IPFS validator network.

## 🤖 **NEW: Automatic Network Self-Healing**

The network now **automatically heals itself** at the start of each epoch when you are the validator! No manual intervention needed.

### How It Works

**Every epoch start (blocks 0-10) when you're the validator:**

1. **🔍 Health Assessment**: Checks assignment coverage and profile status
2. **🔧 Automatic Fixes**: Fixes empty assignments and rebuilds profiles
3. **✅ Verification**: Confirms healing worked before proceeding
4. **📊 Logging**: Reports healing status in epoch summary

### Self-Healing Criteria

The network self-heals when:
- **Assignment coverage** < 95% OR **empty assignments** > 5
- **User profiles** with 0 files exist

### Self-Healing Actions

1. **Fix Empty Assignments**:
   - Uses 1+ day old miners with 10MB+ available space
   - Checks file size + 20% safety margin for capacity
   - Round-robin distribution for fairness

2. **Rebuild User Profiles**:
   - Rebuilds profiles directly from file assignments
   - Ensures profiles accurately reflect assigned files

### Monitoring Self-Healing

Check epoch logs for self-healing status:
```bash
tail -f logs/epoch_orchestrator.log | grep -E "(self-healing|Self-Healing)"
```

Example log output:
```
🛠️ Starting automatic network self-healing routine
📊 Assessing network health...
📁 Assignment health: 214/232 files have miners (92.2%)
🔧 Fixing empty file assignments...
✅ Fixed 18 empty file assignments
✅ Network self-healing successful!
```

---

## 🚨 Emergency Fixes (Standalone Scripts)

These scripts can be run immediately to fix critical issues:

### Quick Status Check

```bash
# Get a quick overview of network health
python scripts/comprehensive_fix.py --status
```

### Fix Empty Assignments

```bash
# Fix files with no miner assignments
python scripts/simple_reliable_assignment.py --fix-all

# Fix specific file
python scripts/simple_reliable_assignment.py QmXXXXX...

# Check assignment distribution
python scripts/simple_reliable_assignment.py --distribution
```

### Fix Profile Issues

```bash
# Rebuild all user profiles from file assignments
python scripts/simple_profile_fix.py --fix-all

# Check profile status
python scripts/simple_profile_fix.py --status

# Fix specific user profile
python scripts/simple_profile_fix.py 5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY
```

### Comprehensive Fix (Recommended)

```bash
# Run complete fix: assignments + profiles + verification
python scripts/comprehensive_fix.py
```

### Diagnostic Tools

```bash
# Diagnose why profiles show 0 miners
python scripts/diagnose_profile_issue.py

# Emergency crisis diagnosis
psql -f scripts/emergency_miner_crisis_fix.sql
```

---

## 🔧 Integrated Solutions (Main System)

These changes fix the root cause in the epoch orchestrator:

### What Was Changed

1. **Simple File Assignment Processor** (`rabbitmq/simple_file_assignment_processor.py`)
   - Replaces complex scoring with simple distribution
   - Uses 1+ day old miners with capacity
   - Ensures broad network distribution
   - Avoids the complex logic that caused failures

2. **Improved Profile Reconstruction** (`app/utils/blockchain_submission.py`)
   - Rebuilds profiles directly from file assignments
   - Uses simple, reliable logic
   - Eliminates the gap between assignments and profiles

3. **Updated Epoch Orchestrator** (`epoch_orchestrator.py`)
   - Uses `simple_file_assignment_processor.py` instead of complex version
   - Adds profile rebuild step before blockchain submission
   - More robust error handling

### Key Principles of the Fix

- **Simplicity over complexity**: Removed weighted scoring algorithms
- **Reliability over optimization**: Prefer working over "perfectly optimized"
- **Per-file assignment**: No complex batch processing that can fail
- **Broad distribution**: Round-robin style assignment across network
- **Profile reconstruction**: Always rebuild from actual file assignments

---

## 🔄 How It Works

### 1. Simple Assignment Logic

```python
# Old: Complex scoring with many failure points
score = storage_score * 0.40 + file_score * 0.20 + health_score * 0.15 + ...

# New: Simple criteria
reliable_miners = miners.filter(
    age >= 1_day,
    health_score >= 50,
    available_space > 100KB
)
selected = distribute_round_robin(reliable_miners, count=5)
```

### 2. Reliable Profile Reconstruction

```sql
-- Direct rebuild from file assignments
WITH miner_file_assignments AS (
    SELECT miner_id, COUNT(*) as files_count, SUM(size) as files_size
    FROM (all_miner_assignments)
    GROUP BY miner_id
)
-- Build profiles directly from this data
```

### 3. Integrated Flow

```
1. Simple File Assignment → Reliable assignments
2. Profile Reconstruction → Profiles match assignments  
3. Blockchain Submission → Accurate data submitted
```

---

## 📊 Configuration

### Environment Variables

```bash
# Simple assignment settings
export REPLICAS_PER_FILE="5"           # Replicas per file
export MIN_MINER_HEALTH_SCORE="50"     # Lower threshold for reliability

# Use simple processor in orchestrator
# (automatically used with updated orchestrator)
```

### Migration from Complex to Simple

1. **Immediate**: Run emergency fix scripts
2. **Long-term**: Deploy updated orchestrator with simple processor
3. **Verification**: Use status scripts to monitor health

---

## 🐛 Troubleshooting

### "No reliable miners found"

```bash
# Check active miners
psql -c "SELECT COUNT(*) FROM registration WHERE node_type='StorageMiner' AND status='active';"

# Check miner age (need 1+ day old)
psql -c "SELECT COUNT(*) FROM registration WHERE node_type='StorageMiner' AND registered_at <= NOW() - INTERVAL '1 day';"
```

### "User profiles show 0 miners"

```bash
# Diagnose the issue
python scripts/diagnose_profile_issue.py

# Fix profiles directly
python scripts/simple_profile_fix.py --fix-all
```

### "Assignments failing"

```bash
# Check assignment health
python scripts/simple_reliable_assignment.py --distribution

# Fix empty assignments
python scripts/simple_reliable_assignment.py --fix-all
```

---

## 🎯 Success Metrics

After applying fixes, you should see:

- **Assignment Coverage**: >95% of files have miners
- **Profile Coverage**: >95% of assigned files in profiles  
- **Network Distribution**: Assignments spread across miners
- **Zero Empty Assignments**: <5 files without miners

Use the status check to monitor:

```bash
python scripts/comprehensive_fix.py --status
```

Expected output:
```
📊 QUICK STATUS CHECK
==========================================
📁 Files: 214/232 have miners (18 empty)
👤 Profiles: 45 published, 214 files covered  
⛏️ Miners: 67 active

🏥 Health Scores:
   Assignment coverage: 92.2%
   Profile coverage: 100.0%
✅ System is healthy!
```

---

## 🚀 Deployment Strategy

### Phase 1: Emergency Response

1. Run comprehensive fix immediately:
   ```bash
   python scripts/comprehensive_fix.py
   ```

2. Monitor with status checks:
   ```bash
   python scripts/comprehensive_fix.py --status
   ```

### Phase 2: Integrated Solution

1. Deploy updated code:
   - `rabbitmq/simple_file_assignment_processor.py`
   - Updated `epoch_orchestrator.py`
   - Updated `app/utils/blockchain_submission.py`

2. Restart epoch orchestrator

3. Monitor next epoch for proper function

### Phase 3: Maintenance

1. Keep emergency scripts for troubleshooting
2. Use status checks for monitoring
3. Run assignment distribution checks periodically

---

## ⚠️ Important Notes

- **Emergency scripts are safe** - they only fix broken assignments
- **Integrated solution prevents recurrence** - fixes root cause  
- **Always backup database** before running major fixes
- **Test in staging first** if possible
- **Monitor assignment distribution** to ensure fairness

---

## 📝 File Overview

| File | Purpose | When to Use |
|------|---------|-------------|
| `comprehensive_fix.py` | Complete fix + verification | **First response to crisis** |
| `simple_reliable_assignment.py` | Fix empty assignments | Empty assignment issues |
| `simple_profile_fix.py` | Fix profile issues | Profiles showing 0 miners |
| `diagnose_profile_issue.py` | Debug profile problems | Understanding profile issues |
| `simple_file_assignment_processor.py` | Replace complex processor | **Integrate into main system** |
| Updated `blockchain_submission.py` | Reliable profile rebuild | **Integrate into main system** |
| Updated `epoch_orchestrator.py` | Use simple processor | **Integrate into main system** |

**Bold items** are the core integrated solution that prevents future issues. 