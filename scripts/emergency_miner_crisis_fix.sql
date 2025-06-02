-- EMERGENCY MINER ASSIGNMENT CRISIS DIAGNOSIS
-- Run these queries to understand what happened to all the miner assignments

-- 1. Check the scope of the problem
SELECT 
    'TOTAL FILES' as metric,
    COUNT(*) as count
FROM file_assignments
UNION ALL
SELECT 
    'EMPTY ASSIGNMENTS' as metric,
    COUNT(*) as count
FROM file_assignments
WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL AND miner4 IS NULL AND miner5 IS NULL
UNION ALL
SELECT 
    'PARTIAL ASSIGNMENTS' as metric,
    COUNT(*) as count
FROM file_assignments
WHERE NOT (miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL AND miner4 IS NULL AND miner5 IS NULL)
  AND (miner1 IS NULL OR miner2 IS NULL OR miner3 IS NULL OR miner4 IS NULL OR miner5 IS NULL);

-- 2. Check when the disaster happened (look for the mass update timestamp)
SELECT 
    DATE_TRUNC('hour', updated_at) as update_hour,
    COUNT(*) as files_updated,
    COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as emptied_assignments
FROM file_assignments
WHERE updated_at > NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', updated_at)
ORDER BY update_hour DESC;

-- 3. Check if we have any working miners in the system
SELECT 
    'ACTIVE MINERS' as metric,
    COUNT(*) as count
FROM registration 
WHERE node_type = 'StorageMiner' AND status = 'active'
UNION ALL
SELECT 
    'HEALTHY MINERS' as metric,
    COUNT(*) as count
FROM registration r
JOIN miner_stats ms ON r.node_id = ms.node_id
WHERE r.node_type = 'StorageMiner' 
  AND r.status = 'active'
  AND ms.health_score >= 70;

-- 4. Check recent health check activity (this might be the culprit)
SELECT 
    'RECENT HEALTH CHECKS' as metric,
    COUNT(*) as count
FROM miner_epoch_health
WHERE updated_at > NOW() - INTERVAL '6 hours'
UNION ALL
SELECT 
    'RECENT HEALTH FAILURES' as metric,
    COUNT(*) as count
FROM miner_epoch_health
WHERE updated_at > NOW() - INTERVAL '6 hours'
  AND (ping_failures > 0 OR pin_check_failures > 0);

-- 5. Look for the exact timestamp when everything went wrong
SELECT 
    updated_at,
    COUNT(*) as files_affected,
    'MASS_UPDATE_EVENT' as event_type
FROM file_assignments
WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL AND miner4 IS NULL AND miner5 IS NULL
GROUP BY updated_at
HAVING COUNT(*) > 10  -- Mass updates affecting more than 10 files
ORDER BY updated_at DESC;

-- 6. Check if availability manager logs or health check logs coincide
-- (This would need to be cross-referenced with application logs)

-- 7. Sample of affected files for investigation
SELECT 
    id, cid, owner, 
    created_at, updated_at,
    'File created but miners cleared' as issue
FROM file_assignments
WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL AND miner4 IS NULL AND miner5 IS NULL
ORDER BY updated_at DESC
LIMIT 10;

-- 8. Check if the problem correlates with any particular owners
SELECT 
    owner,
    COUNT(*) as affected_files,
    MIN(updated_at) as first_cleared,
    MAX(updated_at) as last_cleared
FROM file_assignments
WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL AND miner4 IS NULL AND miner5 IS NULL
GROUP BY owner
ORDER BY affected_files DESC;

-- EMERGENCY FIX PREPARATION
-- 9. Check available miners for reassignment
SELECT 
    r.node_id,
    r.status,
    COALESCE(ms.health_score, 0) as health_score,
    COALESCE(nm.ipfs_storage_max, 0) as storage_max,
    COALESCE(nm.ipfs_repo_size, 0) as storage_used,
    (COALESCE(nm.ipfs_storage_max, 0) - COALESCE(nm.ipfs_repo_size, 0)) as available_storage
FROM registration r
LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
LEFT JOIN (
    SELECT DISTINCT ON (miner_id) 
        miner_id, ipfs_storage_max, ipfs_repo_size
    FROM node_metrics 
    ORDER BY miner_id, block_number DESC
) nm ON r.node_id = nm.miner_id
WHERE r.node_type = 'StorageMiner' 
  AND r.status = 'active'
  AND COALESCE(ms.health_score, 100) >= 50  -- Lower threshold for emergency
ORDER BY COALESCE(ms.health_score, 100) DESC, available_storage DESC
LIMIT 20; 