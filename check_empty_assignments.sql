-- Check files with empty miner assignments
SELECT 
    fa.cid,
    f.name as filename,
    f.size,
    fa.owner,
    fa.miner1,
    fa.miner2,
    fa.miner3,
    fa.miner4,
    fa.miner5,
    fa.created_at,
    fa.updated_at,
    CASE 
        WHEN fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
             AND fa.miner4 IS NULL AND fa.miner5 IS NULL THEN 'ALL_EMPTY'
        WHEN (fa.miner1 IS NOT NULL) + (fa.miner2 IS NOT NULL) + (fa.miner3 IS NOT NULL) 
             + (fa.miner4 IS NOT NULL) + (fa.miner5 IS NOT NULL) < 5 THEN 'PARTIAL'
        ELSE 'FULL'
    END as assignment_status
FROM file_assignments fa
JOIN files f ON fa.cid = f.cid
WHERE (fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
       AND fa.miner4 IS NULL AND fa.miner5 IS NULL)
   OR (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL 
       OR fa.miner4 IS NULL OR fa.miner5 IS NULL)
ORDER BY fa.updated_at DESC;

-- Count assignment status
SELECT 
    CASE 
        WHEN fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
             AND fa.miner4 IS NULL AND fa.miner5 IS NULL THEN 'ALL_EMPTY'
        WHEN (CASE WHEN fa.miner1 IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN fa.miner2 IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN fa.miner3 IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN fa.miner4 IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN fa.miner5 IS NOT NULL THEN 1 ELSE 0 END) < 5 THEN 'PARTIAL'
        ELSE 'FULL'
    END as assignment_status,
    COUNT(*) as count
FROM file_assignments fa
GROUP BY 
    CASE 
        WHEN fa.miner1 IS NULL AND fa.miner2 IS NULL AND fa.miner3 IS NULL 
             AND fa.miner4 IS NULL AND fa.miner5 IS NULL THEN 'ALL_EMPTY'
        WHEN (CASE WHEN fa.miner1 IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN fa.miner2 IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN fa.miner3 IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN fa.miner4 IS NOT NULL THEN 1 ELSE 0 END +
              CASE WHEN fa.miner5 IS NOT NULL THEN 1 ELSE 0 END) < 5 THEN 'PARTIAL'
        ELSE 'FULL'
    END
ORDER BY assignment_status;

-- Check available miners
SELECT 
    r.node_id,
    r.status as registration_status,
    COALESCE(ms.health_score, 0) as health_score,
    COALESCE(ms.total_files_pinned, 0) as files_pinned,
    COALESCE(nm.ipfs_storage_max, 0) as storage_max,
    COALESCE(nm.ipfs_repo_size, 0) as storage_used
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
ORDER BY COALESCE(ms.health_score, 0) DESC;

-- Check specific problematic file
SELECT 
    'file_assignments' as source,
    fa.cid,
    fa.owner,
    fa.miner1,
    fa.miner2,
    fa.miner3,
    fa.miner4,
    fa.miner5,
    fa.created_at,
    fa.updated_at
FROM file_assignments fa
WHERE fa.cid = 'bafkreifrm5azdkeoyxg5om7eeqfidabraxoecllmm4enkovzj7ber5hvkm'

UNION ALL

SELECT 
    'pending_assignment_file' as source,
    paf.cid,
    paf.owner,
    paf.status,
    paf.filename,
    NULL,
    NULL,
    NULL,
    paf.created_at,
    paf.processed_at
FROM pending_assignment_file paf
WHERE paf.cid = 'bafkreifrm5azdkeoyxg5om7eeqfidabraxoecllmm4enkovzj7ber5hvkm'; 