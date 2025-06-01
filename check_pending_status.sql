-- Check specific file status in pending_assignment_file
SELECT 
    id,
    cid,
    filename,
    owner,
    file_size_bytes,
    status,
    created_at,
    processed_at,
    CASE 
        WHEN processed_at IS NULL THEN 'Never processed'
        ELSE CONCAT('Processed ', AGE(NOW(), processed_at), ' ago')
    END as processing_info
FROM pending_assignment_file 
WHERE cid = 'bafkreifrm5azdkeoyxg5om7eeqfidabraxoecllmm4enkovzj7ber5hvkm';

-- Check all pending assignment files by status
SELECT 
    status,
    COUNT(*) as count,
    MIN(created_at) as oldest_file,
    MAX(created_at) as newest_file
FROM pending_assignment_file
GROUP BY status
ORDER BY status;

-- Check pending files that should be processed but haven't been assigned
SELECT 
    paf.cid,
    paf.filename,
    paf.owner,
    paf.file_size_bytes,
    paf.status,
    paf.created_at,
    paf.processed_at,
    CASE 
        WHEN fa.cid IS NOT NULL THEN 'HAS ASSIGNMENT'
        ELSE 'NO ASSIGNMENT'
    END as assignment_status
FROM pending_assignment_file paf
LEFT JOIN file_assignments fa ON paf.cid = fa.cid
WHERE paf.status = 'processed'
ORDER BY paf.created_at DESC
LIMIT 10;

-- Check if there are any files stuck in 'pending' status
SELECT 
    cid,
    filename,
    owner,
    status,
    created_at,
    AGE(NOW(), created_at) as age
FROM pending_assignment_file
WHERE status != 'processed' AND status != 'assigned'
ORDER BY created_at ASC;

-- Check recent file assignment activity
SELECT 
    'Recent assignments' as activity,
    COUNT(*) as count
FROM file_assignments
WHERE updated_at > NOW() - INTERVAL '1 hour'

UNION ALL

SELECT 
    'Recent pending files' as activity,
    COUNT(*) as count
FROM pending_assignment_file
WHERE created_at > NOW() - INTERVAL '1 hour'

UNION ALL

SELECT 
    'Recent processing' as activity,
    COUNT(*) as count
FROM pending_assignment_file
WHERE processed_at > NOW() - INTERVAL '1 hour'; 