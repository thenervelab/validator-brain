-- Check status of specific file
-- Replace the CID below with the file you want to check
SELECT 'files' as table_name, cid, name, size, created_date 
FROM files 
WHERE cid = 'bafkreifrm5azdkeoyxg5om7eeqfidabraxoecllmm4enkovzj7ber5hvkm'

UNION ALL

SELECT 'file_assignments' as table_name, cid, 
       CONCAT('Owner: ', owner, ', Miners: ', 
              COALESCE(miner1, 'NULL'), ', ', 
              COALESCE(miner2, 'NULL'), ', ', 
              COALESCE(miner3, 'NULL'), ', ', 
              COALESCE(miner4, 'NULL'), ', ', 
              COALESCE(miner5, 'NULL')) as name,
       0 as size, updated_at as created_date
FROM file_assignments 
WHERE cid = 'bafkreifrm5azdkeoyxg5om7eeqfidabraxoecllmm4enkovzj7ber5hvkm'

UNION ALL

SELECT 'pending_assignment_file' as table_name, cid, 
       CONCAT(filename, ' (', status, ')') as name,
       file_size_bytes as size, created_at as created_date
FROM pending_assignment_file 
WHERE cid = 'bafkreifrm5azdkeoyxg5om7eeqfidabraxoecllmm4enkovzj7ber5hvkm';

-- Check all unassigned files (files without assignments)
SELECT 
    f.cid,
    f.name,
    f.size,
    f.created_date,
    'NO ASSIGNMENT' as status
FROM files f
WHERE NOT EXISTS (
    SELECT 1 FROM file_assignments fa WHERE fa.cid = f.cid
)
ORDER BY f.created_date DESC
LIMIT 20;

-- Check pending assignment files that need processing
SELECT 
    cid,
    filename,
    owner,
    file_size_bytes,
    status,
    created_at,
    processed_at
FROM pending_assignment_file
WHERE status = 'processed'
ORDER BY created_at DESC
LIMIT 20;

-- Summary statistics
SELECT 
    'Total files' as metric,
    COUNT(*) as count
FROM files

UNION ALL

SELECT 
    'Files with assignments' as metric,
    COUNT(*) as count
FROM file_assignments

UNION ALL

SELECT 
    'Unassigned files' as metric,
    COUNT(*) as count
FROM files f
WHERE NOT EXISTS (
    SELECT 1 FROM file_assignments fa WHERE fa.cid = f.cid
)

UNION ALL

SELECT 
    'Pending assignment files' as metric,
    COUNT(*) as count
FROM pending_assignment_file
WHERE status = 'processed'; 