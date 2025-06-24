"""
Model for pending assignment file tracking.
"""

from datetime import datetime
from typing import Optional, List

from app.db.connection import get_db_pool


class PendingAssignmentFile:
    def __init__(self, id: int, cid: str, owner: str, filename: Optional[str] = None,
                 file_size_bytes: Optional[int] = None, created_at: datetime = None,
                 processed_at: Optional[datetime] = None, status: str = 'pending',
                 error_message: Optional[str] = None):
        self.id = id
        self.cid = cid
        self.owner = owner
        self.filename = filename
        self.file_size_bytes = file_size_bytes
        self.created_at = created_at or datetime.now()
        self.processed_at = processed_at
        self.status = status
        self.error_message = error_message

    @classmethod
    async def create(cls, cid: str, owner: str,
                     filename: Optional[str] = None) -> 'PendingAssignmentFile':
        """Create a new pending assignment file record."""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO pending_assignment_file (cid, owner, filename)
                VALUES ($1, $2, $3)
                RETURNING id, cid, owner, filename, file_size_bytes, created_at, processed_at, status, error_message
            """, cid, owner, filename)

            return cls(**dict(row))

    @classmethod
    async def get_by_cid(cls, cid: str) -> Optional['PendingAssignmentFile']:
        """Get a pending assignment file by CID."""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, cid, owner, filename, file_size_bytes, created_at, processed_at, status, error_message
                FROM pending_assignment_file
                WHERE cid = $1
            """, cid)

            if row:
                return cls(**dict(row))
            return None

    @classmethod
    async def get_pending_files(cls, limit: int = 100) -> List['PendingAssignmentFile']:
        """Get pending files that need size processing."""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, cid, owner, filename, file_size_bytes, created_at, processed_at, status, error_message
                FROM pending_assignment_file
                WHERE status = 'pending' AND file_size_bytes IS NULL
                ORDER BY created_at ASC
                LIMIT $1
            """, limit)

            return [cls(**dict(row)) for row in rows]

    async def update_size(self, file_size_bytes: int) -> None:
        """Update the file size and mark as processed. Also updates the main files table."""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. Update the pending assignment file record
                await conn.execute("""
                    UPDATE pending_assignment_file
                    SET file_size_bytes = $1, processed_at = CURRENT_TIMESTAMP, status = 'processed'
                    WHERE id = $2
                """, file_size_bytes, self.id)

                # 2. Upsert into the main 'files' table to ensure it has the correct size
                await conn.execute("""
                    INSERT INTO files (cid, size)
                    VALUES ($1, $2)
                    ON CONFLICT (cid) DO UPDATE SET
                        size = EXCLUDED.size,
                        updated_at = CURRENT_TIMESTAMP
                """, self.cid, file_size_bytes)

            self.file_size_bytes = file_size_bytes
            self.processed_at = datetime.now()
            self.status = 'processed'

    async def mark_failed(self, error_message: str) -> None:
        """Mark the file as failed with an error message."""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE pending_assignment_file
                SET status = 'failed', error_message = $1, processed_at = CURRENT_TIMESTAMP
                WHERE id = $2
            """, error_message, self.id)

            self.status = 'failed'
            self.error_message = error_message
            self.processed_at = datetime.now()

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'cid': self.cid,
            'owner': self.owner,
            'filename': self.filename,
            'file_size_bytes': self.file_size_bytes,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'processed_at': self.processed_at.isoformat() if self.processed_at else None,
            'status': self.status,
            'error_message': self.error_message
        } 