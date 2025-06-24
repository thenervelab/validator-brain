"""
Model for pending user profile reconstruction tracking.
"""

from datetime import datetime
from typing import Optional, List

from app.db.connection import get_db_pool


class PendingUserProfile:
    def __init__(self, id: int, cid: str, owner: str, created_at: datetime,
                 published_at: Optional[datetime] = None, status: str = 'pending',
                 error_message: Optional[str] = None, files_count: int = 0,
                 files_size: int = 0, block_number: int = 0):
        self.id = id
        self.cid = cid
        self.owner = owner
        self.created_at = created_at
        self.published_at = published_at
        self.status = status
        self.error_message = error_message
        self.files_count = files_count
        self.files_size = files_size
        self.block_number = block_number

    @classmethod
    async def create(cls, cid: str, owner: str, files_count: int = 0,
                     files_size: int = 0, block_number: int = 0) -> 'PendingUserProfile':
        """Create a new pending user profile record"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO pending_user_profile (cid, owner, files_count, files_size, block_number)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
                """,
                cid, owner, files_count, files_size, block_number
            )
            return cls(**dict(row))

    @classmethod
    async def get_by_id(cls, profile_id: int) -> Optional['PendingUserProfile']:
        """Get a pending user profile by ID"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM pending_user_profile WHERE id = $1",
                profile_id
            )
            if row:
                return cls(**dict(row))
            return None

    @classmethod
    async def get_by_cid(cls, cid: str) -> Optional['PendingUserProfile']:
        """Get a pending user profile by CID"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM pending_user_profile WHERE cid = $1",
                cid
            )
            if row:
                return cls(**dict(row))
            return None

    @classmethod
    async def get_by_owner(cls, owner: str) -> Optional['PendingUserProfile']:
        """Get a pending user profile by owner"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM pending_user_profile WHERE owner = $1 ORDER BY created_at DESC LIMIT 1",
                owner
            )
            if row:
                return cls(**dict(row))
            return None

    @classmethod
    async def get_all(cls, status: Optional[str] = None, limit: int = 100) -> List[
        'PendingUserProfile']:
        """Get all pending user profiles, optionally filtered by status"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    "SELECT * FROM pending_user_profile WHERE status = $1 ORDER BY created_at DESC LIMIT $2",
                    status, limit
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM pending_user_profile ORDER BY created_at DESC LIMIT $1",
                    limit
                )
            return [cls(**dict(row)) for row in rows]

    async def mark_published(self) -> None:
        """Mark the profile as published"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pending_user_profile 
                SET status = 'published', published_at = CURRENT_TIMESTAMP 
                WHERE id = $1
                """,
                self.id
            )
        self.status = 'published'
        self.published_at = datetime.now()

    async def mark_failed(self, error_message: str) -> None:
        """Mark the profile as failed with error message"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pending_user_profile 
                SET status = 'failed', error_message = $1 
                WHERE id = $2
                """,
                error_message, self.id
            )
        self.status = 'failed'
        self.error_message = error_message

    async def update_cid(self, new_cid: str) -> None:
        """Update the CID of the profile"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE pending_user_profile SET cid = $1 WHERE id = $2",
                new_cid, self.id
            )
        self.cid = new_cid

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'cid': self.cid,
            'owner': self.owner,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'published_at': self.published_at.isoformat() if self.published_at else None,
            'status': self.status,
            'error_message': self.error_message,
            'files_count': self.files_count,
            'files_size': self.files_size,
            'block_number': self.block_number
        } 