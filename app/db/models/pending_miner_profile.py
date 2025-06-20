from datetime import datetime
from typing import Optional, List
from app.db.connection import get_db_pool


class PendingMinerProfile:
    """Model for pending miner profile records"""

    def __init__(
            self,
            id: Optional[int] = None,
            cid: Optional[str] = None,
            node_id: Optional[str] = None,
            created_at: Optional[datetime] = None,
            published_at: Optional[datetime] = None,
            status: Optional[str] = 'pending',
            error_message: Optional[str] = None,
            files_count: Optional[int] = 0,
            files_size: Optional[int] = 0,
            block_number: Optional[int] = 0
    ):
        self.id = id
        self.cid = cid
        self.node_id = node_id
        self.created_at = created_at
        self.published_at = published_at
        self.status = status
        self.error_message = error_message
        self.files_count = files_count
        self.files_size = files_size
        self.block_number = block_number

    @classmethod
    async def create(cls, cid: str, node_id: str, files_count: int = 0, files_size: int = 0,
                     block_number: int = 0) -> 'PendingMinerProfile':
        """Create a new pending miner profile record"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO pending_miner_profile (cid, node_id, files_count, files_size, block_number)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
                """,
                cid, node_id, files_count, files_size, block_number
            )
            return cls(**dict(row))

    @classmethod
    async def get_by_cid(cls, cid: str) -> Optional['PendingMinerProfile']:
        """Get a pending miner profile by CID"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM pending_miner_profile WHERE cid = $1",
                cid
            )
            return cls(**dict(row)) if row else None

    @classmethod
    async def get_by_node_id(cls, node_id: str) -> Optional['PendingMinerProfile']:
        """Get a pending miner profile by node_id"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM pending_miner_profile WHERE node_id = $1",
                node_id
            )
            return cls(**dict(row)) if row else None

    @classmethod
    async def get_pending(cls, limit: int = 100) -> List['PendingMinerProfile']:
        """Get pending profiles that need to be published"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM pending_miner_profile 
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT $1
                """,
                limit
            )
            return [cls(**dict(row)) for row in rows]

    async def mark_published(self) -> None:
        """Mark the profile as published"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pending_miner_profile 
                SET status = 'published', published_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                self.id
            )
            self.status = 'published'
            self.published_at = datetime.utcnow()

    async def mark_failed(self, error_message: str) -> None:
        """Mark the profile as failed with an error message"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pending_miner_profile 
                SET status = 'failed', error_message = $2
                WHERE id = $1
                """,
                self.id, error_message
            )
            self.status = 'failed'
            self.error_message = error_message 