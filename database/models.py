import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Table, DateTime, BigInteger, Boolean
from sqlalchemy.sql import func
import sys
import os

# Adjust path to import config, assuming this file is in database/models.py
# and config.py is in the project root.
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config import DATABASE_URL, ECHO_SQL # Import ECHO_SQL from config

# engine = create_async_engine(DATABASE_URL, echo=True) # echo=True for logging SQL, remove in prod
# Let's make echo configurable, defaulting to False for production
echo_sql = os.getenv("ECHO_SQL", "False").lower() == "true"
engine = create_async_engine(DATABASE_URL, echo=ECHO_SQL)

AsyncSessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

# Association table for Miner and ContentItem (Many-to-Many)
miner_content_association = Table('miner_content_association', Base.metadata,
    Column('miner_id', String, ForeignKey('miners.id'), primary_key=True),
    Column('content_id', String, ForeignKey('content_items.cid'), primary_key=True),
    Column('pinned_at', DateTime(timezone=True), server_default=func.now())
)

# Association table for User and ContentItem (Many-to-Many)
user_content_association = Table('user_content_association', Base.metadata,
    Column('user_id', String, ForeignKey('users.id'), primary_key=True),
    Column('content_id', String, ForeignKey('content_items.cid'), primary_key=True),
    Column('owned_at', DateTime(timezone=True), server_default=func.now())
)

class Miner(Base):
    __tablename__ = "miners"

    id = Column(String, primary_key=True, index=True)  # Assuming miner ID is a string (e.g., account address)
    profile_cid = Column(String, nullable=True) # Changed from profile_data (LargeBinary) to profile_cid (String)
    state_data = Column(String, nullable=True) # Raw state from ipfsPallet.minerStates (adjust type if complex)
    total_files_pinned = Column(Integer, nullable=True)
    total_files_size_bytes = Column(BigInteger, nullable=True) # u128 can be large
    declared_storage_capacity_bytes = Column(BigInteger, nullable=True) # From ExecutionUnit.NodeMetrics
    is_active = Column(Boolean, default=True, nullable=False, index=True)

    # Relationships
    pinned_content = relationship(
        "ContentItem",
        secondary=miner_content_association,
        back_populates="pinned_by_miners"
    )

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, index=True) # Assuming user ID is a string (e.g., account address)
    profile_cid = Column(String, nullable=True) # Changed from profile_data (LargeBinary) to profile_cid (String)
    is_active = Column(Boolean, default=True, nullable=False, index=True)

    # Relationships
    owned_content = relationship(
        "ContentItem",
        secondary=user_content_association,
        back_populates="owned_by_users"
    )

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())

class ContentItem(Base):
    __tablename__ = "content_items"

    cid = Column(String, primary_key=True, index=True) # The IPFS Content Identifier
    # We might add more fields here later, e.g., file size if not implicitly part of miner/user links,
    # or type of content if discernible.

    # Relationships
    pinned_by_miners = relationship(
        "Miner",
        secondary=miner_content_association,
        back_populates="pinned_content"
    )
    owned_by_users = relationship(
        "User",
        secondary=user_content_association,
        back_populates="owned_content"
    )

    first_seen_at = Column(DateTime(timezone=True), server_default=func.now())
    # last_verified_at = Column(DateTime(timezone=True), onupdate=func.now()) # If we implement verification

# --- Indexer State Table ---
class IndexerState(Base):
    __tablename__ = "indexer_state"
    # Using a fixed ID to ensure only one row, or a more descriptive primary key if needed.
    # For simplicity, a single row with id=1 is common for such singleton config tables.
    id = Column(Integer, primary_key=True, default=1, autoincrement=False)
    last_processed_block_hash = Column(String, nullable=True)
    last_processed_block_number = Column(BigInteger, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

# --- Helper function to create tables (call this once during app setup) ---
async def create_db_and_tables():
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all) # Uncomment for development to force drop/recreate
        await conn.run_sync(Base.metadata.create_all)
    print("Database tables created or ensure they already exist.")

# --- Example usage (you would typically run this in your main app startup) ---
# async def main_db_setup():
#     await create_db_and_tables()
#
# if __name__ == "__main__":
#     asyncio.run(main_db_setup()) 