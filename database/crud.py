from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import update
from typing import Any

import sys
import os

# Adjust path to import models
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from database.models import Miner, User, ContentItem, IndexerState # miner_content_association, user_content_association are implicitly handled via relationships

# --- Indexer State CRUD ---
async def get_indexer_state(db: AsyncSession) -> IndexerState | None:
    result = await db.execute(select(IndexerState).filter(IndexerState.id == 1))
    return result.scalars().first()

async def update_indexer_state(db: AsyncSession, block_hash: str, block_number: int) -> IndexerState:
    state = await get_indexer_state(db)
    if state:
        state.last_processed_block_hash = block_hash
        state.last_processed_block_number = block_number
    else:
        state = IndexerState(
            id=1, 
            last_processed_block_hash=block_hash, 
            last_processed_block_number=block_number
        )
        db.add(state)
    # The calling function (in main.py) will handle db.commit()
    return state

# --- Miner CRUD --- 

async def get_miner_by_id(db: AsyncSession, miner_id: str, load_pinned_content: bool = False):
    query = select(Miner).filter(Miner.id == miner_id)
    if load_pinned_content:
        query = query.options(selectinload(Miner.pinned_content))
    result = await db.execute(query)
    return result.scalars().first()

async def upsert_miner(db: AsyncSession, miner_id: str, profile_cid_str: str | None, 
                       state_data: Any | None, total_files_pinned: int | None, 
                       total_files_size_bytes: int | None, declared_capacity_bytes: int | None):
    miner = await get_miner_by_id(db, miner_id)
    if miner:
        if profile_cid_str is not None: miner.profile_cid = profile_cid_str
        if state_data is not None: miner.state_data = str(state_data)
        if total_files_pinned is not None: miner.total_files_pinned = total_files_pinned
        if total_files_size_bytes is not None: miner.total_files_size_bytes = total_files_size_bytes
        if declared_capacity_bytes is not None: miner.declared_storage_capacity_bytes = declared_capacity_bytes
        miner.is_active = True 
    else:
        miner = Miner(
            id=miner_id,
            profile_cid=profile_cid_str,
            state_data=str(state_data) if state_data is not None else None,
            total_files_pinned=total_files_pinned,
            total_files_size_bytes=total_files_size_bytes,
            declared_storage_capacity_bytes=declared_capacity_bytes,
            is_active=True 
        )
        db.add(miner)
        print(f"Creating new active miner: {miner_id}")
    return miner

async def deactivate_miners_not_in_list(db: AsyncSession, active_miner_ids_from_chain: set[str]):
    """Deactivates miners in the DB that are not in the provided active list from the chain."""
    # Get all currently active miner IDs from the database
    db_active_miners_query = await db.execute(select(Miner.id).filter(Miner.is_active == True))
    db_active_miner_ids = {m_id for m_id, in db_active_miners_query.fetchall()}

    miners_to_deactivate_ids = db_active_miner_ids - active_miner_ids_from_chain

    if miners_to_deactivate_ids:
        print(f"Deactivating {len(miners_to_deactivate_ids)} miners: {miners_to_deactivate_ids}")
        await db.execute(
            update(Miner).
            where(Miner.id.in_(miners_to_deactivate_ids)).
            values(is_active=False)
        )
    # else:
        # print("No miners to deactivate.")

# --- User CRUD --- 

async def get_user_by_id(db: AsyncSession, user_id: str, load_owned_content: bool = False):
    query = select(User).filter(User.id == user_id)
    if load_owned_content:
        query = query.options(selectinload(User.owned_content))
    result = await db.execute(query)
    return result.scalars().first()

async def upsert_user(db: AsyncSession, user_id: str, profile_cid_str: str | None):
    """Gets a user by ID or creates it if it doesn't exist, then updates its data."""
    user = await get_user_by_id(db, user_id)

    if user:
        if profile_cid_str is not None: user.profile_cid = profile_cid_str
        user.is_active = True # Mark as active if found in chain data
    else:
        user = User(id=user_id, profile_cid=profile_cid_str, is_active=True) # New users are active
        db.add(user)
        print(f"Creating new active user: {user_id}")
    return user

async def deactivate_users_not_in_list(db: AsyncSession, active_user_ids_from_chain: set[str]):
    """Deactivates users in the DB that are not in the provided active list from the chain."""
    db_active_users_query = await db.execute(select(User.id).filter(User.is_active == True))
    db_active_user_ids = {u_id for u_id, in db_active_users_query.fetchall()}

    users_to_deactivate_ids = db_active_user_ids - active_user_ids_from_chain

    if users_to_deactivate_ids:
        print(f"Deactivating {len(users_to_deactivate_ids)} users: {users_to_deactivate_ids}")
        await db.execute(
            update(User).
            where(User.id.in_(users_to_deactivate_ids)).
            values(is_active=False)
        )
    # else:
        # print("No users to deactivate.")

# --- ContentItem (CID) CRUD --- 

async def get_content_item_by_cid(db: AsyncSession, cid: str):
    result = await db.execute(select(ContentItem).filter(ContentItem.cid == cid))
    return result.scalars().first()

async def get_or_create_content_item(db: AsyncSession, cid: str):
    content_item = await get_content_item_by_cid(db, cid)
    if not content_item:
        content_item = ContentItem(cid=cid)
        db.add(content_item)
        # print(f"Creating new content item (CID): {cid}") # Less verbose
    return content_item

# --- Relationship Management (with Diffing) --- 

async def update_miner_pinned_cids(db: AsyncSession, miner_id: str, authoritative_cids_from_chain: list[str]):
    """
    Updates the set of CIDs a miner has pinned based on an authoritative list from the chain.
    Adds new pins and removes pins that are no longer in the authoritative list.
    """
    miner = await get_miner_by_id(db, miner_id, load_pinned_content=True)
    if not miner or not miner.is_active: return
    db_pinned_cids_set = {item.cid for item in miner.pinned_content}
    chain_cids_set = set(authoritative_cids_from_chain)
    cids_to_add_set = chain_cids_set - db_pinned_cids_set
    for cid_str in cids_to_add_set:
        content_item = await get_or_create_content_item(db, cid_str)
        miner.pinned_content.append(content_item)
        print(f"Miner {miner_id} now associated with CID {cid_str}")
    cids_to_remove_set = db_pinned_cids_set - chain_cids_set
    if cids_to_remove_set:
        items_to_remove = [item for item in miner.pinned_content if item.cid in cids_to_remove_set]
        for item_to_remove in items_to_remove:
            miner.pinned_content.remove(item_to_remove)
            print(f"Miner {miner_id} no longer associated with CID {item_to_remove.cid}")

    if not cids_to_add_set and not cids_to_remove_set:
        # print(f"Miner {miner_id} pinned CIDs are already up-to-date.") # Can be too verbose
        pass 

async def update_user_owned_cids(db: AsyncSession, user_id: str, authoritative_cids_from_chain: list[str]):
    """
    Updates the set of CIDs a user owns based on an authoritative list from the chain.
    Adds new ownerships and removes ownerships no longer in the authoritative list.
    """
    user = await get_user_by_id(db, user_id, load_owned_content=True)
    if not user or not user.is_active: return
    db_owned_cids_set = {item.cid for item in user.owned_content}
    chain_cids_set = set(authoritative_cids_from_chain)
    cids_to_add_set = chain_cids_set - db_owned_cids_set
    for cid_str in cids_to_add_set:
        content_item = await get_or_create_content_item(db, cid_str)
        user.owned_content.append(content_item)
        print(f"User {user_id} now associated with CID {cid_str}")
    cids_to_remove_set = db_owned_cids_set - chain_cids_set
    if cids_to_remove_set:
        items_to_remove = [item for item in user.owned_content if item.cid in cids_to_remove_set]
        for item_to_remove in items_to_remove:
            user.owned_content.remove(item_to_remove)
            print(f"User {user_id} no longer associated with CID {item_to_remove.cid}")
    
    if not cids_to_add_set and not cids_to_remove_set:
        # print(f"User {user_id} owned CIDs are already up-to-date.")
        pass

# Consider how to handle items like RebalanceRequest, PinningRequest, UnpinRequests from substrate_storage.md
# If these are transient and don't need to be stored long-term or are reflected in the miner/user CID relationships,
# they might not need dedicated CRUD and tables. If they do, you'd add models and CRUD for them here. 