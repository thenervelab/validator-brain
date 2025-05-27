"""Test script for file assignments functionality."""

import asyncio
import os
from datetime import datetime

from dotenv import load_dotenv

from app.db.connection import get_db_pool, init_db_pool, close_db_pool
from app.db.models import FileAssignment

load_dotenv()


async def test_file_assignments():
    """Test file assignment operations."""
    # Initialize database pool
    await init_db_pool()
    pool = get_db_pool()
    
    async with pool.acquire() as conn:
        print("Testing File Assignments")
        print("=" * 60)
        
        # Test data
        test_cid = "bafkreihdcfmbfzxeirzz2ajuyplgjfjmadlav2qa54wycl26n5gmzyzgg4"
        test_owner = "5GdqPwn1Ley9jmxSk52cHFqWKFc864XfaBMffXo1jUtwNmx3"
        test_miners = [
            "12D3KooWKnhGPbTtCgEPWRxGJhtFFcbMTEerfSKMpVnbpLQzByPx",
            "12D3KooWNLehimWAQ1z3AdtmDkUZsUJgJR4RbP5puGgxKtBpp1LK",
            "12D3KooWJ76BveSmtaQh8Y7wjdsdT4GntSGwtXe4qYmuC3NK5TrR",
        ]
        
        # First, ensure the CID exists in the files table
        await conn.execute("""
            INSERT INTO files (cid, name, size, created_date)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (cid) DO NOTHING
        """, test_cid, "test_file.txt", 1024, datetime.utcnow())
        
        # Test 1: Create a new assignment
        print("\n1. Creating new file assignment...")
        assignment = await FileAssignment.create(
            conn,
            cid=test_cid,
            owner=test_owner,
            miners=test_miners
        )
        print(f"   Created assignment ID: {assignment.id}")
        print(f"   CID: {assignment.cid}")
        print(f"   Owner: {assignment.owner}")
        print(f"   Miners: {assignment.get_assigned_miners()}")
        
        # Test 2: Get by CID
        print("\n2. Getting assignment by CID...")
        retrieved = await FileAssignment.get_by_cid(conn, test_cid)
        if retrieved:
            print(f"   Found assignment: {retrieved.to_dict()}")
        else:
            print("   Assignment not found!")
        
        # Test 3: Get by owner
        print("\n3. Getting assignments by owner...")
        owner_assignments = await FileAssignment.get_by_owner(conn, test_owner)
        print(f"   Found {len(owner_assignments)} assignments for owner")
        
        # Test 4: Get by miner
        print("\n4. Getting assignments by miner...")
        miner_assignments = await FileAssignment.get_by_miner(conn, test_miners[0])
        print(f"   Found {len(miner_assignments)} assignments for miner {test_miners[0][:20]}...")
        
        # Test 5: Update miners
        print("\n5. Updating miner assignments...")
        new_miners = test_miners + [
            "12D3KooWJ7tnX13WvUqoMNVzC8Chmtr3N5gzWK52is1myUSbdSs8",
            "12D3KooWLhZVeeDsS8DZwCpTNBzMaqvV2qQ81KktrYnsjSivkA47",
        ]
        await assignment.update_miners(conn, new_miners)
        print(f"   Updated miners: {assignment.get_assigned_miners()}")
        
        # Test 6: Upsert (update existing)
        print("\n6. Testing upsert (update)...")
        upserted = await FileAssignment.upsert(
            conn,
            cid=test_cid,
            owner=test_owner,
            miners=test_miners[:2]  # Only 2 miners this time
        )
        print(f"   Upserted assignment has {len(upserted.get_assigned_miners())} miners")
        
        # Test 7: Create another assignment
        test_cid2 = "bafkreiehp2mweh2v4e3sxsjzra4uxvw7z6weompr6npku5x4k3row7wycm"
        await conn.execute("""
            INSERT INTO files (cid, name, size, created_date)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (cid) DO NOTHING
        """, test_cid2, "test_file2.txt", 2048, datetime.utcnow())
        
        print("\n7. Creating second assignment...")
        assignment2 = await FileAssignment.create(
            conn,
            cid=test_cid2,
            owner=test_owner,
            miners=test_miners[:1]  # Only 1 miner
        )
        print(f"   Created assignment for CID: {assignment2.cid[:20]}...")
        
        # Clean up test data
        print("\n8. Cleaning up test data...")
        await conn.execute("DELETE FROM file_assignments WHERE cid IN ($1, $2)", test_cid, test_cid2)
        await conn.execute("DELETE FROM files WHERE cid IN ($1, $2)", test_cid, test_cid2)
        print("   Test data cleaned up")
        
        print("\n" + "=" * 60)
        print("All tests completed successfully!")
    
    # Close database pool
    await close_db_pool()


if __name__ == "__main__":
    asyncio.run(test_file_assignments()) 