"""Run the node_metrics migration directly."""

import asyncio
import os
from dotenv import load_dotenv
import asyncpg

# Load environment variables
load_dotenv()

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/substrate_fetcher')


async def run_migration():
    """Run the node_metrics table migration."""
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Read the migration file
        with open('db/migrations/20250527053000_add_node_metrics_table.sql', 'r') as f:
            migration_sql = f.read()
        
        # Extract the up migration part
        up_migration = migration_sql.split('-- migrate:up')[1].split('-- migrate:down')[0].strip()
        
        print("Running migration...")
        print("-" * 60)
        print(up_migration)
        print("-" * 60)
        
        # Execute the migration
        await conn.execute(up_migration)
        
        print("Migration completed successfully!")
        
        # Verify the table exists
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'node_metrics'
            )
        """)
        
        if exists:
            print("✓ Table 'node_metrics' created successfully")
        else:
            print("✗ Table 'node_metrics' was not created")
            
    except Exception as e:
        print(f"Error running migration: {e}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migration()) 