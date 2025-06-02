#!/usr/bin/env python3
"""
Test Self-Healing Routine

Test script to verify that the automatic network self-healing logic works correctly.
This simulates the self-healing routine that runs during epoch initialization.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SelfHealingTester:
    def __init__(self):
        self.db_pool = None
    
    async def initialize(self):
        """Initialize database connection."""
        try:
            from app.db.connection import init_db_pool, get_db_pool
            await init_db_pool()
            self.db_pool = await get_db_pool()
            logger.info("✅ Database connection initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize database: {e}")
            return False
    
    async def test_health_assessment(self) -> Dict[str, Any]:
        """Test the health assessment logic."""
        logger.info("🔍 Testing health assessment logic...")
        
        try:
            async with self.db_pool.acquire() as conn:
                # Check assignment coverage
                assignment_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                   OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
                        COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                                   AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments
                    FROM file_assignments
                """)
                
                # Check profile coverage
                profile_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_profiles,
                        COUNT(CASE WHEN status = 'published' THEN 1 END) as published_profiles,
                        COUNT(CASE WHEN files_count = 0 OR files_count IS NULL THEN 1 END) as zero_file_profiles
                    FROM pending_user_profile
                """)
                
                total_files = assignment_stats['total_files'] or 0
                files_with_miners = assignment_stats['files_with_miners'] or 0
                empty_assignments = assignment_stats['empty_assignments'] or 0
                zero_file_profiles = profile_stats['zero_file_profiles'] or 0
                
                assignment_coverage = (files_with_miners / total_files * 100) if total_files > 0 else 100
                
                # Determine if healing is needed
                needs_assignment_healing = assignment_coverage < 95 or empty_assignments > 5
                needs_profile_healing = zero_file_profiles > 0
                
                result = {
                    'total_files': total_files,
                    'files_with_miners': files_with_miners,
                    'empty_assignments': empty_assignments,
                    'assignment_coverage': round(assignment_coverage, 1),
                    'zero_file_profiles': zero_file_profiles,
                    'needs_assignment_healing': needs_assignment_healing,
                    'needs_profile_healing': needs_profile_healing,
                    'needs_healing': needs_assignment_healing or needs_profile_healing
                }
                
                logger.info(f"📊 Health Assessment Results:")
                logger.info(f"   Assignment coverage: {result['assignment_coverage']}%")
                logger.info(f"   Empty assignments: {result['empty_assignments']}")
                logger.info(f"   Zero-file profiles: {result['zero_file_profiles']}")
                logger.info(f"   Needs healing: {result['needs_healing']}")
                
                return result
                
        except Exception as e:
            logger.error(f"❌ Error during health assessment test: {e}")
            return {'error': str(e)}
    
    async def test_reliable_miners_selection(self) -> List[Dict[str, Any]]:
        """Test the reliable miners selection logic."""
        logger.info("⛏️ Testing reliable miners selection...")
        
        try:
            async with self.db_pool.acquire() as conn:
                cutoff_date = datetime.now() - timedelta(days=1)  # 1+ day old miners
                
                miners = await conn.fetch("""
                    SELECT 
                        r.node_id,
                        r.ipfs_peer_id,
                        r.registered_at,
                        COALESCE(nm.ipfs_storage_max, 1000000000) as storage_max,
                        COALESCE(nm.ipfs_repo_size, 0) as storage_used,
                        COALESCE(ms.health_score, 100) as health_score
                    FROM registration r
                    LEFT JOIN (
                        SELECT DISTINCT ON (miner_id) 
                            miner_id, ipfs_storage_max, ipfs_repo_size
                        FROM node_metrics 
                        ORDER BY miner_id, block_number DESC
                    ) nm ON r.node_id = nm.miner_id
                    LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                    WHERE r.node_type = 'StorageMiner' 
                      AND r.status = 'active'
                      AND r.registered_at <= $1
                      AND COALESCE(ms.health_score, 100) >= 50
                    ORDER BY RANDOM()
                """, cutoff_date)
                
                # Filter for capacity (10MB minimum available)
                reliable_miners = []
                for miner in miners:
                    available_space = miner['storage_max'] - miner['storage_used']
                    if available_space > 10_000_000:  # At least 10MB available
                        age_days = (datetime.now() - miner['registered_at']).days
                        reliable_miners.append({
                            'node_id': miner['node_id'],
                            'health_score': miner['health_score'],
                            'available_space': available_space,
                            'age_days': age_days
                        })
                
                logger.info(f"✅ Found {len(reliable_miners)} reliable miners out of {len(miners)} total")
                
                # Show sample of reliable miners
                for i, miner in enumerate(reliable_miners[:5]):
                    available_gb = miner['available_space'] / 1_000_000_000
                    logger.info(f"   {i+1}. {miner['node_id'][:20]}: {available_gb:.1f}GB available, "
                               f"health={miner['health_score']}, age={miner['age_days']}d")
                
                return reliable_miners
                
        except Exception as e:
            logger.error(f"❌ Error testing reliable miners selection: {e}")
            return []
    
    async def test_capacity_checking(self, reliable_miners: List[Dict[str, Any]]) -> None:
        """Test the capacity checking logic for different file sizes."""
        logger.info("📏 Testing capacity checking for different file sizes...")
        
        test_file_sizes = [
            1_000_000,    # 1 MB
            10_000_000,   # 10 MB
            100_000_000,  # 100 MB
            500_000_000,  # 500 MB
        ]
        
        for file_size in test_file_sizes:
            # Calculate required space (file + 20% margin)
            safety_margin = int(file_size * 0.2)
            required_space = file_size + safety_margin
            
            # Count suitable miners
            suitable_miners = [
                m for m in reliable_miners 
                if m['available_space'] >= required_space
            ]
            
            logger.info(f"📄 File size {file_size/1_000_000:.0f}MB: "
                       f"{len(suitable_miners)}/{len(reliable_miners)} suitable miners")
    
    async def test_empty_assignments_detection(self) -> List[Dict[str, Any]]:
        """Test detection of files with empty assignments."""
        logger.info("🔍 Testing empty assignments detection...")
        
        try:
            async with self.db_pool.acquire() as conn:
                empty_files = await conn.fetch("""
                    SELECT 
                        fa.cid,
                        fa.owner,
                        f.size,
                        fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5,
                        CASE 
                            WHEN fa.miner1 IS NULL THEN 1 ELSE 0 
                        END +
                        CASE 
                            WHEN fa.miner2 IS NULL THEN 1 ELSE 0 
                        END +
                        CASE 
                            WHEN fa.miner3 IS NULL THEN 1 ELSE 0 
                        END +
                        CASE 
                            WHEN fa.miner4 IS NULL THEN 1 ELSE 0 
                        END +
                        CASE 
                            WHEN fa.miner5 IS NULL THEN 1 ELSE 0 
                        END as empty_slots
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL 
                           OR fa.miner4 IS NULL OR fa.miner5 IS NULL)
                    AND f.size IS NOT NULL
                    ORDER BY f.size ASC
                    LIMIT 20
                """)
                
                logger.info(f"📂 Found {len(empty_files)} files with empty assignments")
                
                for i, file_info in enumerate(empty_files[:5]):
                    size_mb = (file_info['size'] or 0) / 1_000_000
                    logger.info(f"   {i+1}. {file_info['cid'][:16]}... "
                               f"({size_mb:.1f}MB, {file_info['empty_slots']} empty slots)")
                
                return [dict(row) for row in empty_files]
                
        except Exception as e:
            logger.error(f"❌ Error testing empty assignments detection: {e}")
            return []
    
    async def run_full_test(self) -> None:
        """Run the complete self-healing test suite."""
        logger.info("🧪 Starting comprehensive self-healing test")
        logger.info("=" * 60)
        
        # Test 1: Health Assessment
        health_result = await self.test_health_assessment()
        
        # Test 2: Reliable Miners Selection
        reliable_miners = await self.test_reliable_miners_selection()
        
        # Test 3: Capacity Checking
        if reliable_miners:
            await self.test_capacity_checking(reliable_miners)
        
        # Test 4: Empty Assignments Detection
        empty_files = await self.test_empty_assignments_detection()
        
        # Summary
        logger.info("=" * 60)
        logger.info("📊 SELF-HEALING TEST SUMMARY")
        logger.info("=" * 60)
        
        if 'error' in health_result:
            logger.error(f"❌ Health assessment failed: {health_result['error']}")
            return
        
        logger.info(f"🔍 Network Health:")
        logger.info(f"   Assignment coverage: {health_result['assignment_coverage']}%")
        logger.info(f"   Empty assignments: {health_result['empty_assignments']}")
        logger.info(f"   Zero-file profiles: {health_result['zero_file_profiles']}")
        logger.info(f"   Healing needed: {'YES' if health_result['needs_healing'] else 'NO'}")
        
        logger.info(f"⛏️ Miner Availability:")
        logger.info(f"   Reliable miners: {len(reliable_miners)}")
        
        logger.info(f"📂 Assignment Issues:")
        logger.info(f"   Files with empty slots: {len(empty_files)}")
        
        if health_result['needs_healing']:
            logger.info("💡 RECOMMENDATION: Deploy the self-healing update!")
        else:
            logger.info("✅ RESULT: Network is healthy - self-healing will maintain it!")
        
        logger.info("✅ Self-healing test completed!")
    
    async def cleanup(self):
        """Clean up resources."""
        try:
            if self.db_pool:
                from app.db.connection import close_db_pool
                await close_db_pool()
                logger.info("✅ Database connection closed")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")


async def main():
    """Main entry point."""
    tester = SelfHealingTester()
    
    try:
        # Initialize
        success = await tester.initialize()
        if not success:
            logger.error("Failed to initialize tester")
            return 1
        
        # Run full test suite
        await tester.run_full_test()
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Fatal error during self-healing test: {e}")
        logger.exception("Full traceback:")
        return 1
    finally:
        await tester.cleanup()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 