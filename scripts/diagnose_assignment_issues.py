#!/usr/bin/env python3
"""
Diagnose Assignment Issues

Comprehensive diagnostic tool to understand why file assignments still have empty miners
even after the self-healing routine runs.
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


class AssignmentDiagnostics:
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
    
    async def analyze_current_state(self) -> Dict[str, Any]:
        """Analyze the current state of file assignments."""
        logger.info("🔍 Analyzing current assignment state...")
        
        try:
            async with self.db_pool.acquire() as conn:
                # Get assignment statistics
                assignment_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                   OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
                        COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                                   AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as completely_empty,
                        COUNT(CASE WHEN miner1 IS NULL THEN 1 END) as empty_slot1,
                        COUNT(CASE WHEN miner2 IS NULL THEN 1 END) as empty_slot2,
                        COUNT(CASE WHEN miner3 IS NULL THEN 1 END) as empty_slot3,
                        COUNT(CASE WHEN miner4 IS NULL THEN 1 END) as empty_slot4,
                        COUNT(CASE WHEN miner5 IS NULL THEN 1 END) as empty_slot5
                    FROM file_assignments
                """)
                
                # Get file size distribution
                size_stats = await conn.fetchrow("""
                    SELECT 
                        MIN(f.size) as min_size,
                        MAX(f.size) as max_size,
                        AVG(f.size) as avg_size,
                        COUNT(CASE WHEN f.size IS NULL THEN 1 END) as files_without_size
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                """)
                
                return {
                    'assignment_stats': dict(assignment_stats),
                    'size_stats': dict(size_stats)
                }
                
        except Exception as e:
            logger.error(f"❌ Error analyzing current state: {e}")
            return {}
    
    async def analyze_miners(self) -> Dict[str, Any]:
        """Analyze available miners and their capacity."""
        logger.info("⛏️ Analyzing available miners...")
        
        try:
            async with self.db_pool.acquire() as conn:
                # Get miner statistics
                miners = await conn.fetch("""
                    SELECT 
                        r.node_id,
                        r.registered_at,
                        r.status,
                        COALESCE(nm.ipfs_storage_max, 1000000000) as storage_max,
                        COALESCE(nm.ipfs_repo_size, 0) as storage_used,
                        COALESCE(ms.health_score, 100) as health_score,
                        EXTRACT(EPOCH FROM (NOW() - r.registered_at)) / (24 * 3600) as age_days
                    FROM registration r
                    LEFT JOIN (
                        SELECT DISTINCT ON (miner_id) 
                            miner_id, ipfs_storage_max, ipfs_repo_size
                        FROM node_metrics 
                        ORDER BY miner_id, block_number DESC
                    ) nm ON r.node_id = nm.miner_id
                    LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                    WHERE r.node_type = 'StorageMiner'
                    ORDER BY r.status, r.registered_at DESC
                """)
                
                # Categorize miners
                active_miners = [m for m in miners if m['status'] == 'active']
                old_miners = [m for m in active_miners if m['age_days'] >= 1]
                healthy_miners = [m for m in old_miners if m['health_score'] >= 50]
                
                # Calculate capacity
                miners_with_capacity = []
                for miner in healthy_miners:
                    available_space = miner['storage_max'] - miner['storage_used']
                    if available_space > 10_000_000:  # 10MB minimum
                        miners_with_capacity.append({
                            **dict(miner),
                            'available_space': available_space
                        })
                
                return {
                    'total_miners': len(miners),
                    'active_miners': len(active_miners),
                    'old_miners': len(old_miners),
                    'healthy_miners': len(healthy_miners),
                    'miners_with_capacity': len(miners_with_capacity),
                    'miners_sample': miners_with_capacity[:5] if miners_with_capacity else []
                }
                
        except Exception as e:
            logger.error(f"❌ Error analyzing miners: {e}")
            return {}
    
    async def analyze_problematic_files(self) -> List[Dict[str, Any]]:
        """Analyze files that have empty assignments to understand why."""
        logger.info("📂 Analyzing problematic files...")
        
        try:
            async with self.db_pool.acquire() as conn:
                # Get files with empty assignments
                problematic_files = await conn.fetch("""
                    SELECT 
                        fa.cid,
                        fa.owner,
                        f.name,
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
                    ORDER BY f.size DESC NULLS LAST
                    LIMIT 20
                """)
                
                return [dict(row) for row in problematic_files]
                
        except Exception as e:
            logger.error(f"❌ Error analyzing problematic files: {e}")
            return []
    
    async def test_assignment_logic(self, file_info: Dict[str, Any], miners_with_capacity: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Test the assignment logic for a specific file."""
        file_size = file_info['size'] or 0
        empty_slots = file_info['empty_slots']
        
        # Calculate required space (file + 20% margin)
        safety_margin = int(file_size * 0.2)
        required_space = file_size + safety_margin
        
        # Get current assignments
        current_miners = [
            file_info['miner1'], file_info['miner2'], file_info['miner3'],
            file_info['miner4'], file_info['miner5']
        ]
        assigned_miners = [m for m in current_miners if m is not None]
        
        # Find suitable miners
        suitable_miners = [
            m for m in miners_with_capacity 
            if m['node_id'] not in assigned_miners and m['available_space'] >= required_space
        ]
        
        return {
            'file_size': file_size,
            'required_space': required_space,
            'empty_slots': empty_slots,
            'assigned_miners': len(assigned_miners),
            'suitable_miners': len(suitable_miners),
            'can_fix': len(suitable_miners) >= empty_slots
        }
    
    async def run_comprehensive_diagnosis(self) -> None:
        """Run complete diagnosis of assignment issues."""
        logger.info("🧪 Starting comprehensive assignment diagnosis")
        logger.info("=" * 70)
        
        # 1. Analyze current state
        current_state = await self.analyze_current_state()
        
        if current_state and 'assignment_stats' in current_state:
            stats = current_state['assignment_stats']
            size_stats = current_state['size_stats']
            
            total_files = stats['total_files']
            files_with_miners = stats['files_with_miners']
            completely_empty = stats['completely_empty']
            
            logger.info(f"📊 CURRENT STATE:")
            logger.info(f"   Total files: {total_files}")
            logger.info(f"   Files with miners: {files_with_miners}")
            logger.info(f"   Completely empty: {completely_empty}")
            logger.info(f"   Empty slots per position:")
            logger.info(f"     Slot 1: {stats['empty_slot1']}")
            logger.info(f"     Slot 2: {stats['empty_slot2']}")
            logger.info(f"     Slot 3: {stats['empty_slot3']}")
            logger.info(f"     Slot 4: {stats['empty_slot4']}")
            logger.info(f"     Slot 5: {stats['empty_slot5']}")
            
            logger.info(f"📏 FILE SIZES:")
            logger.info(f"   Min size: {size_stats['min_size']:,} bytes" if size_stats['min_size'] else "   Min size: NULL")
            logger.info(f"   Max size: {size_stats['max_size']:,} bytes" if size_stats['max_size'] else "   Max size: NULL")
            logger.info(f"   Avg size: {size_stats['avg_size']:,.0f} bytes" if size_stats['avg_size'] else "   Avg size: NULL")
            logger.info(f"   Files without size: {size_stats['files_without_size']}")
        
        # 2. Analyze miners
        miner_analysis = await self.analyze_miners()
        
        if miner_analysis:
            logger.info(f"⛏️ MINER ANALYSIS:")
            logger.info(f"   Total registered: {miner_analysis['total_miners']}")
            logger.info(f"   Active: {miner_analysis['active_miners']}")
            logger.info(f"   1+ days old: {miner_analysis['old_miners']}")
            logger.info(f"   Healthy (score ≥50): {miner_analysis['healthy_miners']}")
            logger.info(f"   With capacity (≥10MB): {miner_analysis['miners_with_capacity']}")
            
            if miner_analysis['miners_sample']:
                logger.info(f"   Sample miners with capacity:")
                for i, miner in enumerate(miner_analysis['miners_sample']):
                    available_gb = miner['available_space'] / 1_000_000_000
                    logger.info(f"     {i+1}. {miner['node_id'][:20]}: {available_gb:.1f}GB available, "
                               f"health={miner['health_score']:.0f}, age={miner['age_days']:.1f}d")
        
        # 3. Analyze problematic files
        problematic_files = await self.analyze_problematic_files()
        
        if problematic_files:
            logger.info(f"📂 PROBLEMATIC FILES (top 10):")
            
            miners_with_capacity = []
            if miner_analysis and 'miners_sample' in miner_analysis:
                # Get full list for testing
                async with self.db_pool.acquire() as conn:
                    miners = await conn.fetch("""
                        SELECT 
                            r.node_id,
                            COALESCE(nm.ipfs_storage_max, 1000000000) - COALESCE(nm.ipfs_repo_size, 0) as available_space
                        FROM registration r
                        LEFT JOIN (
                            SELECT DISTINCT ON (miner_id) 
                                miner_id, ipfs_storage_max, ipfs_repo_size
                            FROM node_metrics 
                            ORDER BY miner_id, block_number DESC
                        ) nm ON r.node_id = nm.miner_id
                        WHERE r.node_type = 'StorageMiner' 
                          AND r.status = 'active'
                          AND r.registered_at <= NOW() - INTERVAL '1 day'
                          AND COALESCE(nm.ipfs_storage_max, 1000000000) - COALESCE(nm.ipfs_repo_size, 0) > 10000000
                    """)
                    miners_with_capacity = [dict(row) for row in miners]
            
            for i, file_info in enumerate(problematic_files[:10]):
                size_mb = (file_info['size'] or 0) / 1_000_000
                test_result = await self.test_assignment_logic(file_info, miners_with_capacity)
                
                can_fix_icon = "✅" if test_result['can_fix'] else "❌"
                logger.info(f"   {i+1}. {file_info['cid'][:16]}... ({size_mb:.1f}MB, {file_info['empty_slots']} empty) {can_fix_icon}")
                logger.info(f"      Required space: {test_result['required_space']:,} bytes")
                logger.info(f"      Suitable miners: {test_result['suitable_miners']}")
        
        # 4. Summary and recommendations
        logger.info("=" * 70)
        logger.info("💡 DIAGNOSIS SUMMARY:")
        
        if not miner_analysis or miner_analysis['miners_with_capacity'] == 0:
            logger.info("❌ ROOT CAUSE: No miners with sufficient capacity")
            logger.info("   SOLUTIONS:")
            logger.info("   - Wait for miners to free up space")
            logger.info("   - Lower the 10MB minimum capacity requirement")
            logger.info("   - Check if miner metrics are being updated")
        
        elif not current_state or current_state['assignment_stats']['completely_empty'] == 0:
            logger.info("✅ GOOD NEWS: No completely empty assignments")
            logger.info("   Some files just have partial assignments (normal)")
        
        else:
            logger.info("⚠️ MIXED SITUATION: Some miners available but assignments still failing")
            logger.info("   POSSIBLE CAUSES:")
            logger.info("   - Large files requiring more space than available")
            logger.info("   - Self-healing routine not running frequently enough")
            logger.info("   - Assignment logic has bugs")
            
            if problematic_files:
                fixable_files = sum(1 for f in problematic_files[:10] 
                                   if (await self.test_assignment_logic(f, miners_with_capacity))['can_fix'])
                logger.info(f"   Files that should be fixable: {fixable_files}/{min(10, len(problematic_files))}")
    
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
    diagnostics = AssignmentDiagnostics()
    
    try:
        # Initialize
        success = await diagnostics.initialize()
        if not success:
            logger.error("Failed to initialize diagnostics")
            return 1
        
        # Run comprehensive diagnosis
        await diagnostics.run_comprehensive_diagnosis()
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Fatal error during diagnosis: {e}")
        logger.exception("Full traceback:")
        return 1
    finally:
        await diagnostics.cleanup()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 