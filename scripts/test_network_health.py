#!/usr/bin/env python3
"""
Network Health Test

Comprehensive test to verify that the network assignment and profile fixes are working correctly.
This script checks all aspects of the system health.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
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


class NetworkHealthTester:
    def __init__(self):
        self.db_pool = None
        self.test_results = {}
        
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
    
    async def test_assignment_coverage(self) -> Dict[str, Any]:
        """Test file assignment coverage."""
        try:
            async with self.db_pool.acquire() as conn:
                stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                   OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
                        COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                                   AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments
                    FROM file_assignments
                """)
                
                coverage_percent = (stats['files_with_miners'] / stats['total_files'] * 100) if stats['total_files'] > 0 else 0
                
                result = {
                    'test_name': 'Assignment Coverage',
                    'total_files': stats['total_files'],
                    'files_with_miners': stats['files_with_miners'],
                    'empty_assignments': stats['empty_assignments'],
                    'coverage_percent': round(coverage_percent, 1),
                    'status': 'PASS' if coverage_percent >= 95 else 'FAIL',
                    'target': '≥95% coverage'
                }
                
                logger.info(f"📁 Assignment Coverage: {result['coverage_percent']}% ({result['status']})")
                return result
                
        except Exception as e:
            logger.error(f"❌ Error testing assignment coverage: {e}")
            return {'test_name': 'Assignment Coverage', 'status': 'ERROR', 'error': str(e)}
    
    async def test_profile_coverage(self) -> Dict[str, Any]:
        """Test user profile coverage."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get assignment stats
                assignment_stats = await conn.fetchrow("""
                    SELECT COUNT(*) as files_with_miners
                    FROM file_assignments
                    WHERE miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                          OR miner4 IS NOT NULL OR miner5 IS NOT NULL
                """)
                
                # Get profile stats
                profile_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_profiles,
                        COUNT(CASE WHEN status = 'published' THEN 1 END) as published_profiles,
                        SUM(files_count) as total_profile_files
                    FROM pending_user_profile
                """)
                
                files_with_miners = assignment_stats['files_with_miners']
                profile_files = profile_stats['total_profile_files'] or 0
                
                coverage_percent = (profile_files / files_with_miners * 100) if files_with_miners > 0 else 0
                
                result = {
                    'test_name': 'Profile Coverage',
                    'files_with_miners': files_with_miners,
                    'profile_files': profile_files,
                    'published_profiles': profile_stats['published_profiles'],
                    'coverage_percent': round(coverage_percent, 1),
                    'status': 'PASS' if coverage_percent >= 95 else 'FAIL',
                    'target': '≥95% coverage'
                }
                
                logger.info(f"👤 Profile Coverage: {result['coverage_percent']}% ({result['status']})")
                return result
                
        except Exception as e:
            logger.error(f"❌ Error testing profile coverage: {e}")
            return {'test_name': 'Profile Coverage', 'status': 'ERROR', 'error': str(e)}
    
    async def test_miner_distribution(self) -> Dict[str, Any]:
        """Test miner assignment distribution fairness."""
        try:
            async with self.db_pool.acquire() as conn:
                distribution_stats = await conn.fetchrow("""
                    WITH miner_assignments AS (
                        SELECT node_id, COUNT(*) as assignment_count
                        FROM (
                            SELECT miner1 as node_id FROM file_assignments WHERE miner1 IS NOT NULL
                            UNION ALL
                            SELECT miner2 as node_id FROM file_assignments WHERE miner2 IS NOT NULL
                            UNION ALL
                            SELECT miner3 as node_id FROM file_assignments WHERE miner3 IS NOT NULL
                            UNION ALL
                            SELECT miner4 as node_id FROM file_assignments WHERE miner4 IS NOT NULL
                            UNION ALL
                            SELECT miner5 as node_id FROM file_assignments WHERE miner5 IS NOT NULL
                        ) assignments
                        GROUP BY node_id
                    )
                    SELECT 
                        COUNT(*) as miners_with_assignments,
                        AVG(assignment_count) as avg_assignments,
                        MIN(assignment_count) as min_assignments,
                        MAX(assignment_count) as max_assignments,
                        STDDEV(assignment_count) as stddev_assignments
                    FROM miner_assignments
                """)
                
                # Check if distribution is reasonable (standard deviation not too high)
                avg_assignments = distribution_stats['avg_assignments'] or 0
                stddev_assignments = distribution_stats['stddev_assignments'] or 0
                
                # Consider distribution good if stddev is less than 50% of average
                distribution_ratio = (stddev_assignments / avg_assignments) if avg_assignments > 0 else 0
                distribution_good = distribution_ratio < 0.5
                
                result = {
                    'test_name': 'Miner Distribution',
                    'miners_with_assignments': distribution_stats['miners_with_assignments'],
                    'avg_assignments': round(avg_assignments, 1),
                    'min_assignments': distribution_stats['min_assignments'],
                    'max_assignments': distribution_stats['max_assignments'],
                    'stddev_assignments': round(stddev_assignments, 1),
                    'distribution_ratio': round(distribution_ratio, 2),
                    'status': 'PASS' if distribution_good else 'FAIL',
                    'target': 'Stddev < 50% of average'
                }
                
                logger.info(f"📊 Miner Distribution: {result['miners_with_assignments']} miners, "
                           f"stddev/avg = {result['distribution_ratio']} ({result['status']})")
                return result
                
        except Exception as e:
            logger.error(f"❌ Error testing miner distribution: {e}")
            return {'test_name': 'Miner Distribution', 'status': 'ERROR', 'error': str(e)}
    
    async def test_miner_availability(self) -> Dict[str, Any]:
        """Test miner availability."""
        try:
            async with self.db_pool.acquire() as conn:
                miner_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_miners,
                        COUNT(CASE WHEN status = 'active' THEN 1 END) as active_miners,
                        COUNT(CASE WHEN status = 'active' AND registered_at <= NOW() - INTERVAL '1 day' THEN 1 END) as reliable_miners
                    FROM registration
                    WHERE node_type = 'StorageMiner'
                """)
                
                active_percent = (miner_stats['active_miners'] / miner_stats['total_miners'] * 100) if miner_stats['total_miners'] > 0 else 0
                reliable_percent = (miner_stats['reliable_miners'] / miner_stats['total_miners'] * 100) if miner_stats['total_miners'] > 0 else 0
                
                result = {
                    'test_name': 'Miner Availability',
                    'total_miners': miner_stats['total_miners'],
                    'active_miners': miner_stats['active_miners'],
                    'reliable_miners': miner_stats['reliable_miners'],
                    'active_percent': round(active_percent, 1),
                    'reliable_percent': round(reliable_percent, 1),
                    'status': 'PASS' if reliable_percent >= 50 else 'FAIL',
                    'target': '≥50% reliable miners'
                }
                
                logger.info(f"⛏️ Miner Availability: {result['reliable_miners']}/{result['total_miners']} reliable "
                           f"({result['reliable_percent']}%) ({result['status']})")
                return result
                
        except Exception as e:
            logger.error(f"❌ Error testing miner availability: {e}")
            return {'test_name': 'Miner Availability', 'status': 'ERROR', 'error': str(e)}
    
    async def test_data_consistency(self) -> Dict[str, Any]:
        """Test data consistency between tables."""
        try:
            async with self.db_pool.acquire() as conn:
                # Check for orphaned assignments
                orphaned_assignments = await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM file_assignments fa
                    LEFT JOIN files f ON fa.cid = f.cid
                    WHERE f.cid IS NULL
                """)
                
                # Check for missing file metadata
                missing_metadata = await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE f.size IS NULL OR f.name IS NULL
                """)
                
                # Check for profile-assignment mismatches
                profile_mismatches = await conn.fetchval("""
                    WITH assignment_counts AS (
                        SELECT 
                            owner,
                            COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                       OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as assigned_files
                        FROM file_assignments
                        GROUP BY owner
                    ),
                    profile_counts AS (
                        SELECT owner, files_count
                        FROM pending_user_profile
                        WHERE status = 'published'
                    )
                    SELECT COUNT(*)
                    FROM assignment_counts ac
                    LEFT JOIN profile_counts pc ON ac.owner = pc.owner
                    WHERE pc.files_count IS NULL OR ABS(ac.assigned_files - pc.files_count) > 0
                """)
                
                issues = orphaned_assignments + missing_metadata + profile_mismatches
                
                result = {
                    'test_name': 'Data Consistency',
                    'orphaned_assignments': orphaned_assignments,
                    'missing_metadata': missing_metadata,
                    'profile_mismatches': profile_mismatches,
                    'total_issues': issues,
                    'status': 'PASS' if issues == 0 else 'FAIL',
                    'target': '0 data inconsistencies'
                }
                
                logger.info(f"🔍 Data Consistency: {result['total_issues']} issues found ({result['status']})")
                return result
                
        except Exception as e:
            logger.error(f"❌ Error testing data consistency: {e}")
            return {'test_name': 'Data Consistency', 'status': 'ERROR', 'error': str(e)}
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all health tests."""
        logger.info("🧪 Starting comprehensive network health tests")
        logger.info("=" * 60)
        
        tests = [
            self.test_assignment_coverage(),
            self.test_profile_coverage(),
            self.test_miner_distribution(),
            self.test_miner_availability(),
            self.test_data_consistency()
        ]
        
        results = []
        for test_coro in tests:
            result = await test_coro
            results.append(result)
        
        # Calculate overall health score
        passed_tests = len([r for r in results if r.get('status') == 'PASS'])
        total_tests = len([r for r in results if r.get('status') in ['PASS', 'FAIL']])
        health_score = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        overall_status = 'HEALTHY' if health_score >= 80 else ('WARNING' if health_score >= 60 else 'CRITICAL')
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'health_score': round(health_score, 1),
            'overall_status': overall_status,
            'passed_tests': passed_tests,
            'total_tests': total_tests,
            'individual_results': results
        }
        
        return summary
    
    def print_test_report(self, summary: Dict[str, Any]):
        """Print a formatted test report."""
        print("\n" + "=" * 60)
        print("🏥 NETWORK HEALTH TEST REPORT")
        print("=" * 60)
        print(f"Timestamp: {summary['timestamp']}")
        print(f"Overall Status: {summary['overall_status']}")
        print(f"Health Score: {summary['health_score']}%")
        print(f"Tests Passed: {summary['passed_tests']}/{summary['total_tests']}")
        print()
        
        # Print individual test results
        for result in summary['individual_results']:
            status_emoji = "✅" if result['status'] == 'PASS' else ("❌" if result['status'] == 'FAIL' else "⚠️")
            print(f"{status_emoji} {result['test_name']}: {result['status']}")
            
            # Print key metrics for each test
            if result['test_name'] == 'Assignment Coverage':
                print(f"   Files: {result['files_with_miners']}/{result['total_files']} have miners ({result['coverage_percent']}%)")
                print(f"   Empty assignments: {result['empty_assignments']}")
                
            elif result['test_name'] == 'Profile Coverage':
                print(f"   Profiles: {result['published_profiles']} published")
                print(f"   Coverage: {result['profile_files']}/{result['files_with_miners']} files ({result['coverage_percent']}%)")
                
            elif result['test_name'] == 'Miner Distribution':
                print(f"   Miners: {result['miners_with_assignments']} with assignments")
                print(f"   Distribution: avg={result['avg_assignments']}, stddev={result['stddev_assignments']}")
                
            elif result['test_name'] == 'Miner Availability':
                print(f"   Miners: {result['reliable_miners']}/{result['total_miners']} reliable ({result['reliable_percent']}%)")
                
            elif result['test_name'] == 'Data Consistency':
                print(f"   Issues: {result['total_issues']} found")
                if result['total_issues'] > 0:
                    print(f"     Orphaned: {result['orphaned_assignments']}, Missing metadata: {result['missing_metadata']}")
                    print(f"     Profile mismatches: {result['profile_mismatches']}")
            
            print(f"   Target: {result.get('target', 'N/A')}")
            print()
        
        # Print recommendations
        print("📋 RECOMMENDATIONS:")
        if summary['health_score'] >= 90:
            print("✅ Network is healthy! Continue monitoring.")
        elif summary['health_score'] >= 70:
            print("⚠️ Network has minor issues. Consider running:")
            print("   python scripts/comprehensive_fix.py")
        else:
            print("❌ Network needs immediate attention! Run:")
            print("   python scripts/comprehensive_fix.py")
            print("   Then retest with: python scripts/test_network_health.py")
        
        print()
    
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
    tester = NetworkHealthTester()
    
    try:
        # Initialize
        success = await tester.initialize()
        if not success:
            logger.error("Failed to initialize tester")
            return 1
        
        # Run all tests
        summary = await tester.run_all_tests()
        
        # Print report
        tester.print_test_report(summary)
        
        # Return appropriate exit code
        return 0 if summary['overall_status'] in ['HEALTHY', 'WARNING'] else 1
        
    except Exception as e:
        logger.error(f"❌ Fatal error during testing: {e}")
        logger.exception("Full traceback:")
        return 1
    finally:
        await tester.cleanup()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 