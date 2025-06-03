#!/usr/bin/env python3
"""
Network Balance Analysis Script

This script analyzes the current distribution of files across miners to determine
if rebalancing is needed. It provides detailed statistics about:

1. File count distribution across miners
2. Storage size distribution  
3. Storage utilization analysis
4. Recommendations for rebalancing

Usage:
    python scripts/analyze_network_balance.py [--threshold-multiplier 1.5] [--verbose]
"""

import asyncio
import argparse
import logging
import os
import sys
from typing import Dict, List, Any, Tuple

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class NetworkBalanceAnalyzer:
    def __init__(self, threshold_multiplier: float = 1.5):
        self.threshold_multiplier = threshold_multiplier
        self.min_miner_health_score = 70.0
        
    async def get_miner_distribution_stats(self) -> Dict[str, Any]:
        """Get comprehensive miner distribution statistics."""
        async with get_db_pool().acquire() as conn:
            # Get detailed miner statistics
            rows = await conn.fetch("""
                SELECT 
                    r.node_id,
                    r.registered_at,
                    -- File assignments and sizes
                    COUNT(CASE WHEN fa.miner1 = r.node_id THEN 1 END) +
                    COUNT(CASE WHEN fa.miner2 = r.node_id THEN 1 END) +
                    COUNT(CASE WHEN fa.miner3 = r.node_id THEN 1 END) +
                    COUNT(CASE WHEN fa.miner4 = r.node_id THEN 1 END) +
                    COUNT(CASE WHEN fa.miner5 = r.node_id THEN 1 END) as file_count,
                    
                    COALESCE(SUM(CASE WHEN fa.miner1 = r.node_id THEN f.size END), 0) +
                    COALESCE(SUM(CASE WHEN fa.miner2 = r.node_id THEN f.size END), 0) +
                    COALESCE(SUM(CASE WHEN fa.miner3 = r.node_id THEN f.size END), 0) +
                    COALESCE(SUM(CASE WHEN fa.miner4 = r.node_id THEN f.size END), 0) +
                    COALESCE(SUM(CASE WHEN fa.miner5 = r.node_id THEN f.size END), 0) as total_size_bytes,
                    
                    -- Miner capacity and health
                    COALESCE(nm.ipfs_storage_max, 1000000000) as storage_capacity_bytes,
                    COALESCE(nm.ipfs_repo_size, 0) as used_storage_bytes,
                    COALESCE(ms.health_score, 100) as health_score,
                    EXTRACT(EPOCH FROM (NOW() - TO_TIMESTAMP(r.registered_at))) / 86400 as days_since_registration
                    
                FROM registration r
                LEFT JOIN file_assignments fa ON (fa.miner1 = r.node_id OR fa.miner2 = r.node_id OR 
                                                 fa.miner3 = r.node_id OR fa.miner4 = r.node_id OR fa.miner5 = r.node_id)
                LEFT JOIN files f ON fa.cid = f.cid
                LEFT JOIN (
                    SELECT DISTINCT ON (miner_id) 
                        miner_id, ipfs_storage_max, ipfs_repo_size
                    FROM node_metrics 
                    ORDER BY miner_id, block_number DESC
                ) nm ON r.node_id = nm.miner_id
                LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                WHERE r.node_type = 'StorageMiner' 
                  AND r.status = 'active'
                  AND COALESCE(ms.health_score, 100) >= $1
                GROUP BY r.node_id, r.registered_at, nm.ipfs_storage_max, nm.ipfs_repo_size, ms.health_score
                ORDER BY file_count DESC, total_size_bytes DESC
            """, self.min_miner_health_score)
            
            return [dict(row) for row in rows]
    
    def analyze_distribution_balance(self, miners: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze the balance of file and size distribution."""
        if not miners:
            return {}
        
        # Extract data for analysis
        miners_with_files = [m for m in miners if m['file_count'] > 0]
        file_counts = [int(m['file_count']) for m in miners_with_files]
        size_totals = [float(m['total_size_bytes']) for m in miners_with_files]  # Convert to float
        utilizations = []
        
        for m in miners_with_files:
            storage_capacity = float(m['storage_capacity_bytes'])
            used_storage = float(m['used_storage_bytes'])
            if storage_capacity > 0:
                util = used_storage / storage_capacity
                utilizations.append(min(1.0, util))
        
        if not file_counts:
            return {}
        
        # Calculate statistics
        def calc_stats(data):
            if not data:
                return {}
            data_float = [float(x) for x in data]  # Ensure all values are float
            avg = sum(data_float) / len(data_float)
            variance = sum((float(x) - avg) ** 2 for x in data_float) / len(data_float)
            stddev = variance ** 0.5
            return {
                'min': min(data_float),
                'max': max(data_float), 
                'avg': avg,
                'stddev': stddev,
                'median': sorted(data_float)[len(data_float) // 2]
            }
        
        file_stats = calc_stats(file_counts)
        size_stats = calc_stats(size_totals)
        util_stats = calc_stats(utilizations)
        
        # Identify imbalanced miners
        overloaded_miners = []
        underutilized_miners = []
        
        for miner in miners_with_files:
            file_z = (float(miner['file_count']) - file_stats['avg']) / file_stats['stddev'] if file_stats['stddev'] > 0 else 0
            size_z = (float(miner['total_size_bytes']) - size_stats['avg']) / size_stats['stddev'] if size_stats['stddev'] > 0 else 0
            
            storage_capacity = float(miner['storage_capacity_bytes'])
            used_storage = float(miner['used_storage_bytes'])
            storage_util = used_storage / storage_capacity if storage_capacity > 0 else 0
            
            # Determine imbalance
            is_overloaded = (
                file_z > self.threshold_multiplier or 
                size_z > self.threshold_multiplier or 
                storage_util > 0.85
            )
            
            is_underutilized = (
                file_z < -self.threshold_multiplier and
                size_z < -self.threshold_multiplier and
                storage_util < 0.5
            )
            
            if is_overloaded:
                overloaded_miners.append({
                    'miner_id': miner['node_id'],
                    'file_count': int(miner['file_count']),
                    'size_mb': float(miner['total_size_bytes']) / (1024 * 1024),
                    'utilization': storage_util,
                    'file_z_score': file_z,
                    'size_z_score': size_z
                })
            elif is_underutilized:
                underutilized_miners.append({
                    'miner_id': miner['node_id'],
                    'file_count': int(miner['file_count']),
                    'size_mb': float(miner['total_size_bytes']) / (1024 * 1024),
                    'utilization': storage_util,
                    'available_gb': (storage_capacity - used_storage) / (1024 * 1024 * 1024)
                })
        
        return {
            'total_miners': len(miners),
            'miners_with_files': len(miners_with_files),
            'file_stats': file_stats,
            'size_stats': {k: (v / (1024 * 1024)) if k in ['min', 'max', 'avg', 'stddev', 'median'] else v 
                          for k, v in size_stats.items()},  # Convert to MB
            'utilization_stats': util_stats,
            'overloaded_miners': overloaded_miners,
            'underutilized_miners': underutilized_miners,
            'balance_score': self._calculate_balance_score(file_stats, size_stats),
            'rebalancing_needed': len(overloaded_miners) > 0
        }
    
    def _calculate_balance_score(self, file_stats: Dict, size_stats: Dict) -> float:
        """Calculate a balance score from 0-100 (100 = perfectly balanced)."""
        if not file_stats or not size_stats:
            return 0.0
        
        # Calculate coefficient of variation (CV) for file count and size
        file_cv = file_stats['stddev'] / file_stats['avg'] if file_stats['avg'] > 0 else 0
        size_cv = size_stats['stddev'] / size_stats['avg'] if size_stats['avg'] > 0 else 0
        
        # Perfect balance would have CV near 0, poor balance has CV > 1
        # Convert to 0-100 score where lower CV = higher score
        file_score = max(0, 100 - (file_cv * 50))
        size_score = max(0, 100 - (size_cv * 50))
        
        # Combined score
        return (file_score + size_score) / 2
    
    async def run_analysis(self, verbose: bool = False) -> Dict[str, Any]:
        """Run complete network balance analysis."""
        logger.info("🔍 Analyzing Network Distribution Balance...")
        logger.info("=" * 80)
        
        # Get miner data
        miners = await self.get_miner_distribution_stats()
        
        # Analyze balance
        analysis = self.analyze_distribution_balance(miners)
        
        if not analysis:
            logger.info("❌ No data available for analysis")
            return {}
        
        # Print summary
        logger.info(f"📊 Network Overview:")
        logger.info(f"   Total active miners: {analysis['total_miners']}")
        logger.info(f"   Miners with files: {analysis['miners_with_files']}")
        logger.info(f"   Balance score: {analysis['balance_score']:.1f}/100")
        logger.info("")
        
        # File distribution
        fs = analysis['file_stats']
        logger.info(f"📁 File Count Distribution:")
        logger.info(f"   Average: {fs['avg']:.1f} files per miner")
        logger.info(f"   Range: {fs['min']} - {fs['max']} files")
        logger.info(f"   Standard deviation: {fs['stddev']:.1f}")
        logger.info("")
        
        # Size distribution
        ss = analysis['size_stats']
        logger.info(f"💾 Storage Size Distribution:")
        logger.info(f"   Average: {ss['avg']:.1f} MB per miner")
        logger.info(f"   Range: {ss['min']:.1f} - {ss['max']:.1f} MB")
        logger.info(f"   Standard deviation: {ss['stddev']:.1f} MB")
        logger.info("")
        
        # Storage utilization
        us = analysis['utilization_stats']
        logger.info(f"📈 Storage Utilization:")
        logger.info(f"   Average: {us['avg']*100:.1f}%")
        logger.info(f"   Range: {us['min']*100:.1f}% - {us['max']*100:.1f}%")
        logger.info("")
        
        # Imbalanced miners
        overloaded = analysis['overloaded_miners']
        underutilized = analysis['underutilized_miners']
        
        if overloaded:
            logger.info(f"⚠️  Overloaded Miners ({len(overloaded)}):")
            for i, miner in enumerate(overloaded[:10]):  # Show top 10
                logger.info(f"   {i+1}. {miner['miner_id'][:12]}...: "
                           f"{miner['file_count']} files, {miner['size_mb']:.1f}MB, "
                           f"{miner['utilization']*100:.1f}% used")
                if verbose:
                    logger.info(f"      File Z-score: {miner['file_z_score']:.2f}, "
                               f"Size Z-score: {miner['size_z_score']:.2f}")
            logger.info("")
        
        if underutilized:
            logger.info(f"📉 Underutilized Miners ({len(underutilized)}):")
            for i, miner in enumerate(underutilized[:10]):  # Show top 10
                logger.info(f"   {i+1}. {miner['miner_id'][:12]}...: "
                           f"{miner['file_count']} files, {miner['size_mb']:.1f}MB, "
                           f"{miner['available_gb']:.1f}GB available")
            logger.info("")
        
        # Recommendations
        logger.info("🎯 Recommendations:")
        
        if analysis['rebalancing_needed']:
            logger.info(f"   ⚠️  REBALANCING RECOMMENDED")
            logger.info(f"   - {len(overloaded)} miners are overloaded")
            logger.info(f"   - {len(underutilized)} miners are underutilized")
            logger.info(f"   - Balance score: {analysis['balance_score']:.1f}/100")
            logger.info("")
            logger.info("   📋 To run rebalancing:")
            logger.info("   kubectl apply -f k8s/network-rebalancing-job.yaml")
            logger.info("   # Or enable automatic rebalancing:")
            logger.info("   kubectl apply -f k8s/network-rebalancing-cronjob.yaml")
        else:
            logger.info(f"   ✅ Network is well balanced")
            logger.info(f"   - Balance score: {analysis['balance_score']:.1f}/100")
            logger.info(f"   - No immediate rebalancing needed")
        
        logger.info("")
        logger.info("=" * 80)
        
        return analysis


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Analyze network distribution balance')
    parser.add_argument('--threshold-multiplier', type=float, default=1.5, 
                       help='Threshold multiplier for imbalance detection (default: 1.5)')
    parser.add_argument('--verbose', action='store_true', 
                       help='Show verbose output with detailed statistics')
    
    args = parser.parse_args()
    
    analyzer = NetworkBalanceAnalyzer(threshold_multiplier=args.threshold_multiplier)
    
    try:
        # Initialize database
        await init_db_pool()
        
        # Run analysis
        await analyzer.run_analysis(verbose=args.verbose)
        
    except Exception as e:
        logger.error(f"❌ Error during analysis: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 