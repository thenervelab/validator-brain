"""Health monitoring utilities for the validator service.

This module provides utilities for monitoring the health of the validator service,
including periodic health checks and reporting.
"""

import asyncio
import os
import platform
import time
from datetime import datetime
from typing import Any, Dict

from app.utils.logging import logger
from substrate_fetcher.monitoring import PerformanceMetric, record_metric


async def get_system_health() -> Dict[str, Any]:
    """Get system health metrics.

    Returns:
        Dictionary of system health metrics
    """
    health = {
        "timestamp": datetime.now().isoformat(),
        "hostname": platform.node(),
        "os": platform.system(),
        "cpu_count": os.cpu_count(),
        "process_id": os.getpid(),
    }

    # Add memory usage if psutil is available
    try:
        import psutil

        process = psutil.Process()
        memory_info = process.memory_info()

        health.update(
            {
                "memory_usage_mb": memory_info.rss / (1024 * 1024),
                "memory_percent": process.memory_percent(),
                "cpu_percent": process.cpu_percent(interval=0.1),
                "uptime_seconds": time.time() - process.create_time(),
            },
        )

        # System-wide info
        health.update(
            {
                "system_memory_percent": psutil.virtual_memory().percent,
                "system_cpu_percent": psutil.cpu_percent(interval=0.1),
                "disk_usage_percent": psutil.disk_usage("/").percent,
            },
        )
    except ImportError:
        health["psutil_available"] = False
    except Exception as e:
        health["psutil_error"] = str(e)

    return health


class HealthCheck:
    """Health check manager for validator service."""

    def __init__(self, db_pool, interval_seconds: int = 60):
        """Initialize the health check manager."""
        self.db_pool = db_pool
        self.interval_seconds = interval_seconds
        self.is_running = False
        self.health_task = None
        self.start_time = time.time()

    async def start(self):
        """Start the health check manager."""
        if not self.is_running:
            self.is_running = True
            self.health_task = asyncio.create_task(self._periodic_health_check())
            logger.info("Health check manager started")

    async def stop(self):
        """Stop the health check manager."""
        if self.is_running:
            self.is_running = False
            if self.health_task:
                self.health_task.cancel()
                try:
                    await self.health_task
                except asyncio.CancelledError:
                    pass
            logger.info("Health check manager stopped")

    async def _periodic_health_check(self):
        """Periodically run health checks."""
        while self.is_running:
            try:
                await self._run_health_check()
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check: {e}")
                await asyncio.sleep(10)  # Shorter delay on error

    async def _run_health_check(self):
        """Run a health check on the validator service."""
        # Check uptime
        uptime_seconds = time.time() - self.start_time
        await record_metric(
            PerformanceMetric(name="validator_uptime", value=uptime_seconds, unit="seconds"),
        )

        # Check database connectivity
        start_db = time.time()
        try:
            async with self.db_pool.acquire() as conn:
                # Simple query to test connectivity
                await conn.fetchval("SELECT 1")
                db_latency = time.time() - start_db
                await record_metric(
                    PerformanceMetric(
                        name="db_connection_latency",
                        value=db_latency * 1000,  # Convert to ms
                        unit="ms",
                    ),
                )

                # Check database metrics
                await self._check_database_metrics(conn)
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            await record_metric(
                PerformanceMetric(name="db_connection_success", value=0, unit="bool"),
            )

        # Check memory usage
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            await record_metric(
                PerformanceMetric(
                    name="memory_usage",
                    value=memory_info.rss / (1024 * 1024),  # Convert to MB
                    unit="MB",
                ),
            )
        except ImportError:
            logger.warning("psutil not installed, memory check skipped")
        except Exception as e:
            logger.error(f"Memory check failed: {e}")

    async def _check_database_metrics(self, conn):
        """Check database metrics."""
        # Check storage request count
        request_count = await conn.fetchval("SELECT COUNT(*) FROM pending_submissions")
        await record_metric(
            PerformanceMetric(name="pending_submission_count", value=request_count, unit="count"),
        )

        # Check validation metrics
        validation_count = await conn.fetchval("SELECT COUNT(*) FROM validation_metrics")
        await record_metric(
            PerformanceMetric(name="validation_metrics_count", value=validation_count, unit="count"),
        )

        # Check success rate
        success_rate = await conn.fetchval(
            """
            SELECT COALESCE(
                (SELECT COUNT(*) FROM validation_metrics WHERE success = true) /
                NULLIF(COUNT(*)::float, 0),
                0
            )
            FROM validation_metrics
            WHERE created_at > NOW() - INTERVAL '1 hour'
            """,
        )
        await record_metric(
            PerformanceMetric(
                name="validation_success_rate",
                value=success_rate * 100,  # Convert to percentage
                unit="percent",
            ),
        )
