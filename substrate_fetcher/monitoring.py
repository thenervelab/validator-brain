"""Monitoring system for validator performance metrics.

This module provides a monitoring system to track validator performance,
including success rates, processing times, and other metrics.
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel

from app.utils.logging import logger


class PerformanceMetric(BaseModel):
    """Base model for a performance metric."""

    name: str
    value: float
    timestamp: datetime = datetime.now()
    unit: str = ""
    tags: Dict[str, str] = {}


class ValidationMetrics(BaseModel):
    """Collection of validation performance metrics."""

    validator_id: str
    epoch: int
    block_number: int
    metrics: List[PerformanceMetric] = []
    start_time: datetime = datetime.now()
    end_time: Optional[datetime] = None
    total_duration_ms: int = 0
    phase: str = ""
    success: bool = True
    error_message: Optional[str] = None


class MonitoringSystem:
    """Monitoring system for validator performance."""

    def __init__(self, db_pool):
        """Initialize the monitoring system."""
        self.db_pool = db_pool
        self.metrics_buffer = []
        self.flush_interval_seconds = 60
        self.max_buffer_size = 100
        self.is_running = False
        self.flush_task = None

    async def start(self):
        """Start the monitoring system."""
        if not self.is_running:
            self.is_running = True
            self.flush_task = asyncio.create_task(self._periodic_flush())
            logger.info("Monitoring system started")

    async def stop(self):
        """Stop the monitoring system."""
        if self.is_running:
            self.is_running = False
            if self.flush_task:
                self.flush_task.cancel()
                try:
                    await self.flush_task
                except asyncio.CancelledError:
                    pass
            # Flush remaining metrics
            await self.flush_metrics()
            logger.info("Monitoring system stopped")

    async def _periodic_flush(self):
        """Periodically flush metrics to the database."""
        while self.is_running:
            try:
                await asyncio.sleep(self.flush_interval_seconds)
                await self.flush_metrics()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")

    async def record_metric(self, metric: PerformanceMetric):
        """Record a performance metric."""
        self.metrics_buffer.append(metric)

        # Flush if buffer is full
        if len(self.metrics_buffer) >= self.max_buffer_size:
            await self.flush_metrics()

    async def record_validation_metrics(self, metrics: ValidationMetrics):
        """Record validation metrics."""
        # Add each metric to the buffer
        for metric in metrics.metrics:
            self.metrics_buffer.append(metric)

        # Record the validation event
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO validation_metrics (
                    validator_id, epoch, block_number, phase,
                    start_time, end_time, total_duration_ms,
                    success, error_message
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                metrics.validator_id,
                metrics.epoch,
                metrics.block_number,
                metrics.phase,
                metrics.start_time,
                metrics.end_time or datetime.now(),
                metrics.total_duration_ms,
                metrics.success,
                metrics.error_message,
            )

        # Flush if buffer is full
        if len(self.metrics_buffer) >= self.max_buffer_size:
            await self.flush_metrics()

    async def flush_metrics(self):
        """Flush metrics to the database."""
        if not self.metrics_buffer:
            return

        metrics_to_flush = self.metrics_buffer.copy()
        self.metrics_buffer = []

        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                # Use a more efficient approach for many metrics
                if len(metrics_to_flush) > 100:
                    # Create temporary table
                    await conn.execute(
                        """
                        CREATE TEMP TABLE temp_metrics (
                            name TEXT,
                            value FLOAT,
                            timestamp TIMESTAMP,
                            unit TEXT,
                            tags JSONB
                        )
                        """,
                    )

                    # Insert in batches
                    for i in range(0, len(metrics_to_flush), 100):
                        batch = metrics_to_flush[i : i + 100]
                        values = []
                        for metric in batch:
                            values.append(
                                f"('{metric.name}', {metric.value}, '{metric.timestamp}', "
                                f"'{metric.unit}', '{json.dumps(metric.tags)}'::jsonb)",
                            )

                        await conn.execute(
                            f"""
                            INSERT INTO temp_metrics (name, value, timestamp, unit, tags)
                            VALUES {", ".join(values)}
                            """,
                        )

                    # Insert from temp table to main table
                    await conn.execute(
                        """
                        INSERT INTO performance_metrics (
                            name, value, timestamp, unit, tags
                        )
                        SELECT name, value, timestamp, unit, tags
                        FROM temp_metrics
                        """,
                    )

                    # Drop temp table
                    await conn.execute("DROP TABLE temp_metrics")
                else:
                    # For smaller batches, insert directly
                    for metric in metrics_to_flush:
                        await conn.execute(
                            """
                            INSERT INTO performance_metrics (
                                name, value, timestamp, unit, tags
                            ) VALUES ($1, $2, $3, $4, $5)
                            """,
                            metric.name,
                            metric.value,
                            metric.timestamp,
                            metric.unit,
                            json.dumps(metric.tags),
                        )

        logger.info(f"Flushed {len(metrics_to_flush)} metrics to database")


class PerformanceTracker:
    """Track performance of a validator operation."""

    def __init__(self, name: str, validator_id: str, epoch: int, block_number: int, phase: str):
        """Initialize the performance tracker."""
        self.name = name
        self.validator_id = validator_id
        self.epoch = epoch
        self.block_number = block_number
        self.phase = phase
        self.start_time = time.time()
        self.metrics = []
        self.tags = {}

    def add_tag(self, key: str, value: str):
        """Add a tag to the performance tracker."""
        self.tags[key] = value

    def record_metric(self, name: str, value: float, unit: str = ""):
        """Record a metric within this operation."""
        self.metrics.append(
            PerformanceMetric(name=name, value=value, unit=unit, tags=self.tags.copy()),
        )

    def complete(
        self, success: bool = True, error_message: Optional[str] = None,
    ) -> ValidationMetrics:
        """Complete the tracking and return validation metrics."""
        end_time = time.time()
        duration_ms = int((end_time - self.start_time) * 1000)

        # Record duration metric
        self.record_metric(name=f"{self.name}_duration", value=duration_ms, unit="ms")

        # Record success metric
        self.record_metric(name=f"{self.name}_success", value=1 if success else 0, unit="bool")

        # Create validation metrics
        return ValidationMetrics(
            validator_id=self.validator_id,
            epoch=self.epoch,
            block_number=self.block_number,
            metrics=self.metrics,
            start_time=datetime.fromtimestamp(self.start_time),
            end_time=datetime.fromtimestamp(end_time),
            total_duration_ms=duration_ms,
            phase=self.phase,
            success=success,
            error_message=error_message,
        )


# Global monitoring system instance
monitoring_system = None


async def initialize_monitoring(db_pool):
    """Initialize the monitoring system."""
    global monitoring_system
    monitoring_system = MonitoringSystem(db_pool)
    await monitoring_system.start()
    return monitoring_system


async def shutdown_monitoring():
    """Shutdown the monitoring system."""
    global monitoring_system
    if monitoring_system:
        await monitoring_system.stop()
        monitoring_system = None


def create_performance_tracker(
    name: str, validator_id: str, epoch: int, block_number: int, phase: str,
) -> PerformanceTracker:
    """Create a performance tracker for an operation."""
    return PerformanceTracker(name, validator_id, epoch, block_number, phase)


async def record_validation_metrics(metrics: ValidationMetrics):
    """Record validation metrics."""
    global monitoring_system
    if monitoring_system:
        await monitoring_system.record_validation_metrics(metrics)


async def record_metric(metric: PerformanceMetric):
    """Record a performance metric."""
    global monitoring_system
    if monitoring_system:
        await monitoring_system.record_metric(metric)
