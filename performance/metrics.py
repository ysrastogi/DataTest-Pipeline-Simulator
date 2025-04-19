"""
Metrics collection for data pipelines.
"""
import time
import logging
import psutil
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field, asdict
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@dataclass
class StageMetrics:
    """Metrics for a single pipeline stage."""
    stage_name: str
    start_time: float
    end_time: float = 0
    duration_ms: float = 0
    records_processed: int = 0
    memory_usage_mb: float = 0
    cpu_percent: float = 0
    spark_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self) -> None:
        """Mark the stage as complete and calculate metrics."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        self.memory_usage_mb = psutil.Process().memory_info().rss / (1024 * 1024)
        self.cpu_percent = psutil.Process().cpu_percent()


@dataclass
class PipelineMetrics:
    """Collection of metrics for an entire pipeline run."""
    pipeline_id: str
    start_time: float
    end_time: float = 0
    stages: List[StageMetrics] = field(default_factory=list)
    total_duration_ms: float = 0
    peak_memory_mb: float = 0
    avg_cpu_percent: float = 0
    
    def complete(self) -> None:
        """Mark the pipeline run as complete and calculate aggregate metrics."""
        self.end_time = time.time()
        self.total_duration_ms = (self.end_time - self.start_time) * 1000
        
        if self.stages:
            self.peak_memory_mb = max(stage.memory_usage_mb for stage in self.stages)
            self.avg_cpu_percent = sum(stage.cpu_percent for stage in self.stages) / len(self.stages)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary format."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert metrics to JSON string."""
        return json.dumps(self.to_dict(), default=str)
    
    def save(self, filepath: str) -> None:
        """Save metrics to a JSON file."""
        with open(filepath, 'w') as f:
            f.write(self.to_json())


class MetricsCollector:
    """Collector for pipeline performance metrics."""
    
    def __init__(self, storage_path: Optional[str] = None):
        """Initialize the metrics collector.
        
        Args:
            storage_path: Path to store metrics files
        """
        self.storage_path = storage_path
        self.current_pipeline: Optional[PipelineMetrics] = None
        self.current_stage: Optional[StageMetrics] = None
        self.history: List[PipelineMetrics] = []
    
    @contextmanager
    def pipeline_metrics(self, pipeline_id: str):
        """Context manager for collecting pipeline metrics."""
        try:
            self.start_pipeline(pipeline_id)
            yield self.current_pipeline
        finally:
            self.end_pipeline()
    
    @contextmanager
    def stage_metrics(self, stage_name: str):
        """Context manager for collecting stage metrics."""
        try:
            self.start_stage(stage_name)
            yield self.current_stage
        finally:
            self.end_stage()
    
    def start_pipeline(self, pipeline_id: str) -> PipelineMetrics:
        """Start collecting metrics for a pipeline run."""
        if self.current_pipeline is not None:
            logger.warning("Starting a new pipeline before previous one completed")
            self.end_pipeline()
            
        self.current_pipeline = PipelineMetrics(
            pipeline_id=pipeline_id,
            start_time=time.time()
        )
        return self.current_pipeline
    
    def end_pipeline(self) -> Optional[PipelineMetrics]:
        """End metrics collection for the current pipeline."""
        if self.current_pipeline is None:
            logger.warning("No active pipeline to end")
            return None
            
        if self.current_stage is not None:
            self.end_stage()
            
        self.current_pipeline.complete()
        completed = self.current_pipeline
        self.history.append(completed)
        
        if self.storage_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.storage_path}/{completed.pipeline_id}_{timestamp}.json"
            completed.save(filename)
            
        self.current_pipeline = None
        return completed
    
    def start_stage(self, stage_name: str) -> StageMetrics:
        """Start collecting metrics for a pipeline stage."""
        if self.current_pipeline is None:
            raise ValueError("Cannot start stage without an active pipeline")
            
        if self.current_stage is not None:
            logger.warning("Starting a new stage before previous one completed")
            self.end_stage()
            
        self.current_stage = StageMetrics(
            stage_name=stage_name,
            start_time=time.time()
        )
        return self.current_stage
    
    def end_stage(self) -> Optional[StageMetrics]:
        """End metrics collection for the current stage."""
        if self.current_stage is None:
            logger.warning("No active stage to end")
            return None
            
        if self.current_pipeline is None:
            logger.warning("Ending stage without an active pipeline")
            self.current_stage.complete()
            completed = self.current_stage
            self.current_stage = None
            return completed
            
        self.current_stage.complete()
        completed = self.current_stage
        self.current_pipeline.stages.append(completed)
        self.current_stage = None
        return completed
    
    def record_count(self, count: int) -> None:
        """Record the number of records processed in the current stage."""
        if self.current_stage:
            self.current_stage.records_processed = count
    
    def add_spark_metrics(self, metrics: Dict[str, Any]) -> None:
        """Add Spark-specific metrics to the current stage."""
        if self.current_stage:
            self.current_stage.spark_metrics.update(metrics)