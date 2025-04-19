"""
Basic data models and interfaces for the DataTest Pipeline Simulator.
"""
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum

class DatasetType(Enum):
    """Types of datasets supported by the system."""
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    AVRO = "avro"
    DELTA = "delta"
    DATABASE = "database"

@dataclass
class Dataset:
    """Represents a dataset in the pipeline."""
    name: str
    path: str
    type: DatasetType
    schema: Optional[Dict[str, str]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if isinstance(self.type, str):
            self.type = DatasetType(self.type.lower())

@dataclass
class DataValidationResult:
    """Results of data validation."""
    success: bool
    stage_name: str
    timestamp: datetime = field(default_factory=datetime.now)
    errors: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PipelineResult:
    """Results of a pipeline run."""
    pipeline_name: str
    success: bool
    start_time: datetime
    end_time: Optional[datetime] = None
    stages_results: Dict[str, Any] = field(default_factory=dict)
    output_dataset: Optional[Dataset] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration_seconds(self) -> float:
        """Calculate the duration of the pipeline run."""
        if not self.end_time:
            return 0
        return (self.end_time - self.start_time).total_seconds()