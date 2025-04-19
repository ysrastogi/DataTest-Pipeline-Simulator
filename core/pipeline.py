"""
Core pipeline components for the DataTest Pipeline Simulator.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class PipelineStage(ABC):
    """Base class for all pipeline stages."""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.start_time = None
        self.end_time = None
        self.metrics = {}
        
    @abstractmethod
    def execute(self, input_data: Any) -> Any:
        """Execute the pipeline stage."""
        pass
    
    def validate(self, data: Any) -> bool:
        """Validate data before or after processing."""
        return True
    
    def run(self, input_data: Any) -> Any:
        """Run the stage with timing and logging."""
        logger.info(f"Starting stage: {self.name}")
        self.start_time = datetime.now()
        
        # Validate input
        if not self.validate(input_data):
            logger.error(f"Validation failed for stage: {self.name}")
            raise ValueError(f"Input validation failed for stage: {self.name}")
        
        # Execute stage
        try:
            output_data = self.execute(input_data)
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            self.metrics["duration_seconds"] = duration
            logger.info(f"Completed stage: {self.name} in {duration:.2f} seconds")
            return output_data
        except Exception as e:
            logger.exception(f"Error in stage {self.name}: {str(e)}")
            raise

class Pipeline:
    """Pipeline class that runs a sequence of stages."""
    
    def __init__(self, name: str, stages: List[PipelineStage] = None):
        self.name = name
        self.stages = stages or []
        self.metrics = {}
        self.start_time = None
        self.end_time = None
    
    def add_stage(self, stage: PipelineStage) -> None:
        """Add a stage to the pipeline."""
        self.stages.append(stage)
    
    def run(self, initial_data: Any = None) -> Any:
        """Run the entire pipeline."""
        logger.info(f"Starting pipeline: {self.name}")
        self.start_time = datetime.now()
        
        current_data = initial_data
        
        for stage in self.stages:
            current_data = stage.run(current_data)
        
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        self.metrics["duration_seconds"] = duration
        self.metrics["stage_count"] = len(self.stages)
        
        logger.info(f"Completed pipeline: {self.name} in {duration:.2f} seconds")
        return current_data