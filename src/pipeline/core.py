from typing import List, Dict, Any, Optional, Callable
from pyspark.sql import DataFrame, SparkSession
import uuid
import logging

class Stage:
    """Base class for pipeline stages."""
    
    def __init__(self, name: Optional[str] = None, description: str = ""):
        self.id = str(uuid.uuid4())
        self.name = name or f"stage_{self.id[:8]}"
        self.description = description
        self.stats = {}
        
    def execute(self, df: DataFrame) -> DataFrame:
        """Execute this stage's transformation on the input dataframe."""
        raise NotImplementedError("Subclasses must implement execute()")
        
    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"

class Pipeline:
    """A data processing pipeline composed of sequential stages."""
    
    def __init__(self, name: str = "pipeline", description: str = ""):
        self.id = str(uuid.uuid4())
        self.name = name
        self.description = description
        self.stages: List[Stage] = []
        self.execution_stats: Dict[str, Any] = {}
        self.logger = logging.getLogger(f"pipeline.{name}")
    
    def add_stage(self, stage: Stage) -> 'Pipeline':
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self
        
    def execute(self, input_df: DataFrame) -> DataFrame:
        """Execute all stages in the pipeline sequentially."""
        current_df = input_df
        
        self.logger.info(f"Starting pipeline execution: {self.name}")
        stage_stats = []
        
        for idx, stage in enumerate(self.stages):
            stage_name = stage.name
            self.logger.info(f"Executing stage {idx+1}/{len(self.stages)}: {stage_name}")
            
            import time
            start_time = time.time()
            try:
                current_df = stage.execute(current_df)
                execution_time = time.time() - start_time
                
                # Collect stage statistics
                stats = {
                    "stage_id": stage.id,
                    "stage_name": stage_name,
                    "status": "completed",
                    "execution_time": execution_time,
                    "row_count": current_df.count(),
                    "column_count": len(current_df.columns)
                }
                stage.stats = stats
                stage_stats.append(stats)
                
                self.logger.info(f"Completed stage {stage_name} in {execution_time:.2f}s")
                
            except Exception as e:
                self.logger.error(f"Error in stage {stage_name}: {str(e)}")
                stats = {
                    "stage_id": stage.id,
                    "stage_name": stage_name,
                    "status": "failed",
                    "error": str(e),
                    "execution_time": time.time() - start_time
                }
                stage.stats = stats
                stage_stats.append(stats)
                raise
        
        self.execution_stats = {
            "pipeline_id": self.id,
            "pipeline_name": self.name,
            "status": "completed",
            "stages": stage_stats,
            "total_stages": len(self.stages),
            "start_row_count": input_df.count(),
            "final_row_count": current_df.count()
        }
        
        self.logger.info(f"Pipeline {self.name} completed successfully")
        return current_df