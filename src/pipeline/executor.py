from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import logging
import time
import json
import os
from src.pipeline.core import Pipeline

class PipelineExecutor:
    """Executes and manages pipeline runs."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger("pipeline.executor")
        self.execution_history = []
        
    def execute_pipeline(self, pipeline: Pipeline, input_df: DataFrame, 
                         execution_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute a pipeline and collect execution metrics."""
        execution_id = f"exec_{int(time.time())}"
        execution_params = execution_params or {}
        
        self.logger.info(f"Starting pipeline execution {execution_id} for pipeline {pipeline.name}")
        
        start_time = time.time()
        execution_result = {
            "execution_id": execution_id,
            "pipeline_id": pipeline.id,
            "pipeline_name": pipeline.name,
            "start_time": start_time,
            "status": "running",
            "params": execution_params
        }
        
        try:
            # Execute the pipeline
            result_df = pipeline.execute(input_df)
            
            # Collect execution results
            end_time = time.time()
            execution_time = end_time - start_time
            
            execution_result.update({
                "status": "completed",
                "end_time": end_time,
                "execution_time": execution_time,
                "stage_stats": pipeline.execution_stats.get("stages", []),
                "row_count": result_df.count(),
                "column_count": len(result_df.columns)
            })
            
            self.logger.info(f"Pipeline execution {execution_id} completed in {execution_time:.2f}s")
            
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            
            execution_result.update({
                "status": "failed",
                "end_time": end_time,
                "execution_time": execution_time,
                "error": str(e),
                "stage_stats": pipeline.execution_stats.get("stages", [])
            })
            
            self.logger.error(f"Pipeline execution {execution_id} failed: {str(e)}")
            
        # Add to history
        self.execution_history.append(execution_result)
        
        return execution_result
    
    def save_execution_history(self, output_path: str):
        """Save execution history to a JSON file."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(self.execution_history, f, indent=2)
        
        self.logger.info(f"Saved execution history to {output_path}")