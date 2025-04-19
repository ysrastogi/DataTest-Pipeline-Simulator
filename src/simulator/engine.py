from typing import Dict, List, Optional, Any, Union
from pyspark.sql import SparkSession, DataFrame
import time
import random
import logging
from src.pipeline.core import Pipeline
from src.pipeline.executor import PipelineExecutor

class PipelineSimulator:
    """Simulates pipeline execution with various configurations and input data."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger("simulator.engine")
        self.executor = PipelineExecutor(self.spark)
        self.results = []
    
    def simulate_pipeline(self, pipeline: Pipeline, input_df: DataFrame,
                         scenarios: List[Dict[str, Any]], 
                         iterations: int = 1) -> List[Dict[str, Any]]:
        """
        Simulate pipeline execution across different scenarios.
        
        Args:
            pipeline: The pipeline to simulate
            input_df: Base input data
            scenarios: List of scenario configurations
            iterations: Number of times to run each scenario
        """
        simulation_results = []
        
        self.logger.info(f"Starting simulation with {len(scenarios)} scenarios, {iterations} iterations each")
        
        for scenario_idx, scenario in enumerate(scenarios):
            scenario_name = scenario.get("name", f"scenario_{scenario_idx}")
            self.logger.info(f"Simulating scenario: {scenario_name}")
            
            # Apply any data transformations for this scenario
            scenario_df = self._apply_scenario_data_transformations(input_df, scenario)
            
            # Apply any pipeline modifications for this scenario
            scenario_pipeline = self._apply_scenario_pipeline_modifications(pipeline, scenario)
            
            # Run the scenario for the specified number of iterations
            for iteration in range(iterations):
                self.logger.info(f"Running iteration {iteration+1}/{iterations} for scenario {scenario_name}")
                
                # Execute the pipeline
                execution_params = {
                    "scenario": scenario_name,
                    "iteration": iteration,
                    "timestamp": time.time()
                }
                
                result = self.executor.execute_pipeline(
                    scenario_pipeline, scenario_df, execution_params
                )
                
                # Add scenario information to the result
                result["scenario"] = scenario
                simulation_results.append(result)
                
                # Add some delay between iterations if needed
                if iterations > 1 and iteration < iterations - 1:
                    time.sleep(0.5)
        
        self.results = simulation_results
        self.logger.info(f"Simulation completed with {len(simulation_results)} total runs")
        
        return simulation_results
    
    def _apply_scenario_data_transformations(self, df: DataFrame, 
                                           scenario: Dict[str, Any]) -> DataFrame:
        """Apply data transformations for a scenario."""
        # Example: add noise, filter data, etc. based on scenario config
        result_df = df
        
        # Apply data size scaling if specified
        if "data_scale_factor" in scenario:
            scale_factor = scenario["data_scale_factor"]
            if scale_factor != 1.0:
                # Sample the DataFrame to simulate different data volumes
                if scale_factor < 1.0:
                    result_df = result_df.sample(fraction=scale_factor)
                else:
                    # For scale_factor > 1, we'd need to duplicate data
                    # This is a simplified approach
                    result_df = result_df.union(result_df.sample(fraction=scale_factor-1.0, withReplacement=True))
        
        return result_df
    
    def _apply_scenario_pipeline_modifications(self, pipeline: Pipeline,
                                             scenario: Dict[str, Any]) -> Pipeline:
        """Apply modifications to the pipeline based on scenario."""
        # This would be more complex in a real implementation
        # For now, we just return the original pipeline
        return pipeline