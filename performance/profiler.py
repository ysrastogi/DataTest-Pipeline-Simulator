"""
Performance profiling tools for data pipelines.
"""
import cProfile
import pstats
import io
import time
import logging
import functools
from typing import Dict, Any, Callable, Optional, List, Union
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

class Profiler:
    """Profiler for Python code and Spark operations."""
    
    def __init__(self, enabled: bool = True):
        """Initialize the profiler.
        
        Args:
            enabled: Whether profiling is enabled
        """
        self.enabled = enabled
        self.results = {}
    
    def profile_function(self, func):
        """Decorator to profile a function."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not self.enabled:
                return func(*args, **kwargs)
                
            pr = cProfile.Profile()
            pr.enable()
            
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                pr.disable()
                s = io.StringIO()
                ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
                ps.print_stats()
                
                self.results[func.__name__] = {
                    'profile_text': s.getvalue(),
                    'stats': {
                        'function': func.__name__,
                        'ncalls': ps.total_calls,
                        'tottime': ps.total_tt
                    }
                }
                logger.debug(f"Profiled {func.__name__}: {ps.total_calls} calls, {ps.total_tt:.3f}s total time")
                
        return wrapper
    
    @staticmethod
    def profile_dataframe(df: DataFrame, action: str = "count") -> Dict[str, Any]:
        """Profile a Spark DataFrame execution.
        
        Args:
            df: The DataFrame to profile
            action: The action to trigger execution ("count", "collect", etc.)
            
        Returns:
            Dictionary containing profiling metrics
        """
        if not isinstance(df, DataFrame):
            raise TypeError("Expected a Spark DataFrame")
            
        # Get the Spark context
        sc = df.sparkSession.sparkContext
        
        # Enable Spark UI metrics
        sc.setJobDescription("Profiling job")
        
        # Clear any cached metrics
        sc._jsc.sc().cleanse()
        
        # Execute the action and measure time
        start_time = time.time()
        
        if action == "count":
            result = df.count()
        elif action == "collect":
            result = df.collect()
        else:
            raise ValueError(f"Unsupported action: {action}")
            
        duration = time.time() - start_time
        
        # Get the execution plan
        plan = df._jdf.queryExecution().toString()
        
        # Get metrics from the last job
        last_stage_info = sc.statusTracker().getStageInfo()
        
        metrics = {
            "action": action,
            "duration_seconds": duration,
            "execution_plan": plan,
            "result_size": result if action == "count" else len(result),
            "stages": last_stage_info
        }
        
        return metrics
    
    def get_results(self) -> Dict[str, Any]:
        """Get the profiling results."""
        return self.results
    
    def clear_results(self) -> None:
        """Clear the profiling results."""
        self.results = {}
    
    def print_results(self) -> None:
        """Print the profiling results."""
        for name, data in self.results.items():
            print(f"Profile for {name}:")
            print(data['profile_text'])
            print("-" * 80)