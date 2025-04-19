"""
Benchmarking framework for data pipelines.
"""
import time
import logging
import json
import os
from typing import Dict, List, Any, Callable, Optional, Union, Tuple
from dataclasses import dataclass, field, asdict
import matplotlib.pyplot as plt
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from performance.metrics import MetricsCollector

logger = logging.getLogger(__name__)

@dataclass
class BenchmarkConfig:
    """Configuration for a benchmark run."""
    name: str
    description: str = ""
    iterations: int = 3
    parallel_runs: int = 1
    warm_up_iterations: int = 1
    parameters: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""
    config: BenchmarkConfig
    run_id: str
    start_time: float
    end_time: float = 0
    duration_seconds: float = 0
    iterations_results: List[Dict[str, Any]] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self) -> None:
        """Complete the benchmark and calculate summary statistics."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        
        if not self.iterations_results:
            return
            
        # Extract metrics
        durations = [result.get('duration_seconds', 0) for result in self.iterations_results]
        
        # Calculate summary statistics
        self.summary = {
            'min_duration': min(durations),
            'max_duration': max(durations),
            'avg_duration': sum(durations) / len(durations),
            'total_duration': sum(durations),
            'iterations': len(durations)
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the benchmark result to a dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert the benchmark result to a JSON string."""
        return json.dumps(self.to_dict(), default=str)
    
    def save(self, filepath: str) -> None:
        """Save the benchmark result to a file."""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, default=str, indent=2)


class BenchmarkRunner:
    """Runner for pipeline benchmarks."""
    
    def __init__(self, output_dir: str = "benchmark_results"):
        """Initialize the benchmark runner.
        
        Args:
            output_dir: Directory to store benchmark results
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.results: List[BenchmarkResult] = []
    
    def run_benchmark(self, 
                     config: BenchmarkConfig, 
                     target_func: Callable[..., Any],
                     run_id: Optional[str] = None) -> BenchmarkResult:
        """Run a benchmark according to the provided configuration.
        
        Args:
            config: Benchmark configuration
            target_func: Function to benchmark
            run_id: Optional identifier for this run
            
        Returns:
            Benchmark results
        """
        if run_id is None:
            run_id = f"{config.name}_{int(time.time())}"
            
        logger.info(f"Starting benchmark: {config.name} (Run ID: {run_id})")
        
        result = BenchmarkResult(
            config=config,
            run_id=run_id,
            start_time=time.time()
        )
        
        # Run warm-up iterations if specified
        if config.warm_up_iterations > 0:
            logger.info(f"Running {config.warm_up_iterations} warm-up iterations")
            for i in range(config.warm_up_iterations):
                try:
                    target_func(**config.parameters)
                except Exception as e:
                    logger.error(f"Error during warm-up iteration {i}: {e}")
        
        # Run benchmark iterations
        if config.parallel_runs > 1:
            self._run_parallel_iterations(result, target_func, config)
        else:
            self._run_sequential_iterations(result, target_func, config)
        
        # Complete the benchmark
        result.complete()
        self.results.append(result)
        
        # Save results
        result_path = os.path.join(self.output_dir, f"{run_id}.json")
        result.save(result_path)
        logger.info(f"Benchmark completed: {config.name} - Results saved to {result_path}")
        
        return result
    
    def _run_sequential_iterations(self, 
                                  result: BenchmarkResult, 
                                  target_func: Callable[..., Any], 
                                  config: BenchmarkConfig) -> None:
        """Run benchmark iterations sequentially."""
        for i in range(config.iterations):
            logger.info(f"Running iteration {i+1}/{config.iterations}")
            metrics_collector = MetricsCollector()
            
            start_time = time.time()
            try:
                with metrics_collector.pipeline_metrics(f"{config.name}_iter_{i}"):
                    iteration_result = target_func(**config.parameters)
                success = True
            except Exception as e:
                logger.error(f"Error during iteration {i+1}: {e}")
                iteration_result = None
                success = False
                
            end_time = time.time()
            
            iteration_data = {
                'iteration': i + 1,
                'success': success,
                'start_time': start_time,
                'end_time': end_time,
                'duration_seconds': end_time - start_time,
                'metrics': metrics_collector.history[-1].to_dict() if metrics_collector.history else {}
            }
            
            result.iterations_results.append(iteration_data)
    
    def _run_parallel_iterations(self, 
                               result: BenchmarkResult, 
                               target_func: Callable[..., Any], 
                               config: BenchmarkConfig) -> None:
        """Run benchmark iterations in parallel."""
        logger.info(f"Running {config.iterations} iterations with parallelism {config.parallel_runs}")
        
        with ThreadPoolExecutor(max_workers=config.parallel_runs) as executor:
            futures = []
            
            for i in range(config.iterations):
                future = executor.submit(self._run_single_iteration, target_func, config, i)
                futures.append((i, future))
            
            for i, future in futures:
                try:
                    iteration_data = future.result()
                    result.iterations_results.append(iteration_data)
                except Exception as e:
                    logger.error(f"Error in parallel iteration {i+1}: {e}")
                    result.iterations_results.append({
                        'iteration': i + 1,
                        'success': False,
                        'error': str(e)
                    })
    
    def _run_single_iteration(self, 
                             target_func: Callable[..., Any], 
                             config: BenchmarkConfig, 
                             iteration: int) -> Dict[str, Any]:
        """Run a single benchmark iteration."""
        logger.info(f"Running iteration {iteration+1}/{config.iterations}")
        metrics_collector = MetricsCollector()
        
        start_time = time.time()
        try:
            with metrics_collector.pipeline_metrics(f"{config.name}_iter_{iteration}"):
                iteration_result = target_func(**config.parameters)
            success = True
        except Exception as e:
            logger.error(f"Error during iteration {iteration+1}: {e}")
            iteration_result = None
            success = False
            
        end_time = time.time()
        
        return {
            'iteration': iteration + 1,
            'success': success,
            'start_time': start_time,
            'end_time': end_time,
            'duration_seconds': end_time - start_time,
            'metrics': metrics_collector.history[-1].to_dict() if metrics_collector.history else {}
        }
    
    def compare_benchmarks(self, *benchmark_results: BenchmarkResult) -> pd.DataFrame:
        """Compare multiple benchmark results.
        
        Args:
            benchmark_results: BenchmarkResult objects to compare
            
        Returns:
            DataFrame with comparison data
        """
        comparison_data = []
        
        for result in benchmark_results:
            row = {
                'name': result.config.name,
                'run_id': result.run_id,
                'iterations': result.config.iterations,
                'avg_duration': result.summary.get('avg_duration', 0),
                'min_duration': result.summary.get('min_duration', 0),
                'max_duration': result.summary.get('max_duration', 0),
                'total_duration': result.summary.get('total_duration', 0)
            }
            
            # Add config parameters
            for param_name, param_value in result.config.parameters.items():
                if isinstance(param_value, (int, float, str, bool)):
                    row[f"param_{param_name}"] = param_value
            
            comparison_data.append(row)
            
        return pd.DataFrame(comparison_data)