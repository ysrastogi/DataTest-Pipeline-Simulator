from typing import Dict, List, Any, Optional
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import logging
from src.pipeline.core import Pipeline
from src.pipeline.executor import PipelineExecutor

class PerformanceAnalyzer:
    """Analyzes and reports on pipeline performance metrics."""
    
    def __init__(self):
        self.logger = logging.getLogger("simulator.performance")
    
    def analyze_execution_results(self, execution_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze performance metrics from execution results."""
        if not execution_results:
            return {}
        
        # Extract performance metrics
        execution_times = [result.get("execution_time", 0) for result in execution_results]
        stage_times = {}
        row_counts = []
        
        for result in execution_results:
            row_counts.append(result.get("row_count", 0))
            
            # Collect stage-level metrics
            for stage in result.get("stage_stats", []):
                stage_name = stage.get("stage_name", "unknown")
                if stage_name not in stage_times:
                    stage_times[stage_name] = []
                stage_times[stage_name].append(stage.get("execution_time", 0))
        
        # Calculate statistics
        stats = {
            "execution_time": {
                "mean": np.mean(execution_times),
                "median": np.median(execution_times),
                "min": min(execution_times),
                "max": max(execution_times),
                "stddev": np.std(execution_times)
            },
            "row_count": {
                "mean": np.mean(row_counts),
                "median": np.median(row_counts),
                "min": min(row_counts),
                "max": max(row_counts)
            },
            "stage_stats": {}
        }
        
        # Calculate stage-level statistics
        for stage_name, times in stage_times.items():
            stats["stage_stats"][stage_name] = {
                "mean": np.mean(times),
                "median": np.median(times),
                "min": min(times),
                "max": max(times),
                "stddev": np.std(times),
                "percentage": (np.mean(times) / stats["execution_time"]["mean"]) * 100
            }
        
        return stats
    
    def generate_performance_report(self, execution_results: List[Dict[str, Any]],
                                  output_path: Optional[str] = None):
        """Generate a performance report from execution results."""
        stats = self.analyze_execution_results(execution_results)
        
        # Create a summary report
        report = ["# Pipeline Performance Report\n"]
        
        report.append("## Overall Performance\n")
        report.append(f"- Mean execution time: {stats['execution_time']['mean']:.2f}s")
        report.append(f"- Median execution time: {stats['execution_time']['median']:.2f}s")
        report.append(f"- Min execution time: {stats['execution_time']['min']:.2f}s")
        report.append(f"- Max execution time: {stats['execution_time']['max']:.2f}s")
        report.append(f"- Standard deviation: {stats['execution_time']['stddev']:.2f}s")
        
        report.append("\n## Stage Performance\n")
        for stage_name, stage_stats in stats.get("stage_stats", {}).items():
            report.append(f"### {stage_name}\n")
            report.append(f"- Mean execution time: {stage_stats['mean']:.2f}s")
            report.append(f"- Percentage of total: {stage_stats['percentage']:.1f}%")
            report.append(f"- Min execution time: {stage_stats['min']:.2f}s")
            report.append(f"- Max execution time: {stage_stats['max']:.2f}s")
        
        report_text = "\n".join(report)
        
        # Save to file if specified
        if output_path:
            with open(output_path, 'w') as f:
                f.write(report_text)
            self.logger.info(f"Performance report saved to {output_path}")
        
        return report_text
    
    def plot_performance_charts(self, execution_results: List[Dict[str, Any]],
                              output_dir: Optional[str] = None):
        """Generate performance visualization charts."""
        import os
        import matplotlib.pyplot as plt
        
        if not execution_results:
            return
        
        stats = self.analyze_execution_results(execution_results)
        
        # Create output directory if needed
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Plot 1: Execution time distribution
        plt.figure(figsize=(10, 6))
        execution_times = [result.get("execution_time", 0) for result in execution_results]
        plt.hist(execution_times, bins=20, alpha=0.7)
        plt.title("Distribution of Pipeline Execution Times")
        plt.xlabel("Execution Time (seconds)")
        plt.ylabel("Frequency")
        plt.grid(True, alpha=0.3)
        
        if output_dir:
            plt.savefig(os.path.join(output_dir, "execution_time_distribution.png"))
            plt.close()
        else:
            plt.show()
        
        # Plot 2: Stage execution times (box plot)
        stage_data = {}
        for result in execution_results:
            for stage in result.get("stage_stats", []):
                stage_name = stage.get("stage_name", "unknown")
                if stage_name not in stage_data:
                    stage_data[stage_name] = []
                stage_data[stage_name].append(stage.get("execution_time", 0))
        
        if stage_data:
            plt.figure(figsize=(12, 6))
            plt.boxplot([times for times in stage_data.values()], labels=list(stage_data.keys()))
            plt.title("Stage Execution Time Distribution")
            plt.xlabel("Stage")
            plt.ylabel("Execution Time (seconds)")
            plt.xticks(rotation=45, ha="right")
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            if output_dir:
                plt.savefig(os.path.join(output_dir, "stage_execution_boxplot.png"))
                plt.close()
            else:
                plt.show()