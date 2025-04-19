"""
Visualization tools for performance metrics.
"""
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from typing import Dict, List, Any, Optional, Union, Tuple
import json
import os
from pathlib import Path

from performance.benchmark import BenchmarkResult
from performance.metrics import PipelineMetrics, StageMetrics

class MetricsVisualizer:
    """Visualizer for pipeline performance metrics."""
    
    def __init__(self, output_dir: str = "performance_charts"):
        """Initialize the metrics visualizer.
        
        Args:
            output_dir: Directory to save visualization outputs
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set Seaborn style
        sns.set_theme(style="whitegrid")
    
    def plot_pipeline_duration(self, 
                              metrics: Union[PipelineMetrics, List[PipelineMetrics]],
                              title: str = "Pipeline Duration",
                              save_path: Optional[str] = None) -> plt.Figure:
        """Plot pipeline duration.
        
        Args:
            metrics: Pipeline metrics or list of pipeline metrics
            title: Plot title
            save_path: Path to save the plot
            
        Returns:
            Matplotlib figure
        """
        if not isinstance(metrics, list):
            metrics = [metrics]
            
        # Extract data
        pipeline_ids = [m.pipeline_id for m in metrics]
        durations = [m.total_duration_ms / 1000 for m in metrics]  # Convert to seconds
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.barh(pipeline_ids, durations)
        
        # Add labels and formatting
        ax.set_xlabel("Duration (seconds)")
        ax.set_title(title)
        
        # Add values on bars
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.1, 
                    bar.get_y() + bar.get_height()/2, 
                    f"{width:.2f}s", 
                    va='center')
        
        plt.tight_layout()
        
        # Save if requested
        if save_path:
            full_path = os.path.join(self.output_dir, save_path)
            plt.savefig(full_path, dpi=300)
            
        return fig
    
    def plot_stage_breakdown(self, 
                            metrics: PipelineMetrics,
                            title: Optional[str] = None,
                            save_path: Optional[str] = None) -> plt.Figure:
        """Plot duration breakdown by pipeline stage.
        
        Args:
            metrics: Pipeline metrics
            title: Plot title
            save_path: Path to save the plot
            
        Returns:
            Matplotlib figure
        """
        # Extract data
        stage_names = [s.stage_name for s in metrics.stages]
        durations = [s.duration_ms / 1000 for s in metrics.stages]  # Convert to seconds
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.barh(stage_names, durations)
        
        # Add labels and formatting
        ax.set_xlabel("Duration (seconds)")
        if title:
            ax.set_title(title)
        else:
            ax.set_title(f"Stage Duration Breakdown: {metrics.pipeline_id}")
        
        # Add values on bars
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.1, 
                    bar.get_y() + bar.get_height()/2, 
                    f"{width:.2f}s", 
                    va='center')
        
        plt.tight_layout()
        
        # Save if requested
        if save_path:
            full_path = os.path.join(self.output_dir, save_path)
            plt.savefig(full_path, dpi=300)
            
        return fig
    
    def plot_benchmark_comparison(self,
                                 results: List[BenchmarkResult],
                                 metric: str = "avg_duration",
                                 title: str = "Benchmark Comparison",
                                 save_path: Optional[str] = None) -> plt.Figure:
        """Plot comparison of benchmark results.
        
        Args:
            results: List of benchmark results
            metric: Metric to compare ("avg_duration", "min_duration", etc.)
            title: Plot title
            save_path: Path to save the plot
            
        Returns:
            Matplotlib figure
        """
        # Extract data
        names = [r.config.name for r in results]
        values = [r.summary.get(metric, 0) for r in results]
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.barh(names, values)
        
        # Add labels and formatting
        metric_label = " ".join(metric.split("_")).capitalize()
        ax.set_xlabel(f"{metric_label} (seconds)")
        ax.set_title(title)
        
        # Add values on bars
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.1, 
                    bar.get_y() + bar.get_height()/2, 
                    f"{width:.2f}s", 
                    va='center')
        
        plt.tight_layout()
        
        # Save if requested
        if save_path:
            full_path = os.path.join(self.output_dir, save_path)
            plt.savefig(full_path, dpi=300)
            
        return fig
    
    def plot_iteration_comparison(self,
                                 result: BenchmarkResult,
                                 title: Optional[str] = None,
                                 save_path: Optional[str] = None) -> plt.Figure:
        """Plot comparison of benchmark iterations.
        
        Args:
            result: Benchmark result
            title: Plot title
            save_path: Path to save the plot
            
        Returns:
            Matplotlib figure
        """
        # Extract data
        iterations = [r.get('iteration', i+1) for i, r in enumerate(result.iterations_results)]
        durations = [r.get('duration_seconds', 0) for r in result.iterations_results]
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.bar(iterations, durations)
        
        # Add labels and formatting
        ax.set_xlabel("Iteration")
        ax.set_ylabel("Duration (seconds)")
        if title:
            ax.set_title(title)
        else:
            ax.set_title(f"Iteration Comparison: {result.config.name}")
        
        # Add mean line
        if durations:
            mean_duration = sum(durations) / len(durations)
            ax.axhline(mean_duration, color='red', linestyle='--', alpha=0.7)
            ax.text(0.5, mean_duration + 0.1, f"Mean: {mean_duration:.2f}s", 
                    color='red', ha='center')
        
        plt.tight_layout()
        
        # Save if requested
        if save_path:
            full_path = os.path.join(self.output_dir, save_path)
            plt.savefig(full_path, dpi=300)
            
        return fig
    
    def create_performance_dashboard(self,
                                    metrics_list: List[PipelineMetrics],
                                    benchmark_results: Optional[List[BenchmarkResult]] = None,
                                    output_file: str = "performance_dashboard.html") -> str:
        """Create an HTML dashboard for performance metrics.
        
        Args:
            metrics_list: List of pipeline metrics
            benchmark_results: Optional list of benchmark results
            output_file: Output HTML file
            
        Returns:
            Path to generated HTML file
        """
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        import plotly.express as px
        from datetime import datetime
        
        # Create subplot layout
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=(
                "Pipeline Durations", 
                "Memory Usage",
                "Stage Breakdown", 
                "CPU Usage", 
                "Benchmark Results",
                "Records Processed"
            ),
            specs=[
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "bar"}]
            ]
        )
        
        # Pipeline durations
        pipeline_ids = [m.pipeline_id for m in metrics_list]
        durations = [m.total_duration_ms / 1000 for m in metrics_list]
        
        fig.add_trace(
            go.Bar(x=pipeline_ids, y=durations, name="Duration (s)"),
            row=1, col=1
        )
        
        # Memory usage
        memory_usage = [m.peak_memory_mb for m in metrics_list]
        
        fig.add_trace(
            go.Bar(x=pipeline_ids, y=memory_usage, name="Memory (MB)"),
            row=1, col=2
        )
        
        # Stage breakdown for the last pipeline
        if metrics_list and metrics_list[-1].stages:
            last_metrics = metrics_list[-1]
            stage_names = [s.stage_name for s in last_metrics.stages]
            stage_durations = [s.duration_ms / 1000 for s in last_metrics.stages]
            
            fig.add_trace(
                go.Bar(x=stage_names, y=stage_durations, name="Stage Duration (s)"),
                row=2, col=1
            )
            
            # CPU usage by stage
            cpu_usage = [s.cpu_percent for s in last_metrics.stages]
            
            fig.add_trace(
                go.Bar(x=stage_names, y=cpu_usage, name="CPU (%)"),
                row=2, col=2
            )
            
            # Records processed
            records = [s.records_processed for s in last_metrics.stages]
            
            fig.add_trace(
                go.Bar(x=stage_names, y=records, name="Records"),
                row=3, col=2
            )
        
        # Benchmark results
        if benchmark_results:
            bench_names = [r.config.name for r in benchmark_results]
            avg_durations = [r.summary.get('avg_duration', 0) for r in benchmark_results]
            
            fig.add_trace(
                go.Bar(x=bench_names, y=avg_durations, name="Avg Duration (s)"),
                row=3, col=1
            )
        
        # Update layout
        fig.update_layout(
            title_text="Performance Dashboard",
            height=900,
            width=1200,
            showlegend=False
        )
        
        # Add timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fig.add_annotation(
            xref="paper", yref="paper",
            x=0.5, y=-0.05,
            text=f"Generated on {timestamp}",
            showarrow=False
        )
        
        # Save to HTML
        output_path = os.path.join(self.output_dir, output_file)
        fig.write_html(output_path)
        
        return output_path