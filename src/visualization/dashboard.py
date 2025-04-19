from typing import Dict, List, Any, Optional
import json
import os
import datetime
from src.pipeline.core import Pipeline
from src.pipeline.executor import PipelineExecutor
from src.visualization.graph import PipelineGraph
from src.visualization.flow_diagram import FlowDiagramGenerator

class DashboardGenerator:
    """Generate an interactive dashboard for pipeline monitoring and visualization."""
    
    def __init__(self, pipeline: Optional[Pipeline] = None, 
                execution_results: Optional[List[Dict[str, Any]]] = None):
        self.pipeline = pipeline
        self.execution_results = execution_results or []
    
    def generate_html_dashboard(self, output_path: Optional[str] = None) -> str:
        """Generate an HTML dashboard with pipeline visualizations and execution metrics."""
        # Generate pipeline visualization if a pipeline is provided
        pipeline_visualization = ""
        if self.pipeline:
            flow_generator = FlowDiagramGenerator(self.pipeline)
            mermaid_diagram = flow_generator.generate_mermaid()
            
            pipeline_visualization = f"""
<div class="card">
    <h2>Pipeline Flow</h2>
    <div class="mermaid">
{mermaid_diagram}
    </div>
</div>
"""
        
        # Generate execution metrics if results are provided
        execution_metrics = ""
        if self.execution_results:
            # Calculate statistics
            total_executions = len(self.execution_results)
            successful_executions = sum(1 for r in self.execution_results if r.get("status") == "completed")
            failed_executions = total_executions - successful_executions
            success_rate = (successful_executions / total_executions) * 100 if total_executions > 0 else 0
            
            # Get execution times
            execution_times = [r.get("execution_time", 0) for r in self.execution_results]
            avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
            max_execution_time = max(execution_times) if execution_times else 0
            min_execution_time = min(execution_times) if execution_times else 0
            
            # Generate execution history table
            table_rows = []
            for result in self.execution_results[:10]:  # Show only the last 10 executions
                status_class = "success" if result.get("status") == "completed" else "failure"
                execution_time = result.get("execution_time", 0)
                start_time = datetime.datetime.fromtimestamp(result.get("start_time", 0)).strftime("%Y-%m-%d %H:%M:%S")
                
                table_rows.append(f"""
                <tr>
                    <td>{result.get("execution_id", "N/A")}</td>
                    <td>{start_time}</td>
                    <td>{execution_time:.2f}s</td>
                    <td class="{status_class}">{result.get("status", "N/A")}</td>
                </tr>
                """)
            
            table_body = "".join(table_rows)
            
            execution_metrics = f"""
<div class="card">
    <h2>Execution Metrics</h2>
    <div class="metrics-grid">
        <div class="metric-box">
            <div class="metric-value">{total_executions}</div>
            <div class="metric-label">Total Executions</div>
        </div>
        <div class="metric-box">
            <div class="metric-value">{success_rate:.1f}%</div>
            <div class="metric-label">Success Rate</div>
        </div>
        <div class="metric-box">
            <div class="metric-value">{avg_execution_time:.2f}s</div>
            <div class="metric-label">Avg. Execution Time</div>
        </div>
        <div class="metric-box">
            <div class="metric-value">{max_execution_time:.2f}s</div>
            <div class="metric-label">Max Execution Time</div>
        </div>
    </div>
</div>

<div class="card">
    <h2>Recent Executions</h2>
    <table class="executions-table">
        <thead>
            <tr>
                <th>Execution ID</th>
                <th>Start Time</th>
                <th>Duration</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
            {table_body}
        </tbody>
    </table>
</div>
"""
        
        # Generate the full HTML
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Pipeline Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>
        mermaid.initialize({{ startOnLoad: true }});
    </script>
    <style>
        body {{ 
            font-family: Arial, sans-serif; 
            margin: 0;
            padding: 0;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background-color: #2c3e50;
            color: white;
            padding: 20px;
            text-align: center;
        }}
        .card {{
            background-color: white;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            padding: 20px;
        }}
        h1, h2 {{
            margin-top: 0;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 15px;
        }}
        .metric-box {{
            background-color: #f8f9fa;
            border-radius: 5px;
            padding: 15px;
            text-align: center;
        }}
        .metric-value {{
            font-size: 24px;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        .metric-label {{
            color: #666;
            font-size: 14px;
        }}
        .executions-table {{
            width: 100%;
            border-collapse: collapse;
        }}
        .executions-table th, .executions-table td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        .executions-table th {{
            background-color: #f2f2f2;
        }}
        .success {{ color: green; }}
        .failure {{ color: red; }}
        .mermaid {{ margin: 20px 0; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Pipeline Dashboard</h1>
        <p>{self.pipeline.name if self.pipeline else "Pipeline Simulator"}</p>
    </div>
    
    <div class="container">
        {pipeline_visualization}
        {execution_metrics}
    </div>
</body>
</html>
"""
        
        # Save the HTML if an output path is provided
        if output_path:
            directory = os.path.dirname(output_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)
            
            with open(output_path, 'w') as f:
                f.write(html)
        
        return html