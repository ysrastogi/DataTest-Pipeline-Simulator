import os
import json
import pandas as pd
from typing import Dict, List, Any, Optional

class DataLoader:
    """Utility for loading pipeline data from files."""
    
    def __init__(self, base_dir: str = "/Users/yashrastogi1/datatest-pipeline-simulator"):
        """Initialize the data loader.
        
        Args:
            base_dir: Base directory where output files are stored
        """
        self.base_dir = base_dir
        self.outputs_dir = os.path.join(base_dir, "outputs")
        self.reports_dir = os.path.join(base_dir, "test_reports")
        self.charts_dir = os.path.join(base_dir, "performance_charts")
    
    def get_available_pipelines(self) -> List[str]:
        """Get a list of available pipeline reports."""
        # Look for pipeline flow HTML files in the outputs/reports directory
        reports_dir = os.path.join(self.outputs_dir, "reports")
        if not os.path.exists(reports_dir):
            return []
            
        pipeline_files = [f for f in os.listdir(reports_dir) if f.endswith("_flow.html")]
        # Extract pipeline names from filenames
        pipeline_names = [f.replace("_flow.html", "") for f in pipeline_files]
        return pipeline_names
    
    def get_pipeline_data(self, pipeline_name: str) -> Dict[str, Any]:
        """Get data for a specific pipeline."""
        # This is a placeholder - in a real implementation, you would parse the HTML files
        # or other data sources to extract structured information about the pipeline
        
        dashboard_path = os.path.join(self.outputs_dir, "reports", f"{pipeline_name}_dashboard.html")
        flow_path = os.path.join(self.outputs_dir, "reports", f"{pipeline_name}_flow.html")
        
        return {
            "name": pipeline_name,
            "dashboard_path": dashboard_path if os.path.exists(dashboard_path) else None,
            "flow_path": flow_path if os.path.exists(flow_path) else None
        }
    
    def get_test_results(self) -> pd.DataFrame:
        """Get test results as a pandas DataFrame."""
        # Look for test report HTML files
        test_reports = [f for f in os.listdir(self.reports_dir) if f.endswith("_tests_report.html")]
        
        # This is a placeholder - in a real implementation, you would parse the HTML reports
        # to extract structured data about test results
        
        # Return a sample DataFrame for demonstration
        return pd.DataFrame({
            "test_name": ["Test 1", "Test 2", "Test 3"],
            "status": ["Passed", "Failed", "Passed"],
            "duration": [1.2, 2.3, 0.8],
            "pipeline": ["user_pipeline", "transaction_pipeline", "analytics_pipeline"]
        })
    
    def load_performance_dashboard(self) -> Optional[str]:
        """Load the path to the performance dashboard HTML."""
        dashboard_path = os.path.join(self.charts_dir, "dashboard.html")
        return dashboard_path if os.path.exists(dashboard_path) else None