import requests
import json
import os
from typing import Dict, List, Any, Optional

class ApiClient:
    """Client for interacting with the DataTest Pipeline Simulator API."""
    
    def __init__(self, base_url: str = "http://localhost:8082", api_key: Optional[str] = None):
        """Initialize the API client.
        
        Args:
            base_url: Base URL of the API
            api_key: API key for authentication (can also be set via DATATEST_API_KEY env variable)
        """
        self.base_url = base_url
        self.api_key = api_key or os.environ.get("DATATEST_API_KEY", "")
        
        # Default headers for all requests
        self.headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key
        }
    
    def get_pipelines(self) -> List[Dict[str, Any]]:
        """Get all available pipelines."""
        response = requests.get(f"{self.base_url}/pipelines", headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def get_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Get a specific pipeline by ID."""
        response = requests.get(f"{self.base_url}/pipelines/{pipeline_id}", headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def get_test_results(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get test results."""
        response = requests.get(
            f"{self.base_url}/tests/results", 
            params={"limit": limit},
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_performance_metrics(self, pipeline_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get performance metrics for pipelines."""
        params = {}
        if pipeline_id:
            params["pipeline_id"] = pipeline_id
            
        response = requests.get(
            f"{self.base_url}/benchmarks/metrics", 
            params=params,
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_dashboard(self, pipeline_name: str) -> Dict[str, Any]:
        """Generate a dashboard for a pipeline."""
        data = {
            "pipeline_name": pipeline_name,
            "output_path": None  # Let the API decide the output path
        }
        response = requests.post(
            f"{self.base_url}/visualizations/dashboard",
            json=data,
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()