"""
Pydantic models for the DataTest Pipeline Simulator API.
"""
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field


class PipelineParams(BaseModel):
    """Parameters for pipeline execution."""
    pipeline_name: str = Field(..., description="Name of the pipeline to run")
    parameters: Optional[Dict[str, Any]] = Field(default={}, description="Key-value parameters for the pipeline")
    output_dir: Optional[str] = Field(default="outputs", description="Directory to store output files")


class BenchmarkParams(BaseModel):
    """Parameters for benchmark execution."""
    benchmark_name: str = Field(..., description="Name of the benchmark to run")
    iterations: int = Field(default=1, description="Number of iterations to run")
    parallel: int = Field(default=1, description="Number of parallel runs")
    warmup: int = Field(default=0, description="Number of warm-up iterations")
    output_dir: Optional[str] = Field(default="benchmark_results", description="Output directory")


class TestParams(BaseModel):
    """Parameters for test execution."""
    test_dirs: List[str] = Field(default=["tests"], description="Directories to search for tests")
    pattern: str = Field(default="*_test.py", description="Pattern for test file discovery")
    tags: Optional[List[str]] = Field(default=None, description="Filter tests by tags")
    output_dir: str = Field(default="test_reports", description="Directory for test reports")


class ValidationParams(BaseModel):
    """Parameters for validation."""
    target: str = Field(..., description="Target file or data to validate")
    schema: Optional[str] = Field(default=None, description="Schema file to validate against")
    config: Optional[str] = Field(default=None, description="Configuration file")


class PipelineResult(BaseModel):
    """Result of pipeline execution."""
    pipeline_id: str
    pipeline_name: str
    status: str
    execution_time: float
    stage_results: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    output_path: Optional[str] = None


class TestResult(BaseModel):
    """Result of test execution."""
    total: int
    passed: int
    failed: int
    error: int
    skipped: int
    report_path: Optional[str] = None
    test_details: Optional[List[Dict[str, Any]]] = None


class ValidationResult(BaseModel):
    """Result of validation."""
    target: str
    is_valid: bool
    errors: List[str] = []
    warnings: List[str] = []
    details: Optional[Dict[str, Any]] = None


class DashboardGenerationParams(BaseModel):
    """Parameters for dashboard generation."""
    pipeline_name: str
    execution_results: List[Dict[str, Any]]
    output_path: Optional[str] = None


class DashboardResult(BaseModel):
    """Result of dashboard generation."""
    dashboard_path: str
    pipeline_name: str
    metrics_summary: Dict[str, Any]