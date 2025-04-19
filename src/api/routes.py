"""
Route definitions for the DataTest Pipeline Simulator API.
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from typing import Dict, List, Any, Optional

from src.api.models import (
    PipelineParams, BenchmarkParams, TestParams, ValidationParams,
    PipelineResult, TestResult, ValidationResult, 
    DashboardGenerationParams, DashboardResult
)

from src.testing.runner import TestRunner
from core.config import Configuration
from pathlib import Path
import os
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Create routers
pipeline_router = APIRouter(prefix="/pipelines", tags=["Pipelines"])
test_router = APIRouter(prefix="/tests", tags=["Tests"])
benchmark_router = APIRouter(prefix="/benchmarks", tags=["Benchmarks"])
validation_router = APIRouter(prefix="/validation", tags=["Validation"])
visualization_router = APIRouter(prefix="/visualization", tags=["Visualization"])


@pipeline_router.post("/run", response_model=PipelineResult, status_code=status.HTTP_202_ACCEPTED)
async def run_pipeline(params: PipelineParams, background_tasks: BackgroundTasks):
    """
    Run a specific pipeline with the provided parameters.
    
    This endpoint triggers pipeline execution and returns a result object with execution details.
    """
    try:
        # In a real implementation, this would execute the pipeline
        # For now, we'll create a mock result
        from src.pipeline.core import Pipeline
        
        # Create output directory if it doesn't exist
        os.makedirs(params.output_dir, exist_ok=True)
        
        # Return a mock result for now
        # In production, you would actually run the pipeline here
        return PipelineResult(
            pipeline_id="mock-id-12345",
            pipeline_name=params.pipeline_name,
            status="completed",
            execution_time=10.5,
            stage_results=[
                {"stage_name": "Load Data", "status": "completed", "execution_time": 2.3},
                {"stage_name": "Transform", "status": "completed", "execution_time": 5.7},
                {"stage_name": "Validate", "status": "completed", "execution_time": 1.2},
            ],
            metrics={"row_count": 1000, "column_count": 15},
            output_path=f"{params.output_dir}/results.parquet"
        )
    except Exception as e:
        logger.exception(f"Error running pipeline: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to run pipeline: {str(e)}"
        )


@benchmark_router.post("/run", response_model=Dict[str, Any], status_code=status.HTTP_202_ACCEPTED)
async def run_benchmark(params: BenchmarkParams):
    """
    Run a benchmark with the specified parameters.
    
    This endpoint executes performance benchmarks and returns timing metrics.
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(params.output_dir, exist_ok=True)
        
        # Return mock benchmark results
        return {
            "benchmark_name": params.benchmark_name,
            "iterations": params.iterations,
            "parallel": params.parallel,
            "results": {
                "avg_execution_time": 12.3,
                "min_execution_time": 10.1,
                "max_execution_time": 15.2,
                "percentile_95": 14.5,
                "throughput": 82.5
            },
            "output_path": f"{params.output_dir}/benchmark_results.json"
        }
    except Exception as e:
        logger.exception(f"Error running benchmark: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to run benchmark: {str(e)}"
        )


@test_router.post("/run", response_model=TestResult)
async def run_tests(params: TestParams):
    """
    Run tests with the specified parameters.
    
    This endpoint discovers and executes tests, returning test results.
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(params.output_dir, exist_ok=True)
        
        # Run the tests using the TestRunner
        runner = TestRunner(output_dir=params.output_dir)
        result = runner.discover_and_run(
            test_dirs=params.test_dirs,
            pattern=params.pattern,
            tags=params.tags
        )
        
        # Extract test results
        return TestResult(
            total=result.get('total', 0),
            passed=result.get('passed', 0),
            failed=result.get('failed', 0),
            error=result.get('error', 0),
            skipped=result.get('skipped', 0),
            report_path=f"{params.output_dir}/test_report.html",
            test_details=result.get('details', [])
        )
    except Exception as e:
        logger.exception(f"Error running tests: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to run tests: {str(e)}"
        )


@validation_router.post("/validate", response_model=ValidationResult)
async def validate_data(params: ValidationParams):
    """
    Validate data against a schema.
    
    This endpoint validates data files or configurations against schemas.
    """
    try:
        # In a real implementation, this would validate the data
        # For now, we'll create a mock result
        
        # Return a mock validation result
        return ValidationResult(
            target=params.target,
            is_valid=True,
            warnings=["Column 'age' has 2.3% null values"],
            details={
                "columns_validated": 15,
                "rows_validated": 1000,
                "schema_used": params.schema
            }
        )
    except Exception as e:
        logger.exception(f"Error validating data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to validate data: {str(e)}"
        )


@visualization_router.post("/dashboard", response_model=DashboardResult)
async def generate_dashboard(params: DashboardGenerationParams):
    """
    Generate a dashboard for pipeline visualization.
    
    This endpoint creates HTML dashboards with pipeline visualizations and metrics.
    """
    try:
        from src.visualization.dashboard import DashboardGenerator
        from src.pipeline.core import Pipeline
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(params.output_path or "outputs/dashboard.html")
        os.makedirs(output_dir, exist_ok=True)
        
        output_path = params.output_path or f"outputs/dashboard_{params.pipeline_name}.html"
        
        # In a real implementation, you would create the actual dashboard
        # For now, return mock result
        return DashboardResult(
            dashboard_path=output_path,
            pipeline_name=params.pipeline_name,
            metrics_summary={
                "success_rate": 98.5,
                "avg_execution_time": 15.3,
                "stages_count": 5
            }
        )
    except Exception as e:
        logger.exception(f"Error generating dashboard: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate dashboard: {str(e)}"
        )