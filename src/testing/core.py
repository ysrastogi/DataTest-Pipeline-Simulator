from typing import Dict, List, Any, Optional, Callable, Union, Set
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import inspect
import time
import uuid
import logging
import json
import os
from datetime import datetime

class TestCase:
    """Base class for all test cases."""
    
    def __init__(self, name: str, description: Optional[str] = None):
        self.id = str(uuid.uuid4())
        self.name = name
        self.description = description or ""
        self.tags: Set[str] = set()
        self.logger = logging.getLogger(f"testing.{name}")
        self._setup_functions: List[Callable] = []
        self._teardown_functions: List[Callable] = []
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration: Optional[float] = None
        self.status: str = "not_run"  # not_run, running, passed, failed, error, skipped
        self.error_message: Optional[str] = None
        self.error_details: Optional[str] = None
        self.metadata: Dict[str, Any] = {}
    
    def setup(self, func: Callable) -> Callable:
        """Decorator to register a setup function."""
        self._setup_functions.append(func)
        return func
    
    def teardown(self, func: Callable) -> Callable:
        """Decorator to register a teardown function."""
        self._teardown_functions.append(func)
        return func
    
    def add_tag(self, tag: str) -> 'TestCase':
        """Add a tag to this test case."""
        self.tags.add(tag)
        return self
    
    def run(self, *args, **kwargs) -> bool:
        """Run the test case."""
        self.start_time = time.time()
        self.status = "running"
        
        try:
            # Run setup functions
            for setup_func in self._setup_functions:
                setup_func(*args, **kwargs)
            
            # Run the test
            self._run_test(*args, **kwargs)
            
            # If we got here, the test passed
            self.status = "passed"
            return True
            
        except AssertionError as e:
            # Test failed due to assertion error
            self.status = "failed"
            self.error_message = str(e)
            self.error_details = self._get_formatted_traceback()
            return False
            
        except Exception as e:
            # Test encountered an error
            self.status = "error"
            self.error_message = f"{type(e).__name__}: {str(e)}"
            self.error_details = self._get_formatted_traceback()
            return False
            
        finally:
            # Run teardown functions
            for teardown_func in self._teardown_functions:
                try:
                    teardown_func(*args, **kwargs)
                except Exception as e:
                    self.logger.error(f"Error in teardown: {str(e)}")
            
            self.end_time = time.time()
            self.duration = self.end_time - self.start_time
    
    def _run_test(self, *args, **kwargs):
        """Implement this method in subclasses to define the test logic."""
        raise NotImplementedError("Subclasses must implement _run_test()")
    
    def _get_formatted_traceback(self) -> str:
        """Get a formatted traceback for the current exception."""
        import traceback
        return traceback.format_exc()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert test case to a dictionary representation."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "tags": list(self.tags),
            "status": self.status,
            "duration": self.duration,
            "error_message": self.error_message,
            "error_details": self.error_details,
            "metadata": self.metadata
        }
    
    def __str__(self) -> str:
        status_str = {
            "not_run": "⏳",
            "running": "🔄",
            "passed": "✅",
            "failed": "❌",
            "error": "⚠️",
            "skipped": "⏭️"
        }.get(self.status, "?")
        
        duration_str = f" ({self.duration:.2f}s)" if self.duration is not None else ""
        return f"{status_str} {self.name}{duration_str}"

class PipelineTestCase(TestCase):
    """Test case for testing a data pipeline."""
    
    def __init__(self, name: str, pipeline, input_data: Optional[DataFrame] = None, 
                 expected_output: Optional[DataFrame] = None, description: Optional[str] = None):
        super().__init__(name, description)
        self.pipeline = pipeline
        self.input_data = input_data
        self.expected_output = expected_output
        self.actual_output: Optional[DataFrame] = None
        self.add_tag("pipeline")
    
    def with_input(self, input_data: DataFrame) -> 'PipelineTestCase':
        """Set the input data for this test case."""
        self.input_data = input_data
        return self
    
    def with_expected_output(self, expected_output: DataFrame) -> 'PipelineTestCase':
        """Set the expected output data for this test case."""
        self.expected_output = expected_output
        return self
    
    def _run_test(self, *args, **kwargs):
        """Run the pipeline and compare the output with the expected output."""
        from src.pipeline.executor import PipelineExecutor
        
        # Get Spark session
        spark = kwargs.get("spark") or SparkSession.builder.getOrCreate()
        
        # Validate that we have input data
        if self.input_data is None:
            raise ValueError("Input data is required for pipeline test case")
        
        # Execute the pipeline
        executor = PipelineExecutor(spark)
        result = executor.execute_pipeline(self.pipeline, self.input_data)
        
        # Store the execution result in metadata
        self.metadata["execution_result"] = result
        
        # If expected output is provided, compare the results
        if self.expected_output is not None:
            self._compare_dataframes(self.actual_output, self.expected_output)
    
    def _compare_dataframes(self, actual: DataFrame, expected: DataFrame):
        """Compare two DataFrames for equality."""
        # Check schema equality
        if actual.schema != expected.schema:
            raise AssertionError(f"Schema mismatch:\nActual: {actual.schema}\nExpected: {expected.schema}")
        
        # Check row count equality
        actual_count = actual.count()
        expected_count = expected.count()
        if actual_count != expected_count:
            raise AssertionError(f"Row count mismatch: Actual={actual_count}, Expected={expected_count}")
        
        # Check data equality (this is an expensive operation)
        # We use except to find rows in one DataFrame but not in the other
        diff1 = actual.exceptAll(expected)
        diff2 = expected.exceptAll(actual)
        
        if diff1.count() > 0 or diff2.count() > 0:
            diff1_sample = diff1.limit(5).collect()
            diff2_sample = diff2.limit(5).collect()
            raise AssertionError(
                f"Data mismatch:\nRows in actual but not in expected: {diff1_sample}\n" +
                f"Rows in expected but not in actual: {diff2_sample}"
            )

class TestSuite:
    """A collection of test cases."""
    
    def __init__(self, name: str, description: Optional[str] = None):
        self.id = str(uuid.uuid4())
        self.name = name
        self.description = description or ""
        self.test_cases: List[TestCase] = []
        self.tags: Set[str] = set()
        self.logger = logging.getLogger(f"testing.suite.{name}")
    
    def add_test_case(self, test_case: TestCase) -> 'TestSuite':
        """Add a test case to this suite."""
        self.test_cases.append(test_case)
        return self
    
    def add_tag(self, tag: str) -> 'TestSuite':
        """Add a tag to this test suite."""
        self.tags.add(tag)
        return self
    
    def run(self, *args, **kwargs) -> Dict[str, Any]:
        """Run all test cases in this suite."""
        start_time = time.time()
        
        self.logger.info(f"Running test suite: {self.name}")
        self.logger.info(f"Number of test cases: {len(self.test_cases)}")
        
        results = {
            "total": len(self.test_cases),
            "passed": 0,
            "failed": 0,
            "error": 0,
            "skipped": 0,
            "test_results": []
        }
        
        for idx, test_case in enumerate(self.test_cases):
            self.logger.info(f"Running test case {idx+1}/{len(self.test_cases)}: {test_case.name}")
            
            success = test_case.run(*args, **kwargs)
            results["test_results"].append(test_case.to_dict())
            
            if test_case.status == "passed":
                results["passed"] += 1
                self.logger.info(f"Test case passed: {test_case.name}")
            elif test_case.status == "failed":
                results["failed"] += 1
                self.logger.warning(f"Test case failed: {test_case.name} - {test_case.error_message}")
            elif test_case.status == "error":
                results["error"] += 1
                self.logger.error(f"Test case error: {test_case.name} - {test_case.error_message}")
            elif test_case.status == "skipped":
                results["skipped"] += 1
                self.logger.info(f"Test case skipped: {test_case.name}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        results["duration"] = duration
        results["start_time"] = start_time
        results["end_time"] = end_time
        
        self.logger.info(f"Test suite completed: {self.name}")
        self.logger.info(f"Total: {results['total']}, Passed: {results['passed']}, " +
                       f"Failed: {results['failed']}, Error: {results['error']}, " +
                       f"Skipped: {results['skipped']}")
        self.logger.info(f"Duration: {duration:.2f} seconds")
        
        return results
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert test suite to a dictionary representation."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "tags": list(self.tags),
            "test_cases": [tc.to_dict() for tc in self.test_cases]
        }