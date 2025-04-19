from typing import Dict, List, Any, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import random
import logging
import copy
from src.pipeline.core import Pipeline, Stage
from src.testing.core import TestCase, PipelineTestCase, TestSuite

logger = logging.getLogger("testing.generator")

class TestCaseGenerator:
    """Generates test cases from pipeline definitions."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or SparkSession.builder.getOrCreate()
    
    def generate_from_pipeline(self, pipeline: Pipeline, 
                             input_data: Optional[DataFrame] = None,
                             expected_output: Optional[DataFrame] = None) -> PipelineTestCase:
        """Generate a test case from a pipeline definition."""
        test_name = f"test_{pipeline.name.lower().replace(' ', '_')}"
        test_description = f"Auto-generated test for pipeline: {pipeline.name}"
        
        # Create a test case
        test_case = PipelineTestCase(
            name=test_name,
            pipeline=pipeline,
            input_data=input_data,
            expected_output=expected_output,
            description=test_description
        )
        
        return test_case
    
    def generate_stage_tests(self, pipeline: Pipeline, 
                           input_data: DataFrame) -> TestSuite:
        """Generate tests for each stage in the pipeline."""
        suite = TestSuite(f"{pipeline.name} Stage Tests")
        
        current_df = input_data
        
        for idx, stage in enumerate(pipeline.stages):
            stage_name = stage.name
            test_name = f"test_stage_{idx}_{stage_name.lower().replace(' ', '_')}"
            
            # Create a copy of the stage for testing
            stage_copy = copy.deepcopy(stage)
            
            # Create a test pipeline with just this stage
            stage_pipeline = Pipeline(name=f"Stage {idx}: {stage_name}")
            stage_pipeline.add_stage(stage_copy)
            
            # Create a test case for this stage
            test_case = PipelineTestCase(
                name=test_name,
                pipeline=stage_pipeline,
                input_data=current_df,
                description=f"Test for stage {idx}: {stage_name}"
            )
            
            test_case.add_tag("stage_test")
            suite.add_test_case(test_case)
            
            # Execute the stage to get the output for the next stage
            try:
                current_df = stage.execute(current_df)
            except Exception as e:
                logger.warning(f"Failed to execute stage {stage_name} during test generation: {str(e)}")
                break
        
        return suite
    
    def generate_data_quality_tests(self, df: DataFrame, 
                                  column_checks: Dict[str, List[str]] = None) -> TestSuite:
        """Generate data quality tests for a DataFrame."""
        from src.testing.assertions import DataFrameAssertions
        
        suite = TestSuite("Data Quality Tests")
        
        # Default checks for all columns if not specified
        if column_checks is None:
            column_checks = {col: ["not_null", "unique"] for col in df.columns}
        
        # Create test cases for each column
        for column, checks in column_checks.items():
            if "not_null" in checks:
                # Create a test case for null check
                class NotNullTest(TestCase):
                    def __init__(self, column):
                        super().__init__(f"test_{column}_not_null", f"Check that {column} has no null values")
                        self.column = column
                        self.add_tag("data_quality")
                        self.add_tag("not_null")
                    
                    def _run_test(self, *args, **kwargs):
                        test_df = kwargs.get("df", df)
                        DataFrameAssertions.assert_column_values_not_null(test_df, self.column)
                
                suite.add_test_case(NotNullTest(column))
            
            if "unique" in checks:
                # Create a test case for uniqueness check
                class UniqueTest(TestCase):
                    def __init__(self, column):
                        super().__init__(f"test_{column}_unique", f"Check that {column} has unique values")
                        self.column = column
                        self.add_tag("data_quality")
                        self.add_tag("unique")
                    
                    def _run_test(self, *args, **kwargs):
                        test_df = kwargs.get("df", df)
                        DataFrameAssertions.assert_column_values_unique(test_df, self.column)
                
                suite.add_test_case(UniqueTest(column))
        
        # Add a row count test
        class RowCountTest(TestCase):
            def __init__(self):
                super().__init__("test_row_count", "Check row count is as expected")
                self.expected_count = df.count()
                self.add_tag("data_quality")
                self.add_tag("row_count")
            
            def _run_test(self, *args, **kwargs):
                test_df = kwargs.get("df", df)
                DataFrameAssertions.assert_row_count(test_df, self.expected_count)
        
        suite.add_test_case(RowCountTest())
        
        return suite
    
    def generate_performance_test(self, pipeline: Pipeline, 
                                input_data: DataFrame) -> TestCase:
        """Generate a performance test for a pipeline."""
        class PerformanceTest(TestCase):
            def __init__(self, pipeline, input_data, max_execution_time=60):
                super().__init__(f"test_{pipeline.name.lower().replace(' ', '_')}_performance",
                               f"Performance test for pipeline: {pipeline.name}")
                self.pipeline = pipeline
                self.input_data = input_data
                self.max_execution_time = max_execution_time
                self.add_tag("performance")
            
            def _run_test(self, *args, **kwargs):
                from src.pipeline.executor import PipelineExecutor
                import time
                
                spark = kwargs.get("spark") or SparkSession.builder.getOrCreate()
                executor = PipelineExecutor(spark)
                
                start_time = time.time()
                result = executor.execute_pipeline(self.pipeline, self.input_data)
                execution_time = time.time() - start_time
                
                self.metadata["execution_time"] = execution_time
                self.metadata["execution_result"] = result
                
                assert execution_time <= self.max_execution_time, \
                    f"Pipeline execution took {execution_time:.2f}s, exceeding maximum allowed time of {self.max_execution_time}s"
        
        return PerformanceTest(pipeline, input_data)