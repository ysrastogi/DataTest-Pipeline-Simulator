import pytest
from unittest import mock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
import copy
from src.testing.generator import TestCaseGenerator
from src.pipeline.core import Pipeline, Stage
from src.testing.core import TestCase, PipelineTestCase, TestSuite
from src.testing.assertions import DataFrameAssertions

# filepath: /Users/yashrastogi1/datatest-pipeline-simulator/src/testing/test_generator.py



class TestTestCaseGenerator:
    """Test class for TestCaseGenerator."""
    
    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        mock_spark = mock.Mock(spec=SparkSession)
        return mock_spark
    
    @pytest.fixture
    def mock_pipeline(self):
        """Create a mock Pipeline."""
        mock_pipeline = mock.Mock(spec=Pipeline)
        mock_pipeline.name = "test_pipeline"
        mock_pipeline.stages = []
        return mock_pipeline
    
    @pytest.fixture
    def mock_stage(self):
        """Create a mock Stage that is safe to copy."""
        mock_stage = mock.Mock(spec=Stage)
        mock_stage.name = "test_stage"
        mock_stage.execute = lambda df: df
        return mock_stage
    
    @pytest.fixture
    def mock_dataframe(self):
        """Create a mock DataFrame."""
        mock_df = mock.Mock(spec=DataFrame)
        mock_df.columns = ["id", "name", "value"]
        mock_df.count.return_value = 10
        return mock_df
    
    def test_init_with_spark(self, mock_spark):
        """Test initialization with provided SparkSession."""
        generator = TestCaseGenerator(spark=mock_spark)
        assert generator.spark == mock_spark
    
    def test_init_without_spark(self):
        """Test initialization without providing SparkSession."""
        with mock.patch('pyspark.sql.SparkSession.builder') as mock_builder:
            mock_session = mock.Mock(spec=SparkSession)
            mock_builder.getOrCreate.return_value = mock_session
            
            generator = TestCaseGenerator()
            
            assert generator.spark == mock_session
            mock_builder.getOrCreate.assert_called_once()
    
    def test_generate_from_pipeline(self, mock_spark, mock_pipeline, mock_dataframe):
        """Test generating a test case from a pipeline."""
        generator = TestCaseGenerator(spark=mock_spark)
        
        test_case = generator.generate_from_pipeline(
            pipeline=mock_pipeline,
            input_data=mock_dataframe,
            expected_output=mock_dataframe
        )
        
        assert isinstance(test_case, PipelineTestCase)
        assert test_case.name == "test_test_pipeline"
        assert test_case.pipeline == mock_pipeline
        assert test_case.input_data == mock_dataframe
        assert test_case.expected_output == mock_dataframe
        assert "Auto-generated test for pipeline: test_pipeline" in test_case.description
    
    def test_generate_from_pipeline_with_spaces_in_name(self, mock_spark, mock_dataframe):
        """Test generating a test case from a pipeline with spaces in name."""
        pipeline = mock.Mock(spec=Pipeline)
        pipeline.name = "Test Pipeline With Spaces"
        
        generator = TestCaseGenerator(spark=mock_spark)
        test_case = generator.generate_from_pipeline(pipeline, mock_dataframe)
        
        assert test_case.name == "test_test_pipeline_with_spaces"
    
    def test_generate_stage_tests(self, mock_spark, mock_pipeline, mock_stage, mock_dataframe):
        """Test generating tests for each stage in the pipeline."""
        # Add a stage to the pipeline
        mock_pipeline.stages = [mock_stage]
        
        # Define our safe copy strategy to avoid the deepcopy issue
        def safe_copy_mock_stage(*args, **kwargs):
            new_stage = mock.Mock(spec=Stage)
            new_stage.name = mock_stage.name
            new_stage.execute = mock_stage.execute
            return new_stage
        
        generator = TestCaseGenerator(spark=mock_spark)
        
        # Patch deepcopy to handle our stage
        with mock.patch('copy.deepcopy', side_effect=lambda x: safe_copy_mock_stage() if x is mock_stage else copy.deepcopy(x)):
            test_suite = generator.generate_stage_tests(
                pipeline=mock_pipeline,
                input_data=mock_dataframe
            )
        
        assert isinstance(test_suite, TestSuite)
        assert len(test_suite.test_cases) == 1
        assert test_suite.test_cases[0].name == "test_stage_0_test_stage"
        assert "stage_test" in test_suite.test_cases[0].tags
    
    def test_generate_stage_tests_with_multiple_stages(self, mock_spark, mock_pipeline, mock_dataframe):
        """Test generating tests for a pipeline with multiple stages."""
        # Create multiple mock stages
        stage1 = mock.Mock(spec=Stage)
        stage1.name = "stage1"
        stage1.execute = lambda df: df
        
        stage2 = mock.Mock(spec=Stage)
        stage2.name = "stage2"
        stage2.execute = lambda df: df
        
        mock_pipeline.stages = [stage1, stage2]
        
        # Function to safely copy our stage mocks
        def safe_copy_stage(stage):
            new_stage = mock.Mock(spec=Stage)
            new_stage.name = stage.name
            new_stage.execute = stage.execute
            return new_stage
        
        # Create a side effect function for our patched deepcopy
        def copy_side_effect(obj):
            if obj in [stage1, stage2]:
                return safe_copy_stage(obj)
            return copy.deepcopy(obj)
        
        generator = TestCaseGenerator(spark=mock_spark)
        
        with mock.patch('copy.deepcopy', side_effect=copy_side_effect):
            test_suite = generator.generate_stage_tests(
                pipeline=mock_pipeline,
                input_data=mock_dataframe
            )
        
        assert isinstance(test_suite, TestSuite)
        assert len(test_suite.test_cases) == 2
        assert test_suite.test_cases[0].name == "test_stage_0_stage1"
        assert test_suite.test_cases[1].name == "test_stage_1_stage2"
    
    def test_generate_stage_tests_with_execution_error(self, mock_spark, mock_pipeline, mock_dataframe):
        """Test handling of stage execution errors during test generation."""
        # Create a stage that raises an exception when executed
        error_stage = mock.Mock(spec=Stage)
        error_stage.name = "error_stage"
        error_stage.execute = mock.Mock(side_effect=Exception("Test execution error"))
        
        mock_pipeline.stages = [error_stage]
        
        def safe_copy_stage(stage):
            new_stage = mock.Mock(spec=Stage)
            new_stage.name = stage.name
            new_stage.execute = stage.execute
            return new_stage
        
        generator = TestCaseGenerator(spark=mock_spark)
        
        with mock.patch('copy.deepcopy', side_effect=lambda x: safe_copy_stage(x) if x is error_stage else copy.deepcopy(x)):
            with mock.patch('src.testing.generator.logger') as mock_logger:
                test_suite = generator.generate_stage_tests(
                    pipeline=mock_pipeline,
                    input_data=mock_dataframe
                )
        
        assert len(test_suite.test_cases) == 1
        mock_logger.warning.assert_called_once()
        assert "Failed to execute stage" in mock_logger.warning.call_args[0][0]
    
    def test_generate_data_quality_tests_default_checks(self, mock_spark, mock_dataframe):
        """Test generating data quality tests with default checks."""
        generator = TestCaseGenerator(spark=mock_spark)
        
        with mock.patch('src.testing.assertions.DataFrameAssertions') as mock_assertions:
            test_suite = generator.generate_data_quality_tests(df=mock_dataframe)
        
        assert isinstance(test_suite, TestSuite)
        # Should have 2 tests per column (not_null, unique) + 1 row count test
        expected_test_count = (2 * len(mock_dataframe.columns)) + 1
        assert len(test_suite.test_cases) == expected_test_count
        
        # Verify test names and tags
        test_names = [tc.name for tc in test_suite.test_cases]
        assert "test_id_not_null" in test_names
        assert "test_id_unique" in test_names
        assert "test_name_not_null" in test_names
        assert "test_name_unique" in test_names
        assert "test_value_not_null" in test_names
        assert "test_value_unique" in test_names
        assert "test_row_count" in test_names
        
        # Verify all tests have data_quality tag
        for tc in test_suite.test_cases:
            assert "data_quality" in tc.tags
    
    def test_generate_data_quality_tests_custom_checks(self, mock_spark, mock_dataframe):
        """Test generating data quality tests with custom checks."""
        generator = TestCaseGenerator(spark=mock_spark)
        
        column_checks = {
            "id": ["not_null"],
            "name": ["unique"]
        }
        
        with mock.patch('src.testing.assertions.DataFrameAssertions') as mock_assertions:
            test_suite = generator.generate_data_quality_tests(
                df=mock_dataframe,
                column_checks=column_checks
            )
        
        assert isinstance(test_suite, TestSuite)
        # Should have 1 test for id, 1 for name, and 1 row count test
        assert len(test_suite.test_cases) == 3
        
        # Verify specific test names
        test_names = [tc.name for tc in test_suite.test_cases]
        assert "test_id_not_null" in test_names
        assert "test_name_unique" in test_names
        assert "test_row_count" in test_names
        
        # Verify specific tags
        not_null_tests = [tc for tc in test_suite.test_cases if "not_null" in tc.tags]
        unique_tests = [tc for tc in test_suite.test_cases if "unique" in tc.tags]
        assert len(not_null_tests) == 1
        assert len(unique_tests) == 1
    
    def test_generate_performance_test(self, mock_spark, mock_pipeline, mock_dataframe):
        """Test generating a performance test."""
        generator = TestCaseGenerator(spark=mock_spark)
        
        performance_test = generator.generate_performance_test(
            pipeline=mock_pipeline,
            input_data=mock_dataframe
        )
        
        assert isinstance(performance_test, TestCase)
        assert performance_test.name == "test_test_pipeline_performance"
        assert "performance" in performance_test.tags
        
        # Verify the test implementation
        with mock.patch('src.pipeline.executor.PipelineExecutor') as mock_executor_class:
            mock_executor = mock.Mock()
            mock_executor_class.return_value = mock_executor
            mock_executor.execute_pipeline.return_value = mock_dataframe
            
            with mock.patch('time.time', side_effect=[0, 10]):  # Mock 10 seconds execution time
                # This should pass since default max_execution_time is 60
                performance_test._run_test(spark=mock_spark)
                
                assert mock_executor.execute_pipeline.called_with(mock_pipeline, mock_dataframe)
                assert performance_test.metadata["execution_time"] == 10
                assert performance_test.metadata["execution_result"] == mock_dataframe
    
    def test_performance_test_timeout(self, mock_spark, mock_pipeline, mock_dataframe):
        """Test performance test fails when execution exceeds time limit."""
        generator = TestCaseGenerator(spark=mock_spark)
        
        # Create test with 5 second limit
        performance_test = generator.generate_performance_test(
            pipeline=mock_pipeline,
            input_data=mock_dataframe
        )
        # Override the default max execution time
        performance_test.max_execution_time = 5
        
        with mock.patch('src.pipeline.executor.PipelineExecutor') as mock_executor_class:
            mock_executor = mock.Mock()
            mock_executor_class.return_value = mock_executor
            mock_executor.execute_pipeline.return_value = mock_dataframe
            
            with mock.patch('time.time', side_effect=[0, 10]):  # Mock 10 seconds execution time
                # This should fail since max_execution_time is 5
                with pytest.raises(AssertionError) as excinfo:
                    performance_test._run_test(spark=mock_spark)
                
                assert "exceeding maximum allowed time" in str(excinfo.value)