from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import functools

from src.pipeline.core import Pipeline
from src.transformations.base import LambdaTransformation
from src.transformations.filters import RangeFilter
from src.transformations.aggregations import GroupByAggregation
from src.simulator.mock_data import MockDataGenerator

from src.testing.core import TestCase, PipelineTestCase, TestSuite
from src.testing.assertions import DataFrameAssertions
from src.testing.test_generator import TestCaseGenerator
from src.testing.runner import TestRunner
from src.testing.reporting import TestReporter

# Initialize Spark
spark = SparkSession.builder.appName("TestEngineDemo").getOrCreate()

# Generate mock data
print("Generating mock data...")
data_generator = MockDataGenerator(spark)
user_df = data_generator.generate_user_data(count=500)

# Create a pipeline to test
print("Creating test pipeline...")
pipeline = Pipeline(name="User Age Analysis")

# Define a named function instead of lambda for better serialization
def add_age_group(df):
    return df.withColumn("age_group", 
                         F.when(F.col("age") < 30, "18-29")
                         .when(F.col("age") < 50, "30-49")
                         .otherwise("50+"))

pipeline.add_stage(
    RangeFilter(column="age", min_value=18, max_value=65, name="Filter Users by Age Range")
)

pipeline.add_stage(
    LambdaTransformation(
        add_age_group,  # Using named function instead of lambda
        name="Add Age Group"
    )
)

pipeline.add_stage(
    GroupByAggregation(
        group_by_cols=["age_group", "country"],
        agg_expressions={
            "user_count": F.count("*"),
            "avg_age": F.avg("age")
        },
        name="Aggregate by Age Group and Country"
    )
)

# Create test cases using different approaches
print("\n--- CREATING TEST CASES ---")

# Create a test suite first to avoid reference errors
print("\n--- CREATING TEST SUITE ---")
suite = TestSuite("User Pipeline Tests")

# 1. Manual test case creation
print("Creating manual test case...")
manual_test = PipelineTestCase(
    name="test_user_age_analysis_manual",
    pipeline=pipeline,
    input_data=user_df,
    description="Manual test for User Age Analysis pipeline"
)
suite.add_test_case(manual_test)

# 2. Automated test case generation
print("Generating automated test cases...")
test_generator = TestCaseGenerator(spark)

# Generate a basic pipeline test
pipeline_test = test_generator.generate_from_pipeline(
    pipeline=pipeline, 
    input_data=user_df
)
suite.add_test_case(pipeline_test)

# Generate performance test
performance_test = test_generator.generate_performance_test(
    pipeline=pipeline,
    input_data=user_df
)
suite.add_test_case(performance_test)

# 3. Custom assertion test
print("Creating custom assertion test...")
class UserIdUniquenessTest(TestCase):
    def __init__(self):
        super().__init__(
            name="test_user_id_uniqueness",
            description="Test that user_ids are unique"
        )
        self.add_tag("data_quality")
    
    def _run_test(self, *args, **kwargs):
        df = user_df
        DataFrameAssertions.assert_column_values_unique(df, "user_id")

custom_test = UserIdUniquenessTest()
suite.add_test_case(custom_test)

# Handle problematic operations with try/except
try:
    # Generate tests for each stage
    stage_tests = test_generator.generate_stage_tests(
        pipeline=pipeline,
        input_data=user_df
    )
    
    # Add all stage tests
    for test_case in stage_tests.test_cases:
        suite.add_test_case(test_case)
except Exception as e:
    print(f"Warning: Could not generate stage tests: {e}")
    print("Continuing with other tests...")

try:
    # Generate data quality tests
    data_quality_tests = test_generator.generate_data_quality_tests(
        df=user_df,
        column_checks={
            "user_id": ["not_null", "unique"],
            "age": ["not_null"],
            "country": ["not_null"]
        }
    )
    
    # Add all data quality tests
    for test_case in data_quality_tests.test_cases:
        suite.add_test_case(test_case)
except Exception as e:
    print(f"Warning: Could not generate data quality tests: {e}")
    print("Continuing with other tests...")

# Run the tests
print("\n--- RUNNING TESTS ---")
runner = TestRunner(spark, output_dir="test_reports")
results = runner.run_suite(suite)

# Print summary
print("\n--- TEST RESULTS ---")
total = results["total"]
passed = results["passed"]
failed = results["failed"]
errors = results["error"]
skipped = results["skipped"]

print(f"Total tests: {total}")
print(f"Passed: {passed}")
print(f"Failed: {failed}")
print(f"Errors: {errors}")
print(f"Skipped: {skipped}")

print("\nTest reports generated in the 'test_reports' directory")