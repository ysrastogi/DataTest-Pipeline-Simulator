import pytest
from pyspark.sql import functions as F  # Import functions module correctly
from pyspark.sql.types import StructType, StructField, StringType, LongType
from tests.conftest import SparkTestBase, spark_session

class TestPipelineValidation(SparkTestBase):
    """Test class for validating data pipelines."""
    
    def test_simple_transformation(self, spark_session):
        """Test a simple data transformation."""
        # Skip if no Spark available
        if not spark_session:
            pytest.skip("Spark session not available")
            
        # Create test data
        input_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        input_df = spark_session.createDataFrame(input_data, ["name", "age"])
        
        # Define schema with correct nullability
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", LongType(), True),
            StructField("category", StringType(), False)  # Match the nullability of withColumn
        ])
        
        # Define expected output with explicit schema
        expected_data = [("Alice", 25, "Young"), ("Bob", 30, "Adult"), ("Charlie", 35, "Adult")]
        expected_df = spark_session.createDataFrame(expected_data, schema)
        
        # Apply transformation (this would normally call your pipeline code)
        actual_df = input_df.withColumn("category", 
                                       F.when(input_df.age < 30, "Young")
                                       .otherwise("Adult"))
        
        # Assert the results
        self.assert_dataframe_equal(actual_df, expected_df)