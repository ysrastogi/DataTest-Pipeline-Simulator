import os
import json
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("pipeline-test") \
        .master("local[*]") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_schema_fixture():
    """Fixture for loading the customer schema."""
    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "examples/schemas/relationships/customer_schema.json"
    )
    
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    
    return schema

@pytest.fixture
def create_test_dataframe(spark_session):
    """Factory fixture to create test DataFrames."""
    def _create_df(data, schema=None):
        """Create a DataFrame from data."""
        if not spark_session:
            pytest.skip("Spark session not available")
            
        return spark_session.createDataFrame(data, schema=schema)
    
    return _create_df

class SparkTestBase:
    """Base class for Spark testing with DataFrame assertions."""
    
    @staticmethod
    def assert_dataframe_equal(actual, expected, check_column_order=True, check_schema=True):
        """Assert that two DataFrames are equal."""
        if check_schema:
            assert actual.schema == expected.schema, f"Schema mismatch: {actual.schema} != {expected.schema}"
        
        if check_column_order:
            assert actual.columns == expected.columns, f"Column order mismatch: {actual.columns} != {expected.columns}"
        else:
            assert set(actual.columns) == set(expected.columns), f"Column sets mismatch: {set(actual.columns)} != {set(expected.columns)}"
        
        # Compare data (convert to sorted lists of rows for deterministic comparison)
        actual_data = sorted(actual.collect())
        expected_data = sorted(expected.collect())
        
        assert len(actual_data) == len(expected_data), f"Row count mismatch: {len(actual_data)} != {len(expected_data)}"
        
        for i, (a_row, e_row) in enumerate(zip(actual_data, expected_data)):
            assert a_row == e_row, f"Row {i} mismatch: {a_row} != {e_row}"
    
    @staticmethod
    def assert_schema_contains_columns(df, expected_columns):
        """Assert that DataFrame contains all expected columns."""
        missing_columns = set(expected_columns) - set(df.columns)
        assert not missing_columns, f"Missing columns: {missing_columns}"
    
    @staticmethod
    def assert_row_count(df, expected_count):
        """Assert that DataFrame has expected number of rows."""
        actual_count = df.count()
        assert actual_count == expected_count, f"Row count mismatch: {actual_count} != {expected_count}"