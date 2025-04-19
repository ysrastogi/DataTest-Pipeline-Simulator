import json
import os
import pytest
from typing import Dict, List, Any, Optional

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
    def _create_df(data: List[Dict[str, Any]], schema: Optional[List[str]] = None):
        """Create a DataFrame from a list of dictionaries."""
        if not spark_session:
            pytest.skip("Spark session not available")
            
        if schema:
            return spark_session.createDataFrame(data, schema=schema)
        else:
            return spark_session.createDataFrame(data)
    
    return _create_df