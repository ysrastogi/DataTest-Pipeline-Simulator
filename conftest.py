from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("pytest-spark") \
        .getOrCreate()
    yield spark
    spark.stop()