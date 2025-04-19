import pytest

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("pytest-spark") \
        .getOrCreate()
    yield spark
    spark.stop()