import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
        .appName("pytest-spark") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_spark_functionality(spark):
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "value"])
    assert df.count() == 2
    assert df.filter(df.name == "Alice").count() == 1