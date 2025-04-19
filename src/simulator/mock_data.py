from typing import Dict, List, Optional, Any, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import random
import string
import datetime
import uuid
from faker import Faker

class MockDataGenerator:
    """Generates mock data for pipeline simulation."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.faker = Faker()
    
    def generate_user_data(self, count: int = 1000) -> DataFrame:
        """Generate mock user data."""
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("registration_date", TimestampType(), True),
            StructField("country", StringType(), True),
            StructField("is_active", StringType(), True)
        ])
        
        data = []
        for _ in range(count):
            data.append((
                str(uuid.uuid4()),
                self.faker.name(),
                self.faker.email(),
                random.randint(18, 70),
                self.faker.date_time_between(start_date="-2y", end_date="now"),
                self.faker.country(),
                random.choice(["true", "false"])
            ))
        
        return self.spark.createDataFrame(data, schema)
    
    def generate_transaction_data(self, count: int = 5000, user_df: Optional[DataFrame] = None) -> DataFrame:
        """Generate mock transaction data, optionally linked to user data."""
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("transaction_date", TimestampType(), True),
            StructField("category", StringType(), True),
            StructField("status", StringType(), True)
        ])
        
        # Get user IDs if a user DataFrame is provided
        user_ids = []
        if user_df is not None:
            user_ids = [row.user_id for row in user_df.select("user_id").collect()]
        
        categories = ["food", "electronics", "clothing", "entertainment", "travel"]
        statuses = ["completed", "pending", "failed", "refunded"]
        
        data = []
        for _ in range(count):
            # Use a real user ID if available, otherwise generate a random one
            user_id = random.choice(user_ids) if user_ids else str(uuid.uuid4())
            
            data.append((
                str(uuid.uuid4()),
                user_id,
                round(random.uniform(10.0, 500.0), 2),
                self.faker.date_time_between(start_date="-1y", end_date="now"),
                random.choice(categories),
                random.choice(statuses)
            ))
        
        return self.spark.createDataFrame(data, schema)
    
    def generate_product_data(self, count: int = 200) -> DataFrame:
        """Generate mock product data."""
        schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("inventory", IntegerType(), True),
            StructField("rating", DoubleType(), True)
        ])
        
        categories = ["electronics", "clothing", "food", "home", "sports"]
        
        data = []
        for _ in range(count):
            data.append((
                str(uuid.uuid4()),
                self.faker.bs(),
                random.choice(categories),
                round(random.uniform(10.0, 1000.0), 2),
                random.randint(0, 500),
                round(random.uniform(1.0, 5.0), 1)
            ))
        
        return self.spark.createDataFrame(data, schema)