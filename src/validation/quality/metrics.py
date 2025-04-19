from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Dict, Any, List

def calculate_column_metrics(df: DataFrame, column_name: str) -> Dict[str, Any]:
    """
    Calculate comprehensive metrics for a single column.
    
    Args:
        df: DataFrame containing the column
        column_name: Name of the column to analyze
        
    Returns:
        Dictionary of metrics
    """
    if column_name not in df.columns:
        return {"error": f"Column '{column_name}' not found in DataFrame"}
    
    # Get column data type
    column_type = df.schema[column_name].dataType
    type_name = type(column_type).__name__.replace("Type", "").lower()
    
    # Basic metrics for all types
    metrics = {
        "type": type_name,
        "count": df.count(),
        "null_count": df.filter(F.col(column_name).isNull()).count(),
    }
    
    # Skip further analysis if count is 0
    if metrics["count"] == 0:
        metrics["null_percentage"] = 0.0
        return metrics
    
    metrics["null_percentage"] = metrics["null_count"] / metrics["count"]
    metrics["distinct_count"] = df.select(column_name).distinct().count()
    metrics["distinct_percentage"] = metrics["distinct_count"] / metrics["count"]
    
    # Type-specific metrics
    if isinstance(column_type, (IntegerType, LongType, FloatType, DoubleType)):
        # Numeric metrics
        numeric_metrics = df.select(
            F.min(column_name).alias("min"),
            F.max(column_name).alias("max"),
            F.mean(column_name).alias("mean"),
            F.stddev(column_name).alias("stddev"),
            F.expr(f"percentile({column_name}, 0.5)").alias("median"),
            F.expr(f"percentile({column_name}, array(0.25, 0.5, 0.75))").alias("quartiles")
        ).collect()[0].asDict()
        
        metrics.update(numeric_metrics)
        
        # Add frequency distribution
        try:
            # Get histogram for numeric columns with reasonable cardinality
            if metrics["distinct_count"] <= 100:
                value_counts = df.groupBy(column_name) \
                    .count() \
                    .orderBy(F.desc("count")) \
                    .limit(20) \
                    .collect()
                
                metrics["top_values"] = [
                    {"value": row[column_name], "count": row["count"]}
                    for row in value_counts
                ]
        except Exception as e:
            metrics["histogram_error"] = str(e)
    
    elif isinstance(column_type, StringType):
        # String metrics
        string_metrics = df.select(
            F.min(F.length(column_name)).alias("min_length"),
            F.max(F.length(column_name)).alias("max_length"),
            F.mean(F.length(column_name)).alias("mean_length")
        ).collect()[0].asDict()
        
        metrics.update(string_metrics)
        
        # Get top values
        try:
            value_counts = df.groupBy(column_name) \
                .count() \
                .orderBy(F.desc("count")) \
                .limit(20) \
                .collect()
            
            metrics["top_values"] = [
                {"value": row[column_name], "count": row["count"]}
                for row in value_counts
            ]
        except Exception as e:
            metrics["top_values_error"] = str(e)
    
    elif isinstance(column_type, (DateType, TimestampType)):
        # Date/timestamp metrics
        date_metrics = df.select(
            F.min(column_name).alias("min_date"),
            F.max(column_name).alias("max_date")
        ).collect()[0].asDict()
        
        metrics.update(date_metrics)
    
    return metrics