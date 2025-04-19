from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Dict, List, Any, Union, Optional
import math

from src.validation.core import Validator, ValidationResult

class AnomalyDetector(Validator):
    """Base class for anomaly detectors."""
    
    def __init__(self, name: str):
        super().__init__(name)
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Detect anomalies in the DataFrame."""
        raise NotImplementedError("Subclasses must implement validate method")
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Detect anomalies and return them as a DataFrame.
        
        Returns:
            DataFrame containing only the anomalous records
        """
        raise NotImplementedError("Subclasses must implement detect_anomalies method")

class StatisticalAnomalyDetector(AnomalyDetector):
    """Detect anomalies based on statistical methods (e.g., Z-score)."""
    
    def __init__(self, column: str, threshold: float = 3.0, name: str = None):
        """
        Initialize the statistical anomaly detector.
        
        Args:
            column: Column to analyze for anomalies
            threshold: Z-score threshold for anomaly detection
            name: Name of this detector
        """
        name = name or f"ZScoreDetector({column})"
        super().__init__(name)
        self.column = column
        self.threshold = threshold
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Detect anomalies using Z-score method."""
        if self.column not in df.columns:
            return ValidationResult(
                success=False,
                name=self.name,
                details={"error": f"Column '{self.column}' not found in DataFrame"}
            )
        
        try:
            # Calculate statistics
            stats = df.select(
                F.mean(self.column).alias("mean"),
                F.stddev(self.column).alias("stddev"),
                F.count(self.column).alias("count")
            ).collect()[0]
            
            mean = stats["mean"]
            stddev = stats["stddev"]
            count = stats["count"]
            
            if count == 0 or stddev == 0:
                return ValidationResult(
                    success=True,
                    name=self.name,
                    details={
                        "message": "Not enough data or variance for anomaly detection",
                        "mean": mean,
                        "stddev": stddev,
                        "count": count
                    }
                )
            
            # Detect anomalies
            anomaly_df = df.withColumn(
                "z_score", 
                F.abs((F.col(self.column) - F.lit(mean)) / F.lit(stddev))
            ).filter(F.col("z_score") > self.threshold)
            
            anomaly_count = anomaly_df.count()
            anomaly_percentage = anomaly_count / count if count > 0 else 0.0
            
            # Consider successful if anomalies are < 5%
            success = anomaly_percentage < 0.05
            
            details = {
                "column": self.column,
                "mean": mean,
                "stddev": stddev,
                "threshold": self.threshold,
                "total_records": count,
                "anomaly_count": anomaly_count,
                "anomaly_percentage": anomaly_percentage
            }
            
            # Include sample anomalies if there are any
            if anomaly_count > 0:
                anomalies = anomaly_df.orderBy(F.desc("z_score")).limit(10).collect()
                details["sample_anomalies"] = [
                    {
                        "value": row[self.column],
                        "z_score": row["z_score"]
                    }
                    for row in anomalies
                ]
            
            return ValidationResult(success=success, name=self.name, details=details)
            
        except Exception as e:
            return ValidationResult(
                success=False,
                name=self.name,
                details={"error": str(e)}
            )
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """Detect anomalies and return them as a DataFrame."""
        if self.column not in df.columns:
            return df.limit(0)  # Return empty DataFrame with same schema
        
        try:
            # Calculate statistics
            stats = df.select(
                F.mean(self.column).alias("mean"),
                F.stddev(self.column).alias("stddev")
            ).collect()[0]
            
            mean = stats["mean"]
            stddev = stats["stddev"]
            
            if stddev == 0:
                return df.limit(0)  # No variation, so no anomalies
            
            # Detect and return anomalies
            return df.withColumn(
                "z_score", 
                F.abs((F.col(self.column) - F.lit(mean)) / F.lit(stddev))
            ).withColumn(
                "anomaly_detector", 
                F.lit(self.name)
            ).filter(F.col("z_score") > self.threshold)
            
        except Exception as e:
            self.logger.error(f"Error detecting anomalies: {e}", exc_info=True)
            return df.limit(0)  # Return empty DataFrame with same schema

class IQRAnomalyDetector(AnomalyDetector):
    """Detect anomalies based on Interquartile Range (IQR)."""
    
    def __init__(self, column: str, iqr_multiplier: float = 1.5, name: str = None):
        """
        Initialize the IQR anomaly detector.
        
        Args:
            column: Column to analyze for anomalies
            iqr_multiplier: Multiplier for IQR to determine threshold
            name: Name of this detector
        """
        name = name or f"IQRDetector({column})"
        super().__init__(name)
        self.column = column
        self.iqr_multiplier = iqr_multiplier
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Detect anomalies using IQR method."""
        if self.column not in df.columns:
            return ValidationResult(
                success=False,
                name=self.name,
                details={"error": f"Column '{self.column}' not found in DataFrame"}
            )
        
        try:
            # Calculate quartiles
            quantiles = df.select(
                F.expr(f"percentile({self.column}, array(0.25, 0.5, 0.75))").alias("quartiles"),
                F.count(self.column).alias("count")
            ).collect()[0]
            
            count = quantiles["count"]
            if count == 0:
                return ValidationResult(
                    success=True,
                    name=self.name,
                    details={"message": "No data for anomaly detection"}
                )
            
            q1 = quantiles["quartiles"][0]
            q3 = quantiles["quartiles"][2]
            iqr = q3 - q1
            
            if iqr == 0:
                return ValidationResult(
                    success=True,
                    name=self.name,
                    details={
                        "message": "No variance in data for IQR anomaly detection",
                        "q1": q1,
                        "q3": q3,
                        "iqr": iqr
                    }
                )
            
            # Calculate bounds
            lower_bound = q1 - (self.iqr_multiplier * iqr)
            upper_bound = q3 + (self.iqr_multiplier * iqr)
            
            # Detect anomalies
            anomaly_df = df.filter(
                (F.col(self.column) < lower_bound) | (F.col(self.column) > upper_bound)
            )
            
            anomaly_count = anomaly_df.count()
            anomaly_percentage = anomaly_count / count
            
            # Consider successful if anomalies are < 5%
            success = anomaly_percentage < 0.05
            
            details = {
                "column": self.column,
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "iqr_multiplier": self.iqr_multiplier,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "total_records": count,
                "anomaly_count": anomaly_count,
                "anomaly_percentage": anomaly_percentage
            }
            
            # Include sample anomalies if there are any
            if anomaly_count > 0:
                anomalies = anomaly_df.limit(10).collect()
                details["sample_anomalies"] = [
                    {
                        "value": row[self.column],
                        "is_outlier": row[self.column] < lower_bound or row[self.column] > upper_bound
                    }
                    for row in anomalies
                ]
            
            return ValidationResult(success=success, name=self.name, details=details)
            
        except Exception as e:
            return ValidationResult(
                success=False,
                name=self.name,
                details={"error": str(e)}
            )
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """Detect anomalies and return them as a DataFrame."""
        if self.column not in df.columns:
            return df.limit(0)
        
        try:
            # Calculate quartiles
            quantiles = df.select(
                F.expr(f"percentile({self.column}, array(0.25, 0.5, 0.75))").alias("quartiles")
            ).collect()[0]
            
            q1 = quantiles["quartiles"][0]
            q3 = quantiles["quartiles"][2]
            iqr = q3 - q1
            
            if iqr == 0:
                return df.limit(0)
            
            # Calculate bounds
            lower_bound = q1 - (self.iqr_multiplier * iqr)
            upper_bound = q3 + (self.iqr_multiplier * iqr)
            
            # Detect and return anomalies
            return df.withColumn(
                "lower_bound", F.lit(lower_bound)
            ).withColumn(
                "upper_bound", F.lit(upper_bound)
            ).withColumn(
                "anomaly_detector", F.lit(self.name)
            ).filter(
                (F.col(self.column) < F.col("lower_bound")) | 
                (F.col(self.column) > F.col("upper_bound"))
            )
            
        except Exception as e:
            self.logger.error(f"Error detecting anomalies: {e}", exc_info=True)
            return df.limit(0)