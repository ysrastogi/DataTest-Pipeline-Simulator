from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Dict, List, Any, Union, Callable, Optional, Set
import re

from src.validation.core import Validator, ValidationResult

class DataQualityCheck(Validator):
    """Base class for data quality checks."""
    
    def __init__(self, name: str, description: str = None):
        super().__init__(name)
        self.description = description or f"Data quality check: {name}"
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Run the quality check and return a result."""
        raise NotImplementedError("Subclasses must implement validate method")

class ColumnNullCheck(DataQualityCheck):
    """Check for null values in specified columns."""
    
    def __init__(self, columns: List[str], threshold: float = 0.0, name: str = None):
        """
        Initialize the null check.
        
        Args:
            columns: List of column names to check
            threshold: Maximum acceptable percentage of nulls (0.0-1.0)
            name: Name of this check
        """
        name = name or f"NullCheck({','.join(columns)})"
        super().__init__(name)
        self.columns = columns
        self.threshold = threshold
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Check for nulls in the specified columns."""
        if not self.columns:
            return ValidationResult(
                success=True,
                name=self.name,
                details={"message": "No columns specified for null check"}
            )
        
        # Check that all columns exist
        missing_columns = [col for col in self.columns if col not in df.columns]
        if missing_columns:
            return ValidationResult(
                success=False,
                name=self.name,
                details={
                    "error": f"Columns not found in DataFrame: {', '.join(missing_columns)}",
                    "missing_columns": missing_columns
                }
            )
        
        # Calculate null percentages
        row_count = df.count()
        if row_count == 0:
            return ValidationResult(
                success=True,
                name=self.name,
                details={"message": "DataFrame is empty"}
            )
        
        null_counts = {}
        for column in self.columns:
            null_count = df.filter(F.col(column).isNull()).count()
            null_counts[column] = {
                "null_count": null_count,
                "total_count": row_count,
                "null_percentage": null_count / row_count if row_count > 0 else 0.0
            }
        
        # Check if any column exceeds the threshold
        failed_columns = {
            col: stats for col, stats in null_counts.items() 
            if stats["null_percentage"] > self.threshold
        }
        
        success = len(failed_columns) == 0
        details = {
            "threshold": self.threshold,
            "null_counts": null_counts,
            "failed_columns": failed_columns if not success else {}
        }
        
        return ValidationResult(success=success, name=self.name, details=details)

class UniqueValueCheck(DataQualityCheck):
    """Check for unique values in specified columns."""
    
    def __init__(self, columns: List[str], name: str = None):
        """
        Initialize the uniqueness check.
        
        Args:
            columns: List of column names to check for uniqueness
            name: Name of this check
        """
        name = name or f"UniqueCheck({','.join(columns)})"
        super().__init__(name)
        self.columns = columns
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Check for uniqueness in the specified columns."""
        if not self.columns:
            return ValidationResult(
                success=True,
                name=self.name,
                details={"message": "No columns specified for uniqueness check"}
            )
        
        # Check that all columns exist
        missing_columns = [col for col in self.columns if col not in df.columns]
        if missing_columns:
            return ValidationResult(
                success=False,
                name=self.name,
                details={
                    "error": f"Columns not found in DataFrame: {', '.join(missing_columns)}",
                    "missing_columns": missing_columns
                }
            )
        
        # Check uniqueness for each column
        uniqueness_stats = {}
        for column in self.columns:
            total_count = df.count()
            distinct_count = df.select(column).distinct().count()
            
            uniqueness_stats[column] = {
                "total_count": total_count,
                "distinct_count": distinct_count,
                "uniqueness_ratio": distinct_count / total_count if total_count > 0 else 1.0,
                "is_unique": distinct_count == total_count
            }
        
        # Identify non-unique columns
        non_unique_columns = {
            col: stats for col, stats in uniqueness_stats.items() 
            if not stats["is_unique"]
        }
        
        success = len(non_unique_columns) == 0
        details = {
            "uniqueness_stats": uniqueness_stats,
            "non_unique_columns": non_unique_columns if not success else {}
        }
        
        return ValidationResult(success=success, name=self.name, details=details)

class PatternCheck(DataQualityCheck):
    """Check if values in a column match a regex pattern."""
    
    def __init__(self, column: str, pattern: str, match_type: str = "all", 
                 threshold: float = 1.0, name: str = None):
        """
        Initialize the pattern check.
        
        Args:
            column: Column name to check
            pattern: Regex pattern to match
            match_type: Type of matching ('all', 'any', or 'none')
            threshold: Minimum acceptable percentage of matches (0.0-1.0)
            name: Name of this check
        """
        name = name or f"PatternCheck({column})"
        super().__init__(name)
        self.column = column
        self.pattern = pattern
        self.match_type = match_type
        self.threshold = threshold
        self._compiled_pattern = re.compile(pattern)
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Check if values in the column match the pattern."""
        if self.column not in df.columns:
            return ValidationResult(
                success=False,
                name=self.name,
                details={"error": f"Column '{self.column}' not found in DataFrame"}
            )
        
        # Register UDF for pattern matching
        from pyspark.sql.functions import udf
        from pyspark.sql.types import BooleanType
        
        @udf(returnType=BooleanType())
        def matches_pattern(value):
            if value is None:
                return False
            return bool(self._compiled_pattern.match(str(value)))
        
        # Calculate match percentage
        total_count = df.count()
        if total_count == 0:
            return ValidationResult(
                success=True,
                name=self.name,
                details={"message": "DataFrame is empty"}
            )
        
        # Count matches
        matches_df = df.withColumn("_matches", matches_pattern(F.col(self.column)))
        
        if self.match_type == "all" or self.match_type == "any":
            match_count = matches_df.filter(F.col("_matches") == True).count()
            match_percentage = match_count / total_count
            
            if self.match_type == "all":
                success = match_percentage >= self.threshold
            else:  # match_type == "any"
                success = match_count > 0
                
        elif self.match_type == "none":
            match_count = matches_df.filter(F.col("_matches") == True).count()
            success = match_count == 0
            match_percentage = match_count / total_count
            
        else:
            return ValidationResult(
                success=False,
                name=self.name,
                details={"error": f"Invalid match_type: {self.match_type}"}
            )
        
        details = {
            "column": self.column,
            "pattern": self.pattern,
            "match_type": self.match_type,
            "threshold": self.threshold,
            "total_count": total_count,
            "match_count": match_count,
            "match_percentage": match_percentage
        }
        
        return ValidationResult(success=success, name=self.name, details=details)