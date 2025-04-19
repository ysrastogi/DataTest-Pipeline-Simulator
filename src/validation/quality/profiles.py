from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Any, Optional, Set, Union
import json

from src.validation.core import Validator, ValidationResult
from src.validation.quality.checks import DataQualityCheck
from src.validation.quality.metrics import calculate_column_metrics

class DataQualityProfile(Validator):
    """
    Creates a comprehensive data quality profile for a DataFrame.
    """
    
    def __init__(self, name: str = "DataQualityProfile"):
        super().__init__(name)
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Generate a complete data quality profile."""
        profile = {}
        
        # Basic DataFrame stats
        row_count = df.count()
        column_count = len(df.columns)
        
        profile["basic_stats"] = {
            "row_count": row_count,
            "column_count": column_count,
            "columns": df.columns
        }
        
        # Column-level metrics
        profile["columns"] = {}
        for column in df.columns:
            column_metrics = calculate_column_metrics(df, column)
            profile["columns"][column] = column_metrics
        
        # Schema information
        profile["schema"] = json.loads(df.schema.json())
        
        return ValidationResult(
            success=True,  # Profiling always succeeds
            name=self.name,
            details=profile
        )

class DataQualitySuite(Validator):
    """
    A suite of data quality checks.
    """
    
    def __init__(self, name: str, checks: List[DataQualityCheck] = None):
        """
        Initialize the data quality suite.
        
        Args:
            name: Name of this suite
            checks: List of DataQualityCheck objects
        """
        super().__init__(name)
        self.checks = checks or []
    
    def add_check(self, check: DataQualityCheck) -> None:
        """Add a check to the suite."""
        self.checks.append(check)
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Run all checks in the suite."""
        results = {}
        all_passed = True
        
        for check in self.checks:
            result = check.validate(df, **kwargs)
            results[check.name] = result.to_dict()
            
            if not result.success:
                all_passed = False
        
        details = {
            "check_count": len(self.checks),
            "checks_passed": sum(1 for r in results.values() if r["success"]),
            "checks_failed": sum(1 for r in results.values() if not r["success"]),
            "check_results": results
        }
        
        return ValidationResult(
            success=all_passed,
            name=self.name,
            details=details
        )