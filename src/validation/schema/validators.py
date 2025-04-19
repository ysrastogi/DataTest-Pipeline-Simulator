from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import List, Dict, Any, Optional, Set, Union
import json

from src.validation.core import Validator, ValidationResult

class SchemaValidator(Validator):
    """
    Validates DataFrame schema against an expected schema.
    """
    
    def __init__(self, 
                 expected_schema: StructType, 
                 name: str = "SchemaValidator",
                 strict: bool = True):
        """
        Initialize the schema validator.
        
        Args:
            expected_schema: The expected PySpark StructType schema
            name: Name of this validator
            strict: If True, requires exact schema match. If False, allows additional columns.
        """
        super().__init__(name)
        self.expected_schema = expected_schema
        self.strict = strict
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Validate the DataFrame schema against the expected schema."""
        actual_schema = df.schema
        
        # Compare schemas
        issues = []
        
        # Check for missing fields
        expected_fields = {f.name: f for f in self.expected_schema.fields}
        actual_fields = {f.name: f for f in actual_schema.fields}
        
        # Find missing fields (expected but not in actual)
        missing_fields = set(expected_fields.keys()) - set(actual_fields.keys())
        if missing_fields:
            issues.append(f"Missing fields: {', '.join(missing_fields)}")
        
        # Check for extra fields
        extra_fields = set(actual_fields.keys()) - set(expected_fields.keys())
        if self.strict and extra_fields:
            issues.append(f"Extra fields: {', '.join(extra_fields)}")
        
        # Check data types for common fields
        type_mismatches = []
        for field_name in set(expected_fields.keys()) & set(actual_fields.keys()):
            expected_type = expected_fields[field_name].dataType
            actual_type = actual_fields[field_name].dataType
            
            if str(expected_type) != str(actual_type):
                type_mismatches.append(
                    f"Field '{field_name}': expected {expected_type}, got {actual_type}"
                )
        
        if type_mismatches:
            issues.append("Type mismatches: " + "; ".join(type_mismatches))
        
        # Create validation result
        success = len(issues) == 0
        details = {
            "issues": issues,
            "expected_schema": self.expected_schema.json(),
            "actual_schema": actual_schema.json(),
            "strict_validation": self.strict
        }
        
        return ValidationResult(success=success, name=self.name, details=details)

class NullableValidator(Validator):
    """
    Validates that non-nullable fields don't contain nulls.
    """
    
    def __init__(self, non_nullable_fields: List[str], name: str = "NullableValidator"):
        """
        Initialize the nullable validator.
        
        Args:
            non_nullable_fields: List of field names that shouldn't contain nulls
            name: Name of this validator
        """
        super().__init__(name)
        self.non_nullable_fields = non_nullable_fields
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """Validate that specified fields don't contain nulls."""
        from pyspark.sql import functions as F
        
        issues = []
        details = {}
        
        # Check each field for nulls
        for field in self.non_nullable_fields:
            if field not in df.columns:
                issues.append(f"Field '{field}' not found in DataFrame")
                continue
                
            # Count nulls in this field
            null_count = df.filter(F.col(field).isNull()).count()
            
            if null_count > 0:
                issues.append(f"Field '{field}' contains {null_count} null values")
                details[field] = {"null_count": null_count}
        
        success = len(issues) == 0
        details["issues"] = issues
        
        return ValidationResult(success=success, name=self.name, details=details)