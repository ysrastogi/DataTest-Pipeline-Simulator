from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Dict, List, Any, Union, Callable, Optional
import re

from src.validation.core import Validator, ValidationResult

class BusinessRule:
    """Base class for business rules."""
    
    def __init__(self, name: str, description: str = None):
        self.name = name
        self.description = description or name
    
    def apply(self, df: DataFrame) -> DataFrame:
        """
        Apply the rule to a DataFrame and return a DataFrame with violations.
        
        Returns:
            DataFrame with violations, or empty DataFrame if no violations
        """
        raise NotImplementedError("Subclasses must implement apply method")

class ColumnValueRule(BusinessRule):
    """Rule to validate values in a column."""
    
    def __init__(self, column: str, condition: str, 
                 name: str = None, description: str = None):
        """
        Initialize the column value rule.
        
        Args:
            column: Column name to check
            condition: SQL-like condition (e.g. "age >= 18")
            name: Name of this rule
            description: Description of this rule
        """
        name = name or f"Rule_{column}_{condition.replace(' ', '_')}"
        super().__init__(name, description)
        self.column = column
        self.condition = condition
    
    def apply(self, df: DataFrame) -> DataFrame:
        """Find violations of this rule."""
        if self.column not in df.columns:
            # Return empty DataFrame with source schema plus violation info
            schema = df.schema.add("rule_name", StringType(), False) \
                             .add("violation_reason", StringType(), False)
            return df.sparkSession.createDataFrame([], schema)
        
        # Identify violations (records where the condition is FALSE)
        # We add columns to identify the rule and reason
        try:
            violations = df.filter(~F.expr(self.condition)) \
                           .withColumn("rule_name", F.lit(self.name)) \
                           .withColumn("violation_reason", 
                                       F.lit(f"Failed condition: {self.condition}"))
            
            return violations
        except Exception as e:
            # Return DataFrame with error info
            error_df = df.limit(0) \
                        .withColumn("rule_name", F.lit(self.name)) \
                        .withColumn("violation_reason", F.lit(f"Error: {str(e)}"))
            return error_df

class CrossColumnRule(BusinessRule):
    """Rule to validate relationships between columns."""
    
    def __init__(self, condition: str, 
                 name: str = None, description: str = None):
        """
        Initialize the cross-column rule.
        
        Args:
            condition: SQL-like condition involving multiple columns
            name: Name of this rule
            description: Description of this rule
        """
        name = name or f"Rule_{condition.replace(' ', '_')}"
        super().__init__(name, description)
        self.condition = condition
    
    def apply(self, df: DataFrame) -> DataFrame:
        """Find violations of this rule."""
        try:
            violations = df.filter(~F.expr(self.condition)) \
                           .withColumn("rule_name", F.lit(self.name)) \
                           .withColumn("violation_reason", 
                                       F.lit(f"Failed condition: {self.condition}"))
            
            return violations
        except Exception as e:
            # Return DataFrame with error info
            error_df = df.limit(0) \
                        .withColumn("rule_name", F.lit(self.name)) \
                        .withColumn("violation_reason", F.lit(f"Error: {str(e)}"))
            return error_df

class CustomRule(BusinessRule):
    """Rule using a custom function for validation."""
    
    def __init__(self, rule_function: Callable[[DataFrame], DataFrame], 
                 name: str = None, description: str = None):
        """
        Initialize the custom rule.
        
        Args:
            rule_function: Function that takes a DataFrame and returns a 
                           DataFrame with violations
            name: Name of this rule
            description: Description of this rule
        """
        name = name or "CustomRule"
        super().__init__(name, description)
        self.rule_function = rule_function
    
    def apply(self, df: DataFrame) -> DataFrame:
        """Apply the custom function to find violations."""
        try:
            return self.rule_function(df)
        except Exception as e:
            # Return DataFrame with error info
            error_df = df.limit(0) \
                        .withColumn("rule_name", F.lit(self.name)) \
                        .withColumn("violation_reason", F.lit(f"Error: {str(e)}"))
            return error_df