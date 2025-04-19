from pyspark.sql import DataFrame
from typing import Dict, List, Any, Optional, Union, Callable
import logging

class ValidationResult:
    """Represents the result of a validation operation."""
    
    def __init__(self, success: bool, name: str, details: Dict[str, Any] = None):
        self.success = success
        self.name = name
        self.details = details or {}
        self.timestamp = import_timestamp()
    
    def __str__(self):
        return f"ValidationResult(name={self.name}, success={self.success})"

    def to_dict(self):
        """Convert to dictionary representation for reporting."""
        return {
            "name": self.name,
            "success": self.success,
            "details": self.details,
            "timestamp": self.timestamp
        }

def import_timestamp():
    """Import timestamp function to avoid circular imports."""
    from datetime import datetime
    return datetime.now()

class Validator:
    """Base class for all validators."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"validation.{self.__class__.__name__}")
    
    def validate(self, df: DataFrame, **kwargs) -> ValidationResult:
        """
        Validate the given DataFrame.
        
        Args:
            df: DataFrame to validate
            **kwargs: Additional parameters for validation
            
        Returns:
            ValidationResult: Result of the validation
        """
        raise NotImplementedError("Subclasses must implement validate method")

class ValidationEngine:
    """
    Main validation engine that orchestrates different validation components.
    """
    
    def __init__(self, name: str = "ValidationEngine"):
        self.name = name
        self.validators: List[Validator] = []
        self.logger = logging.getLogger("validation.engine")
    
    def add_validator(self, validator: Validator) -> None:
        """Add a validator to the engine."""
        self.validators.append(validator)
        self.logger.info(f"Added validator: {validator.name}")
    
    def validate(self, df: DataFrame, **kwargs) -> Dict[str, ValidationResult]:
        """
        Run all validators on the given DataFrame.
        
        Args:
            df: DataFrame to validate
            **kwargs: Additional parameters passed to validators
            
        Returns:
            Dict mapping validator names to ValidationResults
        """
        results = {}
        for validator in self.validators:
            try:
                self.logger.info(f"Running validator: {validator.name}")
                result = validator.validate(df, **kwargs)
                results[validator.name] = result
            except Exception as e:
                self.logger.error(f"Error in validator {validator.name}: {e}", exc_info=True)
                results[validator.name] = ValidationResult(
                    success=False,
                    name=validator.name,
                    details={"error": str(e), "type": "exception"}
                )
        
        return results
    
    def validate_pipeline(self, pipeline, input_df: DataFrame) -> Dict[str, Dict[str, ValidationResult]]:
        """
        Validate each stage of a pipeline.
        
        Args:
            pipeline: Pipeline object to validate
            input_df: Input DataFrame
            
        Returns:
            Dict mapping stage names to validation results
        """
        from src.pipeline.core import Pipeline
        
        if not isinstance(pipeline, Pipeline):
            raise TypeError("Expected Pipeline object")
        
        results = {}
        current_df = input_df
        
        # Validate input data
        results["input"] = self.validate(current_df)
        
        # Validate each stage output
        for i, stage in enumerate(pipeline.stages):
            stage_name = stage.name or f"stage_{i}"
            
            # Process data through this stage
            try:
                current_df = stage.process(current_df)
                # Validate the output of this stage
                results[stage_name] = self.validate(current_df)
            except Exception as e:
                self.logger.error(f"Error processing stage {stage_name}: {e}", exc_info=True)
                results[stage_name] = {
                    "error": ValidationResult(
                        success=False, 
                        name=f"{stage_name}_processing", 
                        details={"error": str(e), "type": "exception"}
                    )
                }
                break
        
        return results