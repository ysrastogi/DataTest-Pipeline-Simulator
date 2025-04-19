from typing import Dict, Any, Optional, List, Callable
from pyspark.sql import DataFrame
from src.pipeline.core import Stage

class Transformation(Stage):
    """Base class for all data transformations."""
    
    def __init__(self, name: Optional[str] = None, description: str = ""):
        super().__init__(name, description)
    
    def validate_input(self, df: DataFrame) -> bool:
        """Validate that input DataFrame meets requirements."""
        return True
    
    def validate_output(self, df: DataFrame) -> bool:
        """Validate that output DataFrame meets requirements."""
        return True

class ColumnTransformation(Transformation):
    """Transformation that operates on specific columns."""
    
    def __init__(self, input_cols: List[str], output_cols: Optional[List[str]] = None, 
                 name: Optional[str] = None, description: str = ""):
        super().__init__(name, description)
        self.input_cols = input_cols
        self.output_cols = output_cols or input_cols
    
    def validate_input(self, df: DataFrame) -> bool:
        """Validate that required input columns exist."""
        df_columns = df.columns
        missing_columns = [col for col in self.input_cols if col not in df_columns]
        
        if missing_columns:
            raise ValueError(f"Missing required input columns: {missing_columns}")
        
        return True

class LambdaTransformation(Transformation):
    """A transformation defined by a lambda function."""
    
    def __init__(self, transform_func: Callable[[DataFrame], DataFrame], 
                 name: Optional[str] = None, description: str = ""):
        super().__init__(name, description)
        self.transform_func = transform_func
    
    def execute(self, df: DataFrame) -> DataFrame:
        """Execute the lambda function on the DataFrame."""
        return self.transform_func(df)