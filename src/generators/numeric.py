"""
Specialized generator for numeric data types.
"""
import random
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple
from .base import DataGenerator

class NumericGenerator(DataGenerator):
    """Generator for numeric data types (integers, floats, etc.)."""
    
    def __init__(self, schema: Dict[str, Any], seed: Optional[int] = None):
        """
        Initialize the numeric generator.
        
        Args:
            schema: Schema definition for numeric fields
            seed: Optional seed for random number generation
        """
        super().__init__(seed)
        self.schema = schema
        self._validate_schema()
    
    def _validate_schema(self) -> None:
        """Validate the schema definition."""
        required_fields = ["fields"]
        for field in required_fields:
            if field not in self.schema:
                raise ValueError(f"Schema missing required field: {field}")
        
        for field_name, field_def in self.schema["fields"].items():
            if "type" not in field_def:
                raise ValueError(f"Field {field_name} missing type definition")
            
            if field_def["type"] not in ["integer", "float", "decimal"]:
                raise ValueError(f"Invalid numeric type for {field_name}: {field_def['type']}")
    
    def generate(self, count: int = 1) -> pd.DataFrame:
        """Generate numeric data based on the schema."""
        data = {}
        
        for field_name, field_def in self.schema["fields"].items():
            field_type = field_def["type"]
            min_val = field_def.get("min", 0)
            max_val = field_def.get("max", 100)
            distribution = field_def.get("distribution", "uniform")
            
            if field_type == "integer":
                if distribution == "uniform":
                    data[field_name] = np.random.randint(min_val, max_val + 1, size=count)
                elif distribution == "normal":
                    mean = field_def.get("mean", (min_val + max_val) / 2)
                    std = field_def.get("std", (max_val - min_val) / 6)  # 99.7% within range
                    values = np.random.normal(mean, std, size=count)
                    data[field_name] = np.clip(np.round(values).astype(int), min_val, max_val)
            
            elif field_type == "float" or field_type == "decimal":
                decimals = field_def.get("decimals", 2)
                if distribution == "uniform":
                    data[field_name] = np.round(np.random.uniform(min_val, max_val, size=count), decimals)
                elif distribution == "normal":
                    mean = field_def.get("mean", (min_val + max_val) / 2)
                    std = field_def.get("std", (max_val - min_val) / 6)
                    values = np.random.normal(mean, std, size=count)
                    data[field_name] = np.round(np.clip(values, min_val, max_val), decimals)
        
        df = pd.DataFrame(data)
        self.metrics["rows_generated"] += count
        self.metrics["generation_count"] += 1
        return df
    
    def apply_corruption(self, data: pd.DataFrame, 
                         corruption_rate: float = 0.05,
                         strategies: Optional[List[str]] = None) -> pd.DataFrame:
        """Apply corruption to numeric data."""
        if strategies is None:
            strategies = ["null", "out_of_range", "type_error"]
        
        corrupted_data = data.copy()
        num_rows = len(corrupted_data)
        num_corrupt = int(num_rows * corruption_rate)
        
        if num_corrupt == 0:
            return corrupted_data
        
        for field_name, field_def in self.schema["fields"].items():
            # Select random indices to corrupt
            corrupt_indices = np.random.choice(num_rows, num_corrupt, replace=False)
            strategy = random.choice(strategies)
            
            if strategy == "null":
                corrupted_data.loc[corrupt_indices, field_name] = np.nan
            
            elif strategy == "out_of_range":
                field_type = field_def["type"]
                min_val = field_def.get("min", 0)
                max_val = field_def.get("max", 100)
                
                # Generate values outside the defined range
                if random.choice([True, False]):
                    # Below minimum
                    if field_type == "integer":
                        corrupted_data.loc[corrupt_indices, field_name] = min_val - np.random.randint(1, 10, size=len(corrupt_indices))
                    else:
                        corrupted_data.loc[corrupt_indices, field_name] = min_val - np.random.uniform(0.1, 10, size=len(corrupt_indices))
                else:
                    # Above maximum
                    if field_type == "integer":
                        corrupted_data.loc[corrupt_indices, field_name] = max_val + np.random.randint(1, 10, size=len(corrupt_indices))
                    else:
                        corrupted_data.loc[corrupt_indices, field_name] = max_val + np.random.uniform(0.1, 10, size=len(corrupt_indices))
            
            elif strategy == "type_error":
                # Add non-numeric values for type errors
                corrupted_data.loc[corrupt_indices, field_name] = "NOT_A_NUMBER"
        
        super().apply_corruption(corrupted_data, corruption_rate, strategies)
        return corrupted_data