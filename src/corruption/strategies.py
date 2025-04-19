"""
Data corruption strategies for testing data pipelines.
"""
import random
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Callable

class CorruptionStrategy:
    """Base class for corruption strategies."""
    
    @staticmethod
    def apply(data: pd.DataFrame, 
              column: str, 
              corruption_rate: float = 0.05,
              **kwargs) -> pd.DataFrame:
        """
        Apply corruption to a column in the data.
        
        Args:
            data: DataFrame to corrupt
            column: Column name to corrupt
            corruption_rate: Percentage of data to corrupt (0.0-1.0)
            **kwargs: Additional parameters for corruption
        
        Returns:
            DataFrame with corrupted data
        """
        raise NotImplementedError("Subclasses must implement this method")

class NullCorruption(CorruptionStrategy):
    """Replace values with nulls."""
    
    @staticmethod
    def apply(data: pd.DataFrame, 
              column: str, 
              corruption_rate: float = 0.05,
              **kwargs) -> pd.DataFrame:
        """Replace values with nulls."""
        corrupted = data.copy()
        row_count = len(data)
        corrupt_count = int(row_count * corruption_rate)
        if corrupt_count == 0:
            return corrupted
            
        indices = np.random.choice(row_count, corrupt_count, replace=False)
        corrupted.loc[indices, column] = np.nan
        return corrupted

class OutOfRangeCorruption(CorruptionStrategy):
    """Generate out-of-range values."""
    
    @staticmethod
    def apply(data: pd.DataFrame, 
              column: str, 
              corruption_rate: float = 0.05,
              min_val: Optional[Union[int, float]] = None,
              max_val: Optional[Union[int, float]] = None,
              **kwargs) -> pd.DataFrame:
        """Generate out-of-range values."""
        corrupted = data.copy()
        row_count = len(data)
        corrupt_count = int(row_count * corruption_rate)
        if corrupt_count == 0:
            return corrupted
            
        indices = np.random.choice(row_count, corrupt_count, replace=False)
        
        # Determine column data type
        dtype = data[column].dtype
        is_int = np.issubdtype(dtype, np.integer)
        
        # If min/max not provided, try to guess reasonable values
        if min_val is None or max_val is None:
            if pd.api.types.is_numeric_dtype(dtype):
                min_val = data[column].min() - 1 if min_val is None else min_val
                max_val = data[column].max() + 1 if max_val is None else max_val
            else:
                return corrupted  # Can't determine range for non-numeric
        
        # Generate values outside the range
        if random.choice([True, False]):
            # Below minimum
            if is_int:
                corrupted.loc[indices, column] = min_val - np.random.randint(1, 100, size=corrupt_count)
            else:
                corrupted.loc[indices, column] = min_val - np.random.uniform(0.1, 100, size=corrupt_count)
        else:
            # Above maximum
            if is_int:
                corrupted.loc[indices, column] = max_val + np.random.randint(1, 100, size=corrupt_count)
            else:
                corrupted.loc[indices, column] = max_val + np.random.uniform(0.1, 100, size=corrupt_count)
        
        return corrupted

class TypeErrorCorruption(CorruptionStrategy):
    """Insert values with wrong type."""
    
    @staticmethod
    def apply(data: pd.DataFrame, 
              column: str, 
              corruption_rate: float = 0.05,
              **kwargs) -> pd.DataFrame:
        """Insert values with wrong type."""
        corrupted = data.copy()
        row_count = len(data)
        corrupt_count = int(row_count * corruption_rate)
        if corrupt_count == 0:
            return corrupted
            
        indices = np.random.choice(row_count, corrupt_count, replace=False)
        
        # Determine column data type
        dtype = data[column].dtype
        
        # Generate type errors based on column type
        if pd.api.types.is_numeric_dtype(dtype):
            # For numeric columns, insert strings
            corrupted.loc[indices, column] = "NOT_A_NUMBER"
        elif pd.api.types.is_string_dtype(dtype):
            # For string columns, insert dictionaries or other objects
            corrupted.loc[indices, column] = str({"a": 1, "b": 2})
        elif pd.api.types.is_datetime64_dtype(dtype):
            # For datetime columns, insert invalid dates
            corrupted.loc[indices, column] = "NOT_A_DATE"
        
        return corrupted

class DuplicateCorruption(CorruptionStrategy):
    """Create duplicate values where they shouldn't exist."""
    
    @staticmethod
    def apply(data: pd.DataFrame, 
              column: str, 
              corruption_rate: float = 0.05,
              **kwargs) -> pd.DataFrame:
        """Create duplicate values where they shouldn't exist."""
        corrupted = data.copy()
        row_count = len(data)
        corrupt_count = int(row_count * corruption_rate)
        if corrupt_count == 0 or corrupt_count < 2:
            return corrupted
            
        # Select indices to corrupt
        indices = np.random.choice(row_count, corrupt_count, replace=False)
        
        # Take values from the first half and apply to the second half
        mid = len(indices) // 2
        source_values = corrupted.loc[indices[:mid], column].values
        
        # If there are fewer source values than target positions, repeat them
        if len(source_values) < len(indices[mid:]):
            source_values = np.tile(source_values, (len(indices[mid:]) // len(source_values)) + 1)
            source_values = source_values[:len(indices[mid:])]
        
        corrupted.loc[indices[mid:], column] = source_values
        
        return corrupted

class CorruptionFactory:
    """Factory to create and apply corruption strategies."""
    
    strategies = {
        "null": NullCorruption,
        "missing": NullCorruption,
        "out_of_range": OutOfRangeCorruption,
        "type_error": TypeErrorCorruption,
        "duplicate": DuplicateCorruption
    }
    
    @classmethod
    def corrupt_data(cls, 
                    data: pd.DataFrame, 
                    columns: Optional[List[str]] = None,
                    strategy: Union[str, List[str]] = "null", 
                    corruption_rate: float = 0.05,
                    **kwargs) -> pd.DataFrame:
        """
        Apply corruption strategies to data.
        
        Args:
            data: DataFrame to corrupt
            columns: List of columns to corrupt (if None, all columns)
            strategy: Corruption strategy name or list of strategies
            corruption_rate: Percentage of data to corrupt
            **kwargs: Additional parameters for corruption strategies
        
        Returns:
            Corrupted DataFrame
        """
        corrupted = data.copy()
        
        if columns is None:
            columns = data.columns.tolist()
        
        strategies = [strategy] if isinstance(strategy, str) else strategy
        
        for column in columns:
            for strat_name in strategies:
                if strat_name in cls.strategies:
                    strat_class = cls.strategies[strat_name]
                    corrupted = strat_class.apply(
                        corrupted,
                        column=column,
                        corruption_rate=corruption_rate,
                        **kwargs
                    )
        
        return corrupted