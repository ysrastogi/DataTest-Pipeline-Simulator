"""
Base class for data generators.
"""
import random
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple

class DataGenerator(ABC):
    """Base class for all data generators."""
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the data generator.
        
        Args:
            seed: Optional seed for random number generation
        """
        self.seed = seed
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
        
        self.metrics = {
            "rows_generated": 0,
            "corruption_applied": False,
            "generation_count": 0
        }
    
    @abstractmethod
    def generate(self, count: int = 1) -> pd.DataFrame:
        """
        Generate data based on the schema.
        
        Args:
            count: Number of data records to generate
            
        Returns:
            DataFrame containing the generated data
        """
        pass
    
    def apply_corruption(self, data: pd.DataFrame, 
                         corruption_rate: float = 0.05,
                         strategies: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Apply corruption to the generated data.
        
        Args:
            data: DataFrame to corrupt
            corruption_rate: Percentage of data to corrupt (0.0-1.0)
            strategies: List of corruption strategies to apply
            
        Returns:
            DataFrame with corrupted data
        """
        # To be implemented in subclasses
        self.metrics["corruption_applied"] = True
        return data
    
    def to_csv(self, data: pd.DataFrame, path: str) -> None:
        """
        Save generated data to CSV.
        
        Args:
            data: DataFrame to save
            path: Path to save the data to
        """
        data.to_csv(path, index=False)
        
    def to_json(self, data: pd.DataFrame, path: str) -> None:
        """
        Save generated data to JSON.
        
        Args:
            data: DataFrame to save
            path: Path to save the data to
        """
        data.to_json(path, orient="records", lines=True)
    
    def reset(self, seed: Optional[int] = None) -> None:
        """
        Reset the generator state.
        
        Args:
            seed: New seed for random number generation
        """
        self.seed = seed if seed is not None else self.seed
        if self.seed is not None:
            random.seed(self.seed)
            np.random.seed(self.seed)
        
        self.metrics = {
            "rows_generated": 0,
            "corruption_applied": False,
            "generation_count": 0
        }