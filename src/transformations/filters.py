from typing import Any, List, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from src.transformations.base import Transformation

class FilterTransformation(Transformation):
    """Filters rows based on a condition."""
    
    def __init__(self, condition: Union[str, Column], name: Optional[str] = None, 
                 description: str = ""):
        super().__init__(name or "filter_transformation", description)
        self.condition = condition
    
    def execute(self, df: DataFrame) -> DataFrame:
        """Apply the filter condition to the DataFrame."""
        return df.filter(self.condition)

class RangeFilter(FilterTransformation):
    """Filter rows where a column value is within a specified range."""
    
    def __init__(self, column: str, min_value: Optional[Any] = None, 
                 max_value: Optional[Any] = None, inclusive: bool = True,
                 name: Optional[str] = None, description: str = ""):
        
        if min_value is None and max_value is None:
            raise ValueError("At least one of min_value or max_value must be specified")
        
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.inclusive = inclusive
        
        # Build the condition
        condition = None
        col_ref = F.col(column)
        
        if min_value is not None and max_value is not None:
            if inclusive:
                condition = (col_ref >= min_value) & (col_ref <= max_value)
            else:
                condition = (col_ref > min_value) & (col_ref < max_value)
        elif min_value is not None:
            condition = col_ref >= min_value if inclusive else col_ref > min_value
        elif max_value is not None:
            condition = col_ref <= max_value if inclusive else col_ref < max_value
        
        name = name or f"range_filter_{column}"
        desc = description or f"Filter {column} between {min_value} and {max_value}"
        
        super().__init__(condition, name, desc)

class ValueFilter(FilterTransformation):
    """Filter rows where a column value matches one of the specified values."""
    
    def __init__(self, column: str, values: List[Any], include: bool = True,
                 name: Optional[str] = None, description: str = ""):
        
        self.column = column
        self.values = values
        self.include = include
        
        # Build the condition
        col_ref = F.col(column)
        condition = col_ref.isin(values) if include else ~col_ref.isin(values)
        
        action = "include" if include else "exclude"
        name = name or f"{action}_values_{column}"
        desc = description or f"{action.capitalize()} rows where {column} is in {values}"
        
        super().__init__(condition, name, desc)