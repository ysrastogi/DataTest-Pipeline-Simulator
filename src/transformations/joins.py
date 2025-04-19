from typing import List, Optional, Dict, Any, Union
from pyspark.sql import DataFrame
from src.transformations.base import Transformation

class JoinTransformation(Transformation):
    """Join the input DataFrame with another DataFrame."""
    
    def __init__(self, right_df: DataFrame, join_type: str = "inner",
                 join_on: Optional[Union[str, List[str]]] = None,
                 join_expr = None, name: Optional[str] = None, 
                 description: str = ""):
        """
        Args:
            right_df: The DataFrame to join with
            join_type: Type of join (inner, left, right, full, etc.)
            join_on: Column(s) to join on (when they have the same name in both DFs)
            join_expr: Join expression (when more complex joining is needed)
        """
        super().__init__(name or f"{join_type}_join", description)
        self.right_df = right_df
        self.join_type = join_type
        
        if join_on and join_expr:
            raise ValueError("Specify either join_on or join_expr, not both")
        
        if not join_on and not join_expr:
            raise ValueError("Either join_on or join_expr must be specified")
        
        self.join_on = join_on
        self.join_expr = join_expr
    
    def execute(self, df: DataFrame) -> DataFrame:
        """Execute the join operation."""
        if self.join_on:
            return df.join(self.right_df, on=self.join_on, how=self.join_type)
        else:
            return df.join(self.right_df, on=self.join_expr, how=self.join_type)

class BroadcastJoinTransformation(JoinTransformation):
    """Perform a broadcast join with another DataFrame."""
    
    def __init__(self, right_df: DataFrame, join_type: str = "inner",
                 join_on: Optional[Union[str, List[str]]] = None,
                 join_expr = None, name: Optional[str] = None, 
                 description: str = ""):
        super().__init__(right_df, join_type, join_on, join_expr, 
                         name or f"broadcast_{join_type}_join", description)
    
    def execute(self, df: DataFrame) -> DataFrame:
        """Execute the broadcast join operation."""
        from pyspark.sql.functions import broadcast
        
        if self.join_on:
            return df.join(broadcast(self.right_df), on=self.join_on, how=self.join_type)
        else:
            return df.join(broadcast(self.right_df), on=self.join_expr, how=self.join_type)