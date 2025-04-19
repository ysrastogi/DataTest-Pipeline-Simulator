from typing import Dict, List, Optional, Union, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from src.transformations.base import Transformation

class GroupByAggregation(Transformation):
    """Apply aggregation functions to grouped data."""
    
    def __init__(self, group_by_cols: List[str], 
                 agg_expressions: Dict[str, Union[str, Column]],
                 name: Optional[str] = None, description: str = ""):
        """
        Args:
            group_by_cols: Columns to group by
            agg_expressions: Dict of output column names to aggregation expressions
                e.g., {"avg_salary": F.avg("salary"), "count": "count"}
        """
        super().__init__(name or "groupby_aggregation", description)
        self.group_by_cols = group_by_cols
        self.agg_expressions = agg_expressions
    
    def execute(self, df: DataFrame) -> DataFrame:
        """Apply groupBy and aggregation to the DataFrame."""
        # Convert string expressions to F.expr
        agg_exprs = {}
        for out_col, expr in self.agg_expressions.items():
            if isinstance(expr, str):
                if expr.lower() == "count":
                    agg_exprs[out_col] = F.count("*")
                else:
                    # Assume it's an expression like "avg(salary)"
                    agg_exprs[out_col] = F.expr(expr)
            else:
                agg_exprs[out_col] = expr
        
        # Create the aggregation
        return df.groupBy(*self.group_by_cols).agg(*[
            agg_expr.alias(out_col) 
            for out_col, agg_expr in agg_exprs.items()
        ])

class WindowAggregation(Transformation):
    """Apply window functions to the DataFrame."""
    
    def __init__(self, partition_by: List[str], order_by: List[str],
                 window_exprs: Dict[str, Column], name: Optional[str] = None, 
                 description: str = ""):
        """
        Args:
            partition_by: Columns to partition by
            order_by: Columns to order by
            window_exprs: Dict of output column names to window expressions
        """
        super().__init__(name or "window_aggregation", description)
        self.partition_by = partition_by
        self.order_by = order_by
        self.window_exprs = window_exprs
    
    def execute(self, df: DataFrame) -> DataFrame:
        """Apply window functions to the DataFrame."""
        from pyspark.sql.window import Window
        
        # Define the window spec
        window_spec = Window.partitionBy(*self.partition_by).orderBy(*self.order_by)
        
        # Apply each window expression
        result_df = df
        for out_col, expr in self.window_exprs.items():
            result_df = result_df.withColumn(out_col, expr.over(window_spec))
        
        return result_df