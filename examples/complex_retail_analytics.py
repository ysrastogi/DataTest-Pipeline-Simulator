from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import time

from src.pipeline.core import Pipeline, Stage
from src.pipeline.executor import PipelineExecutor
from src.transformations.base import LambdaTransformation, Transformation
from src.transformations.filters import RangeFilter, ValueFilter
from src.transformations.aggregations import GroupByAggregation, WindowAggregation
from src.transformations.joins import JoinTransformation, BroadcastJoinTransformation
from src.simulator.mock_data import MockDataGenerator
from src.simulator.engine import PipelineSimulator
from src.simulator.performance import PerformanceAnalyzer
from src.visualization.graph import PipelineGraph
from src.visualization.flow_diagram import FlowDiagramGenerator
from src.visualization.dashboard import DashboardGenerator

# Custom transformation for joining transaction data with user data
class TransactionEnrichment(Transformation):
    """Enrich transaction data with user information."""
    
    def __init__(self, user_df: DataFrame, name: str = "Transaction Enrichment"):
        super().__init__(name=name, description="Join transactions with user data")
        self.user_df = user_df
    
    def execute(self, df: DataFrame) -> DataFrame:
        """Join transaction data with user data to enrich transactions."""
        # Select only needed columns from user data to reduce join size
        slim_user_df = self.user_df.select("user_id", "name", "age", "country")
        
        # Perform the join
        enriched_df = df.join(
            slim_user_df,
            on="user_id",
            how="left"
        )
        
        # Add a flag for transactions with missing user data
        result_df = enriched_df.withColumn(
            "has_user_data", 
            F.when(F.col("name").isNotNull(), True).otherwise(False)
        )
        
        return result_df

# Custom transformation for transaction categorization
class TransactionCategorizer(Transformation):
    """Categorize transactions into spending patterns."""
    
    def __init__(self, name: str = "Transaction Categorizer"):
        super().__init__(name=name, description="Categorize transactions by amount and type")
    
    def execute(self, df: DataFrame) -> DataFrame:
        """Add spending category based on transaction amount."""
        return df.withColumn(
            "spending_category",
            F.when(F.col("amount") < 50, "low")
             .when(F.col("amount") < 200, "medium")
             .otherwise("high")
        ).withColumn(
            "is_international",
            F.when(F.col("country") != "United States", True).otherwise(False)
        )

# Main execution function
def run_retail_analytics_pipeline():
    # Initialize Spark
    spark = SparkSession.builder.appName("RetailAnalyticsPipeline").getOrCreate()
    
    # Create output directory
    output_dir = "outputs/retail_analytics"
    os.makedirs(output_dir, exist_ok=True)
    
    print("Generating mock data...")
    # Generate mock data
    data_generator = MockDataGenerator(spark)
    user_df = data_generator.generate_user_data(count=1000)
    transaction_df = data_generator.generate_transaction_data(count=5000, user_df=user_df)
    product_df = data_generator.generate_product_data(count=300)
    
    # Cache the dataframes for better performance
    user_df.cache()
    transaction_df.cache()
    product_df.cache()
    
    # Show sample data
    print("\nSample User Data:")
    user_df.show(5)
    
    print("\nSample Transaction Data:")
    transaction_df.show(5)
    
    print("\nSample Product Data:")
    product_df.show(5)
    
    # Create a pipeline for user analytics
    print("\nBuilding User Analytics Pipeline...")
    user_pipeline = Pipeline(name="User Demographics Analysis")
    
    # Add transformation stages for user pipeline
    user_pipeline.add_stage(
        RangeFilter(column="age", min_value=18, max_value=70, name="Filter Valid User Age")
    )
    
    user_pipeline.add_stage(
        LambdaTransformation(
            lambda df: df.withColumn("age_decade", F.floor(F.col("age") / 10) * 10),
            name="Add Age Decade"
        )
    )
    
    user_pipeline.add_stage(
        GroupByAggregation(
            group_by_cols=["country", "age_decade"],
            agg_expressions={
                "user_count": F.count("*"),
                "active_users": F.sum(F.when(F.col("is_active") == "true", 1).otherwise(0))
            },
            name="Aggregate User Demographics"
        )
    )
    
    # Create a pipeline for transaction analytics
    print("Building Transaction Analytics Pipeline...")
    transaction_pipeline = Pipeline(name="Transaction Analysis")
    
    # Add transformation stages for transaction pipeline
    transaction_pipeline.add_stage(
        ValueFilter(column="status", values=["completed"], name="Filter Completed Transactions")
    )
    
    transaction_pipeline.add_stage(
        TransactionEnrichment(user_df, name="Enrich with User Data")
    )
    
    transaction_pipeline.add_stage(
        TransactionCategorizer(name="Categorize Transactions")
    )
    
    # Add window functions for time-based analysis
    transaction_pipeline.add_stage(
        WindowAggregation(
            partition_by=["user_id"],
            order_by=["transaction_date"],
            window_exprs={
                "user_running_total": F.sum("amount").over(
                    Window.partitionBy("user_id").orderBy("transaction_date").rowsBetween(
                        Window.unboundedPreceding, Window.currentRow
                    )
                ),
                "transaction_number": F.row_number().over(
                    Window.partitionBy("user_id").orderBy("transaction_date")
                )
            },
            name="Add User Transaction Metrics"
        )
    )
    
    # Add aggregations by category
    transaction_pipeline.add_stage(
        GroupByAggregation(
            group_by_cols=["category", "spending_category"],
            agg_expressions={
                "transaction_count": F.count("*"),
                "total_amount": F.sum("amount"),
                "avg_amount": F.avg("amount")
            },
            name="Aggregate by Category"
        )
    )
    
    # Create a pipeline for joining product and transaction data
    print("Building Product-Transaction Pipeline...")
    product_transaction_pipeline = Pipeline(name="Product Transaction Integration")
    
    # First, create a staging DataFrame with product category in transactions
    # This would typically come from another system
    product_categories = product_df.select("category").distinct()
    transaction_with_product = transaction_df.join(
        product_categories, 
        transaction_df["category"] == product_categories["category"],
        "inner"
    )
    
    # Now build the pipeline stages
    product_transaction_pipeline.add_stage(
        BroadcastJoinTransformation(
            right_df=product_df.select("category", "price", "rating"),
            join_on="category",
            join_type="left",
            name="Join Transaction with Product Info"
        )
    )
    
    product_transaction_pipeline.add_stage(
        LambdaTransformation(
            lambda df: df.withColumn(
                "price_multiplier", 
                F.when(F.col("rating") > 4.0, 1.2).otherwise(1.0)
            ),
            name="Calculate Price Multiplier"
        )
    )
    
    product_transaction_pipeline.add_stage(
        GroupByAggregation(
            group_by_cols=["category"],
            agg_expressions={
                "avg_transaction_amount": F.avg("amount"),
                "avg_product_price": F.avg("price"),
                "avg_price_multiplier": F.avg("price_multiplier"),
                "total_transactions": F.count("*")
            },
            name="Product Category Analysis"
        )
    )
    
    # Execute the pipelines
    print("\nExecuting User Analytics Pipeline...")
    user_executor = PipelineExecutor(spark)
    user_result = user_executor.execute_pipeline(user_pipeline, user_df)
    
    print("\nExecuting Transaction Analytics Pipeline...")
    transaction_executor = PipelineExecutor(spark)
    transaction_result = transaction_executor.execute_pipeline(transaction_pipeline, transaction_df)
    
    print("\nExecuting Product-Transaction Pipeline...")
    product_transaction_executor = PipelineExecutor(spark)
    product_transaction_result = product_transaction_executor.execute_pipeline(
        product_transaction_pipeline, transaction_with_product
    )
    
    # Generate visualizations for each pipeline
    print("\nGenerating pipeline visualizations...")
    
    # User pipeline visualization
    user_graph = PipelineGraph(user_pipeline)
    user_graph.visualize(output_path=f"{output_dir}/user_pipeline_graph.png", show=False)
    
    user_flow_generator = FlowDiagramGenerator(user_pipeline)
    user_flow_generator.generate_html_report(output_path=f"{output_dir}/user_pipeline_flow.html")
    
    # Transaction pipeline visualization
    transaction_graph = PipelineGraph(transaction_pipeline)
    transaction_graph.visualize(output_path=f"{output_dir}/transaction_pipeline_graph.png", show=False)
    
    transaction_flow_generator = FlowDiagramGenerator(transaction_pipeline)
    transaction_flow_generator.generate_html_report(output_path=f"{output_dir}/transaction_pipeline_flow.html")
    
    # Product-Transaction pipeline visualization
    product_transaction_graph = PipelineGraph(product_transaction_pipeline)
    product_transaction_graph.visualize(output_path=f"{output_dir}/product_transaction_pipeline_graph.png", show=False)
    
    product_transaction_flow_generator = FlowDiagramGenerator(product_transaction_pipeline)
    product_transaction_flow_generator.generate_html_report(output_path=f"{output_dir}/product_transaction_pipeline_flow.html")
    
    # Now run simulations with different configurations
    print("\nRunning pipeline simulations...")
    
    # Define simulation scenarios
    scenarios = [
        {
            "name": "baseline",
            "description": "Baseline execution with default parameters"
        },
        {
            "name": "small_data",
            "description": "Small data volume simulation",
            "data_scale_factor": 0.3
        },
        {
            "name": "large_data",
            "description": "Large data volume simulation",
            "data_scale_factor": 1.5
        }
    ]
    
    # Run simulation for transaction pipeline (most complex one)
    simulator = PipelineSimulator(spark)
    simulation_results = simulator.simulate_pipeline(
        transaction_pipeline, 
        transaction_df,
        scenarios=scenarios,
        iterations=3
    )
    
    # Analyze performance
    print("\nAnalyzing performance metrics...")
    performance_analyzer = PerformanceAnalyzer()
    performance_report = performance_analyzer.generate_performance_report(
        simulation_results,
        output_path=f"{output_dir}/performance_report.md"
    )
    
    # Generate performance charts
    performance_analyzer.plot_performance_charts(
        simulation_results,
        output_dir=f"{output_dir}/performance_charts"
    )
    
    # Create dashboard for transaction pipeline
    print("\nGenerating dashboards...")
    transaction_dashboard = DashboardGenerator(transaction_pipeline, simulation_results)
    transaction_dashboard.generate_html_dashboard(output_path=f"{output_dir}/transaction_dashboard.html")
    
    # Save execution history
    transaction_executor.save_execution_history(f"{output_dir}/transaction_execution_history.json")
    
    print("\nRetail analytics pipeline execution completed!")
    print(f"All outputs saved to {output_dir}")

if __name__ == "__main__":
    run_retail_analytics_pipeline()