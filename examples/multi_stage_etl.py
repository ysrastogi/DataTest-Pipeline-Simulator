from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import time

from src.pipeline.core import Pipeline, Stage
from src.pipeline.executor import PipelineExecutor
from src.transformations.base import LambdaTransformation, Transformation
from src.transformations.filters import FilterTransformation, RangeFilter
from src.transformations.aggregations import GroupByAggregation, WindowAggregation
from src.transformations.joins import JoinTransformation, BroadcastJoinTransformation
from src.simulator.mock_data import MockDataGenerator
from src.simulator.engine import PipelineSimulator
from src.simulator.performance import PerformanceAnalyzer
from src.visualization.graph import PipelineGraph
from src.visualization.flow_diagram import FlowDiagramGenerator
from src.visualization.dashboard import DashboardGenerator

# Custom transformations for this ETL pipeline
class DataQualityCheck(Transformation):
    """Performs data quality checks on a DataFrame."""
    
    def __init__(self, checks, name=None, description=""):
        super().__init__(name or "data_quality_check", description)
        self.checks = checks
    
    def execute(self, df):
        print(f"Running data quality checks: {self.name}")
        for check_name, check_func in self.checks.items():
            result = check_func(df)
            print(f"  Check '{check_name}': {'PASSED' if result else 'FAILED'}")
            if not result:
                raise ValueError(f"Data quality check failed: {check_name}")
        return df

class WriteOutput(Transformation):
    """Simulates writing data to an output location."""
    
    def __init__(self, output_name, mode="overwrite", name=None, description=""):
        super().__init__(name or f"write_{output_name}", description)
        self.output_name = output_name
        self.mode = mode
    
    def execute(self, df):
        # In a real scenario, we would write to storage
        # For simulation, we'll just count and print schema
        row_count = df.count()
        print(f"Writing {row_count} rows to '{self.output_name}'")
        print(f"Schema: {df.schema.simpleString()}")
        
        # Create output directory if needed
        os.makedirs("outputs/data", exist_ok=True)
        
        # Write a small sample to CSV for demonstration
        sample_df = df.limit(10)
        sample_df.coalesce(1).write.mode(self.mode).csv(f"outputs/data/{self.output_name}", header=True)
        
        return df

# Initialize Spark
spark = SparkSession.builder.appName("MultiStageETL").getOrCreate()

# Generate mock data
print("Generating mock data...")
data_generator = MockDataGenerator(spark)
user_df = data_generator.generate_user_data(count=1000)
transaction_df = data_generator.generate_transaction_data(count=5000, user_df=user_df)
product_df = data_generator.generate_product_data(count=200)

# Create staging tables for analytics
user_df.createOrReplaceTempView("users_raw")
transaction_df.createOrReplaceTempView("transactions_raw")
product_df.createOrReplaceTempView("products_raw")

#------------------------------------------------------------------------------------------
# PIPELINE 1: User Data Processing Pipeline
#------------------------------------------------------------------------------------------
print("\n--- BUILDING USER PROCESSING PIPELINE ---")
user_pipeline = Pipeline(name="User Data Processing")

# Stage 1: Data validation
user_pipeline.add_stage(
    DataQualityCheck(
        checks={
            "has_user_id": lambda df: df.filter(F.col("user_id").isNull()).count() == 0,
            "has_valid_age": lambda df: df.filter((F.col("age") < 0) | (F.col("age") > 100)).count() == 0
        },
        name="Validate User Data"
    )
)

# Stage 2: Filter active users
user_pipeline.add_stage(
    FilterTransformation(
        condition=F.col("is_active") == "true",
        name="Filter Active Users"
    )
)

# Stage 3: Enrich user data
user_pipeline.add_stage(
    LambdaTransformation(
        lambda df: df.withColumn("age_group", 
                               F.when(F.col("age") < 18, "Under 18")
                                .when(F.col("age") < 30, "18-29")
                                .when(F.col("age") < 50, "30-49")
                                .otherwise("50+"))
                     .withColumn("registration_year", F.year(F.col("registration_date")))
                     .withColumn("user_segment", 
                               F.when(F.year(F.current_date()) - F.year(F.col("registration_date")) > 1, "Established")
                                .otherwise("New")),
        name="Enrich User Data"
    )
)

# Stage 4: Write processed user data
user_pipeline.add_stage(
    WriteOutput(
        output_name="processed_users",
        name="Write Processed User Data"
    )
)

#------------------------------------------------------------------------------------------
# PIPELINE 2: Transaction Processing Pipeline
#------------------------------------------------------------------------------------------
print("\n--- BUILDING TRANSACTION PROCESSING PIPELINE ---")
transaction_pipeline = Pipeline(name="Transaction Processing")

# Stage 1: Data validation
transaction_pipeline.add_stage(
    DataQualityCheck(
        checks={
            "has_transaction_id": lambda df: df.filter(F.col("transaction_id").isNull()).count() == 0,
            "has_valid_amount": lambda df: df.filter(F.col("amount") <= 0).count() == 0
        },
        name="Validate Transaction Data"
    )
)

# Stage 2: Filter completed transactions
transaction_pipeline.add_stage(
    FilterTransformation(
        condition=F.col("status") == "completed",
        name="Filter Completed Transactions"
    )
)

# Stage 3: Calculate transaction metrics
transaction_pipeline.add_stage(
    WindowAggregation(
        partition_by=["user_id"],
        order_by=["transaction_date"],
        window_exprs={
            "user_cumulative_spend": F.sum(F.col("amount")).over(Window.partitionBy("user_id").orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, 0)),
            "transaction_rank": F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("transaction_date").desc()))
        },
        name="Calculate Transaction Metrics"
    )
)

# Stage 4: Join with user data
transaction_pipeline.add_stage(
    BroadcastJoinTransformation(
        right_df=user_df.select("user_id", "name", "age", "country"),
        join_type="left",
        join_on="user_id",
        name="Join with User Data"
    )
)

# Stage 5: Write processed transaction data
transaction_pipeline.add_stage(
    WriteOutput(
        output_name="processed_transactions",
        name="Write Processed Transaction Data"
    )
)

#------------------------------------------------------------------------------------------
# PIPELINE 3: Analytics Pipeline
#------------------------------------------------------------------------------------------
print("\n--- BUILDING ANALYTICS PIPELINE ---")
analytics_pipeline = Pipeline(name="Customer Analytics")

# Stage 1: Create aggregate spending by country and category
analytics_pipeline.add_stage(
    LambdaTransformation(
        lambda df: spark.sql("""
            SELECT 
                u.country,
                t.category,
                COUNT(DISTINCT t.user_id) as unique_customers,
                SUM(t.amount) as total_spend,
                AVG(t.amount) as avg_transaction_value
            FROM transactions_raw t
            JOIN users_raw u ON t.user_id = u.user_id
            WHERE t.status = 'completed'
            GROUP BY u.country, t.category
            ORDER BY total_spend DESC
        """),
        name="Country-Category Spending Analysis"
    )
)

# Stage 2: Add spending rank
analytics_pipeline.add_stage(
    WindowAggregation(
        partition_by=["country"],
        order_by=[F.col("total_spend").desc()],
        window_exprs={
            "category_rank_in_country": F.row_number().over(
                Window.partitionBy("country").orderBy(F.col("total_spend").desc())
            )
        },
        name="Add Spending Rank"
    )
)

# Stage 3: Filter top categories per country
analytics_pipeline.add_stage(
    FilterTransformation(
        condition=F.col("category_rank_in_country") <= 3,
        name="Filter Top 3 Categories per Country"
    )
)

# Stage 4: Write analytics results
analytics_pipeline.add_stage(
    WriteOutput(
        output_name="country_category_analytics",
        name="Write Analytics Results"
    )
)

#------------------------------------------------------------------------------------------
# EXECUTE PIPELINES
#------------------------------------------------------------------------------------------
print("\n--- EXECUTING PIPELINES ---")
executor = PipelineExecutor(spark)

# Create output directories
os.makedirs("outputs/visualizations", exist_ok=True)
os.makedirs("outputs/reports", exist_ok=True)

# Execute and visualize user pipeline
print("\nExecuting User Data Processing pipeline...")
user_result = executor.execute_pipeline(user_pipeline, user_df)
PipelineGraph(user_pipeline).visualize(output_path="outputs/visualizations/user_pipeline_graph.png", show=False)
FlowDiagramGenerator(user_pipeline).generate_html_report(output_path="outputs/reports/user_pipeline_flow.html")

# Execute and visualize transaction pipeline
print("\nExecuting Transaction Processing pipeline...")
transaction_result = executor.execute_pipeline(transaction_pipeline, transaction_df)
PipelineGraph(transaction_pipeline).visualize(output_path="outputs/visualizations/transaction_pipeline_graph.png", show=False)
FlowDiagramGenerator(transaction_pipeline).generate_html_report(output_path="outputs/reports/transaction_pipeline_flow.html")

# Execute and visualize analytics pipeline
print("\nExecuting Analytics pipeline...")
analytics_result = executor.execute_pipeline(analytics_pipeline, None)  # Input is from SQL query
PipelineGraph(analytics_pipeline).visualize(output_path="outputs/visualizations/analytics_pipeline_graph.png", show=False)
FlowDiagramGenerator(analytics_pipeline).generate_html_report(output_path="outputs/reports/analytics_pipeline_flow.html")

#------------------------------------------------------------------------------------------
# PERFORMANCE ANALYSIS
#------------------------------------------------------------------------------------------
print("\n--- RUNNING PERFORMANCE ANALYSIS ---")

# Create simulation scenarios
scenarios = [
    {
        "name": "baseline",
        "description": "Baseline execution"
    },
    {
        "name": "large_data",
        "description": "Large data volume",
        "data_scale_factor": 2.0
    },
    {
        "name": "small_data",
        "description": "Small data volume",
        "data_scale_factor": 0.5
    }
]

# Run simulation for user pipeline
simulator = PipelineSimulator(spark)
print("\nSimulating User Pipeline with different scenarios...")
user_sim_results = simulator.simulate_pipeline(
    pipeline=user_pipeline,
    input_df=user_df,
    scenarios=scenarios,
    iterations=2
)

# Analyze performance
analyzer = PerformanceAnalyzer()
performance_report = analyzer.generate_performance_report(
    user_sim_results,
    output_path="outputs/reports/user_pipeline_performance.md"
)
analyzer.plot_performance_charts(
    user_sim_results,
    output_dir="outputs/visualizations/performance"
)

#------------------------------------------------------------------------------------------
# GENERATE DASHBOARDS
#------------------------------------------------------------------------------------------
print("\n--- GENERATING DASHBOARDS ---")

# Combine all execution results
all_results = [user_result, transaction_result, analytics_result]

# Generate individual dashboards
DashboardGenerator(user_pipeline, [user_result]).generate_html_dashboard(
    output_path="outputs/reports/user_pipeline_dashboard.html"
)

DashboardGenerator(transaction_pipeline, [transaction_result]).generate_html_dashboard(
    output_path="outputs/reports/transaction_pipeline_dashboard.html"
)

DashboardGenerator(analytics_pipeline, [analytics_result]).generate_html_dashboard(
    output_path="outputs/reports/analytics_pipeline_dashboard.html"
)

# Generate a master dashboard with all pipelines
with open("outputs/reports/master_dashboard.html", "w") as f:
    f.write("""<!DOCTYPE html>
<html>
<head>
    <title>ETL Pipelines Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 0; }
        .header { background-color: #2c3e50; color: white; padding: 20px; text-align: center; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .pipeline-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; }
        .pipeline-card { background-color: white; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); padding: 20px; }
        h1, h2 { margin-top: 0; }
        iframe { width: 100%; height: 500px; border: none; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Multi-Stage ETL Process Dashboard</h1>
        <p>Comprehensive view of all data pipelines</p>
    </div>
    
    <div class="container">
        <h2>Pipeline Dashboards</h2>
        <div class="pipeline-grid">
            <div class="pipeline-card">
                <h3>User Data Processing</h3>
                <p>Pipeline for processing and enriching user data</p>
                <a href="user_pipeline_dashboard.html" target="_blank">View Dashboard</a>
                <iframe src="user_pipeline_flow.html"></iframe>
            </div>
            <div class="pipeline-card">
                <h3>Transaction Processing</h3>
                <p>Pipeline for processing transaction data</p>
                <a href="transaction_pipeline_dashboard.html" target="_blank">View Dashboard</a>
                <iframe src="transaction_pipeline_flow.html"></iframe>
            </div>
            <div class="pipeline-card">
                <h3>Customer Analytics</h3>
                <p>Pipeline for generating customer analytics</p>
                <a href="analytics_pipeline_dashboard.html" target="_blank">View Dashboard</a>
                <iframe src="analytics_pipeline_flow.html"></iframe>
            </div>
        </div>
        
        <h2>Performance Analysis</h2>
        <div class="pipeline-card">
            <h3>User Pipeline Performance</h3>
            <iframe src="user_pipeline_performance.md" style="height: 300px;"></iframe>
        </div>
    </div>
</body>
</html>""")

print("\n--- ETL PROCESS COMPLETED SUCCESSFULLY ---")
print(f"Dashboards and reports generated in the 'outputs' directory")

# Summary statistics
print("\n--- SUMMARY STATISTICS ---")
print(f"User pipeline: {len(user_pipeline.stages)} stages, execution time: {user_result.get('execution_time', 0):.2f}s")
print(f"Transaction pipeline: {len(transaction_pipeline.stages)} stages, execution time: {transaction_result.get('execution_time', 0):.2f}s")
print(f"Analytics pipeline: {len(analytics_pipeline.stages)} stages, execution time: {analytics_result.get('execution_time', 0):.2f}s")