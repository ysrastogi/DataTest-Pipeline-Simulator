from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.pipeline.core import Pipeline
from src.pipeline.executor import PipelineExecutor
from src.transformations.base import LambdaTransformation
from src.transformations.filters import RangeFilter
from src.transformations.aggregations import GroupByAggregation
from src.simulator.mock_data import MockDataGenerator
from src.visualization.graph import PipelineGraph
from src.visualization.flow_diagram import FlowDiagramGenerator
from src.visualization.dashboard import DashboardGenerator

# Initialize Spark
spark = SparkSession.builder.appName("PipelineSimulator").getOrCreate()

# Generate mock data
data_generator = MockDataGenerator(spark)
user_df = data_generator.generate_user_data(count=500)
transaction_df = data_generator.generate_transaction_data(count=2000, user_df=user_df)

# Create a pipeline
pipeline = Pipeline(name="User Transaction Analysis")

# Add transformation stages
pipeline.add_stage(
    RangeFilter(column="age", min_value=25, max_value=50, name="Filter Users by Age")
)

pipeline.add_stage(
    LambdaTransformation(
        lambda df: df.withColumn("age_group", 
                                 F.when(F.col("age") < 30, "25-29")
                                 .when(F.col("age") < 40, "30-39")
                                 .otherwise("40-50")),
        name="Add Age Group"
    )
)

pipeline.add_stage(
    GroupByAggregation(
        group_by_cols=["country", "age_group"],
        agg_expressions={
            "user_count": F.count("*"),
            "avg_age": F.avg("age")
        },
        name="Aggregate by Country and Age Group"
    )
)

# Execute the pipeline
executor = PipelineExecutor(spark)
result = executor.execute_pipeline(pipeline, user_df)

# Visualize the pipeline
graph = PipelineGraph(pipeline)
graph.visualize(output_path="outputs/pipeline_graph.png")

flow_generator = FlowDiagramGenerator(pipeline)
flow_generator.generate_html_report(output_path="outputs/pipeline_flow.html")

# Generate a dashboard
dashboard = DashboardGenerator(pipeline, [result])
dashboard.generate_html_dashboard(output_path="outputs/dashboard.html")

print("Pipeline execution completed successfully!")
print(f"Results: {result}")