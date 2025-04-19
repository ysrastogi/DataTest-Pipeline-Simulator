# Example usage
import os
import sys
import pandas as pd
import time
import logging
from pathlib import Path
from performance import MetricsCollector, Profiler, BenchmarkRunner, BenchmarkConfig, MetricsVisualizer
from core.spark import SparkSessionManager

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("metrics_demo")

# Get absolute paths
current_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(current_dir)
data_dir = os.path.join(base_dir, "data")
metrics_dir = os.path.join(base_dir, "metrics")
benchmark_dir = os.path.join(base_dir, "benchmark_results")
charts_dir = os.path.join(base_dir, "performance_charts")

# Create necessary directories with absolute paths
for directory in [data_dir, metrics_dir, benchmark_dir, charts_dir]:
    os.makedirs(directory, exist_ok=True)
    logger.info(f"Created directory: {directory}")

# Create sample data files if they don't exist
def create_sample_data():
    """Create sample data files for testing."""
    default_path = os.path.join(data_dir, "input.csv")
    small_path = os.path.join(data_dir, "small_input.csv")
    large_path = os.path.join(data_dir, "large_input.csv")
    
    if not os.path.exists(default_path):
        # Create a small default dataset
        df = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Person {i}' for i in range(1, 101)],
            'age': [20 + i % 50 for i in range(1, 101)],
            'department': ['HR', 'IT', 'Finance', 'Marketing', 'Sales'] * 20,
            'salary': [50000 + i * 1000 for i in range(1, 101)]
        })
        df.to_csv(default_path, index=False)
        logger.info(f"Created default dataset: {default_path} ({len(df)} rows)")
        
        # Create small dataset
        small_df = df.head(30)
        small_df.to_csv(small_path, index=False)
        logger.info(f"Created small dataset: {small_path} ({len(small_df)} rows)")
        
        # Create large dataset
        large_rows = []
        for i in range(5):  # 5x the data
            temp_df = df.copy()
            temp_df['id'] = temp_df['id'] + (i+1) * 100
            large_rows.append(temp_df)
        large_df = pd.concat(large_rows)
        large_df.to_csv(large_path, index=False)
        logger.info(f"Created large dataset: {large_path} ({len(large_df)} rows)")

# Create the sample data
create_sample_data()

# Create a global metrics collector
metrics_collector = MetricsCollector(storage_path=metrics_dir)

# Patch the save method to ensure it works correctly
def safe_save(self, filepath):
    """Save metrics to a file, ensuring the directory exists."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            f.write(self.to_json())
        logger.info(f"Saved metrics to {filepath}")
    except Exception as e:
        logger.error(f"Could not save metrics to {filepath}: {e}")

# Apply the patch to the PipelineMetrics class
from performance.metrics import PipelineMetrics
PipelineMetrics.save = safe_save

# 1. Basic metrics collection
def run_pipeline(input_size=None, partitions=None, **kwargs):
    """
    Run a sample pipeline with metrics collection
    
    Args:
        input_size: Size of input data (small, medium, large)
        partitions: Number of partitions to use
        **kwargs: Additional parameters from benchmark config
    """
    # Initialize components
    spark_manager = SparkSessionManager()
    
    # Use the global metrics collector
    global metrics_collector
    
    # Configure based on parameters
    input_file = os.path.join(data_dir, "input.csv")
    if input_size == "small":
        input_file = os.path.join(data_dir, "small_input.csv")
    elif input_size == "large":
        input_file = os.path.join(data_dir, "large_input.csv")
    
    output_path = os.path.join(data_dir, "output.parquet")
    
    logger.info(f"Running pipeline with input: {input_file}, partitions: {partitions}")
    
    # Run pipeline with metrics collection
    try:
        with metrics_collector.pipeline_metrics("my_pipeline"):
            spark = spark_manager.get_session()
            
            # Apply partitioning if specified
            if partitions:
                spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
            
            # Stage 1: Load data
            with metrics_collector.stage_metrics("load_data"):
                df = spark.read.csv(input_file, header=True)
                count = df.count()
                metrics_collector.record_count(count)
                logger.info(f"Loaded {count} rows from {input_file}")
            
            # Stage 2: Transform data
            with metrics_collector.stage_metrics("transform_data"):
                df = df.filter("age > 18").groupBy("department").count()
                count = df.count()
                metrics_collector.record_count(count)
                logger.info(f"Transformed data: {count} department groups")
            
            # Stage 3: Write results
            with metrics_collector.stage_metrics("write_results"):
                # Clean up existing directory if it exists
                if os.path.exists(output_path):
                    import shutil
                    shutil.rmtree(output_path)
                    logger.info(f"Removed existing output directory: {output_path}")
                
                # Write to parquet
                df.write.mode("overwrite").parquet(output_path)
                logger.info(f"Wrote results to {output_path}")
        
        logger.info("Pipeline completed successfully")
        return metrics_collector.history[-1] if metrics_collector.history else None
        
    except Exception as e:
        logger.error(f"Error in pipeline execution: {e}", exc_info=True)
        return None

# Run the pipeline once to generate the output files
logger.info("Running pipeline once to generate output files...")
initial_result = run_pipeline(input_size="small", partitions=2)

# 2. Using the profiler
profiler = Profiler()

@profiler.profile_function
def process_data(spark, input_path):
    """Process data from a Parquet file."""
    if not os.path.exists(input_path):
        logger.error(f"Input path does not exist: {input_path}")
        return None
    
    try:
        df = spark.read.parquet(input_path)
        result = df.groupBy("department").agg({"count": "sum"})
        count = result.count()
        logger.info(f"Processed {count} department summaries")
        return result
    except Exception as e:
        logger.error(f"Error processing data: {e}", exc_info=True)
        return None

# Run the profiler only if we have output data
output_path = os.path.join(data_dir, "output.parquet")
if os.path.exists(output_path):
    logger.info(f"Running profiler on {output_path}")
    spark_manager = SparkSessionManager()
    process_data(spark_manager.get_session(), output_path)
else:
    logger.warning(f"Output path not found: {output_path}, skipping profiler")

# 3. Running benchmarks
benchmark_runner = BenchmarkRunner(output_dir=benchmark_dir)

config = BenchmarkConfig(
    name="pipeline_benchmark",
    description="Benchmark for data processing pipeline",
    iterations=2,  # Reduced iterations for faster testing
    warm_up_iterations=0,  # Skip warm-up to avoid issues
    parameters={
        "input_size": "small",  # Using small dataset for testing
        "partitions": 2
    }
)

try:
    logger.info(f"Running benchmark with config: {config.name}")
    result = benchmark_runner.run_benchmark(config, run_pipeline)
    logger.info(f"Benchmark completed: {result.summary if hasattr(result, 'summary') else 'No results'}")
except Exception as e:
    logger.error(f"Error during benchmark: {e}", exc_info=True)
    result = None

# 4. Visualizing results
visualizer = MetricsVisualizer(output_dir=charts_dir)

try:
    if metrics_collector.history:
        logger.info(f"Creating visualizations with {len(metrics_collector.history)} metrics records")
        
        # Pipeline duration chart
        duration_chart = os.path.join(charts_dir, "pipeline_duration.png")
        visualizer.plot_pipeline_duration(metrics_collector.history, save_path=duration_chart)
        logger.info(f"Created pipeline duration chart: {duration_chart}")
        
        # Stage breakdown chart
        if metrics_collector.history[-1].stages:
            stage_chart = os.path.join(charts_dir, "stage_breakdown.png")
            visualizer.plot_stage_breakdown(metrics_collector.history[-1], save_path=stage_chart)
            logger.info(f"Created stage breakdown chart: {stage_chart}")
        
        # Dashboard
        if result:
            dashboard = os.path.join(charts_dir, "dashboard.html")
            visualizer.create_performance_dashboard(metrics_collector.history, [result], "dashboard.html")
            logger.info(f"Created performance dashboard: {dashboard}")
    else:
        logger.warning("No metrics collected, skipping visualization")
except Exception as e:
    logger.error(f"Error during visualization: {e}", exc_info=True)

logger.info("Demo completed successfully")