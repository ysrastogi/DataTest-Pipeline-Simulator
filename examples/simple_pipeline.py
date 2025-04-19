"""
Example of a simple data pipeline using the DataTest Pipeline Simulator.
"""
import os
import pandas as pd
import logging
from pathlib import Path

from core import (
    Pipeline, 
    PipelineStage, 
    Dataset, 
    DatasetType,
    Configuration,
    SparkSessionManager
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create example data
def create_sample_data():
    """Create sample data file for the example."""
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    sample_data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "city": ["New York", "San Francisco", "Chicago", "Boston", "Seattle"]
    }
    
    df = pd.DataFrame(sample_data)
    csv_path = data_dir / "sample_data.csv"
    df.to_csv(csv_path, index=False)
    
    logger.info(f"Created sample data at {csv_path}")
    return str(csv_path)

# Define pipeline stages
class DataLoadStage(PipelineStage):
    """Stage to load data from a file."""
    
    def execute(self, input_data):
        """Load data from CSV file."""
        file_path = input_data
        logger.info(f"Loading data from {file_path}")
        
        # You could use Spark here, but for simplicity using pandas
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows of data")
        
        return df

class DataTransformStage(PipelineStage):
    """Stage to transform the data."""
    
    def execute(self, input_data):
        """Apply transformations to the data."""
        df = input_data
        
        # Simple transformation - convert names to uppercase
        df["name"] = df["name"].str.upper()
        
        # Add a new column
        df["age_group"] = df["age"].apply(
            lambda x: "Young" if x < 30 else ("Middle-aged" if x < 40 else "Senior")
        )
        
        logger.info(f"Transformed data with {len(df)} rows and {len(df.columns)} columns")
        return df

class DataValidationStage(PipelineStage):
    """Stage to validate the data."""
    
    def execute(self, input_data):
        """Validate the transformed data."""
        df = input_data
        
        # Perform some validations
        validation_errors = []
        
        # Check for missing values
        missing_values = df.isnull().sum().sum()
        if missing_values > 0:
            validation_errors.append(f"Found {missing_values} missing values")
        
        # Check age range
        if df["age"].min() < 0 or df["age"].max() > 120:
            validation_errors.append("Age values outside valid range (0-120)")
        
        # Store validation results in metrics
        self.metrics["missing_values"] = missing_values
        self.metrics["validation_errors"] = validation_errors
        self.metrics["is_valid"] = len(validation_errors) == 0
        
        logger.info(f"Validation completed with {len(validation_errors)} errors")
        return df

class DataOutputStage(PipelineStage):
    """Stage to output the processed data."""
    
    def execute(self, input_data):
        """Save the processed data."""
        df = input_data
        
        # Create output directory
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Save as CSV
        output_path = output_dir / "processed_data.csv"
        df.to_csv(output_path, index=False)
        
        # Create dataset object for the output
        output_dataset = Dataset(
            name="processed_data",
            path=str(output_path),
            type=DatasetType.CSV,
            metadata={
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns)
            }
        )
        
        logger.info(f"Saved processed data to {output_path}")
        return output_dataset

def main():
    """Run the example pipeline."""
    logger.info("Starting simple pipeline example")
    
    # Create sample data
    data_path = create_sample_data()
    
    # Create pipeline stages
    load_stage = DataLoadStage(name="data_load")
    transform_stage = DataTransformStage(name="data_transform")
    validation_stage = DataValidationStage(name="data_validation")
    output_stage = DataOutputStage(name="data_output")
    
    # Create and run pipeline
    pipeline = Pipeline(name="simple_data_pipeline")
    pipeline.add_stage(load_stage)
    pipeline.add_stage(transform_stage)
    pipeline.add_stage(validation_stage)
    pipeline.add_stage(output_stage)
    
    # Run the pipeline
    result = pipeline.run(data_path)
    
    # Display results
    logger.info(f"Pipeline completed in {pipeline.metrics['duration_seconds']:.2f} seconds")
    logger.info(f"Output dataset: {result.name} ({result.type.value})")
    logger.info(f"Output path: {result.path}")
    
    # Show metrics from each stage
    for stage in pipeline.stages:
        logger.info(f"Stage '{stage.name}' metrics: {stage.metrics}")
    
    logger.info("Simple pipeline example completed")

if __name__ == "__main__":
    main()