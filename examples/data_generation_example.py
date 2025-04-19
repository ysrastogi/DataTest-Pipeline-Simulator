"""
Example of using the data generation pipeline.
"""
import os
import logging
from pathlib import Path

from src.schema.definition import SchemaDefinition
from src.generators.numeric import NumericGenerator
from src.corruption.strategies import CorruptionFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    """Run the data generation example."""
    logger.info("Starting data generation example")
    
    # Create output directory
    output_dir = Path(__file__).parent / "generated_data"
    output_dir.mkdir(exist_ok=True)
    
    # Define schema
    schema = SchemaDefinition()
    schema.add_numeric_field("id", field_type="integer", min_val=1000, max_val=9999)
    schema.add_numeric_field("value", field_type="float", min_val=0, max_val=1000, 
                           distribution="normal", mean=500, std=150, decimals=2)
    schema.add_numeric_field("quantity", field_type="integer", min_val=1, max_val=100)
    
    # Create generator
    generator = NumericGenerator(schema.schema, seed=42)
    
    # Generate data
    data = generator.generate(count=10000)
    logger.info(f"Generated {len(data)} rows of data")
    
    # Save clean data
    clean_path = output_dir / "clean_data.csv"
    generator.to_csv(data, str(clean_path))
    logger.info(f"Saved clean data to {clean_path}")
    
    # Generate corrupted data
    corrupted_data = generator.apply_corruption(data, corruption_rate=0.05)
    
    # Save corrupted data
    corrupted_path = output_dir / "corrupted_data.csv"
    generator.to_csv(corrupted_data, str(corrupted_path))
    logger.info(f"Saved corrupted data to {corrupted_path}")
    
    # Use corruption factory for more control
    heavily_corrupted = CorruptionFactory.corrupt_data(
        data,
        columns=["value", "quantity"],
        strategy=["null", "out_of_range", "type_error"],
        corruption_rate=0.15
    )
    
    # Save heavily corrupted data
    heavy_path = output_dir / "heavily_corrupted_data.csv"
    generator.to_csv(heavily_corrupted, str(heavy_path))
    logger.info(f"Saved heavily corrupted data to {heavy_path}")
    
    logger.info("Data generation example completed")

if __name__ == "__main__":
    main()