"""
Examples of using the schema definition system for data generation.
"""
import os
import json
import logging
from pathlib import Path

from src.schema.definition import SchemaDefinition
from src.generators.numeric import NumericGenerator
# Import other generators as needed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def basic_schema_example():
    """Create and use a basic schema."""
    logger.info("Creating a basic schema example")
    
    # Create schema definition
    schema = SchemaDefinition()
    
    # Add various field types
    schema.add_numeric_field("user_id", field_type="integer", min_val=1000, max_val=9999)
    schema.add_numeric_field("account_balance", field_type="float", min_val=0, max_val=10000, 
                           distribution="normal", mean=2500, std=1500, decimals=2)
    schema.add_text_field("username", min_length=5, max_length=15)
    schema.add_text_field("email", pattern=r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}")
    schema.add_categorical_field("status", categories=["active", "inactive", "pending", "suspended"],
                               weights=[0.7, 0.1, 0.15, 0.05])
    schema.add_temporal_field("created_at", field_type="datetime", 
                            start_date="2020-01-01", end_date="2023-12-31",
                            format="%Y-%m-%d %H:%M:%S")
    
    # Validate the schema
    try:
        schema.validate()
        logger.info("Schema is valid")
    except ValueError as e:
        logger.error(f"Schema validation failed: {e}")
        return
    
    # Save schema to file
    output_dir = Path(__file__).parent / "schemas"
    output_dir.mkdir(exist_ok=True)
    schema_path = output_dir / "user_schema.json"
    schema.save_schema(str(schema_path))
    logger.info(f"Saved schema to {schema_path}")
    
    # Print schema for inspection
    logger.info(f"Schema structure: {json.dumps(schema.schema, indent=2)}")
    
    return schema

def load_and_extend_schema():
    """Load a schema from file and extend it."""
    schema_path = Path(__file__).parent / "schemas" / "user_schema.json"
    
    if not schema_path.exists():
        logger.error(f"Schema file not found: {schema_path}")
        return None
    
    logger.info(f"Loading schema from {schema_path}")
    schema = SchemaDefinition(schema_path=str(schema_path))
    
    # Extend the schema with additional fields
    schema.add_numeric_field("login_count", field_type="integer", min_val=0, max_val=1000)
    schema.add_temporal_field("last_login", field_type="datetime", 
                            start_date="2023-01-01", end_date="2023-12-31")
    schema.add_categorical_field("subscription_type", 
                               categories=["free", "basic", "premium", "enterprise"],
                               weights=[0.4, 0.3, 0.2, 0.1])
    
    # Save extended schema
    extended_path = Path(__file__).parent / "schemas" / "extended_user_schema.json"
    schema.save_schema(str(extended_path))
    logger.info(f"Saved extended schema to {extended_path}")
    
    return schema

def create_complex_schema():
    """Create a more complex schema with nested structures."""
    logger.info("Creating a complex schema")
    
    # Create schema definition
    schema = SchemaDefinition()
    
    # Add fields for a product catalog
    schema.add_numeric_field("product_id", field_type="integer", min_val=10000, max_val=99999)
    schema.add_text_field("product_name", min_length=5, max_length=50)
    schema.add_numeric_field("price", field_type="float", min_val=0.99, max_val=999.99, decimals=2)
    schema.add_numeric_field("inventory", field_type="integer", min_val=0, max_val=1000)
    schema.add_categorical_field("category", categories=[
        "Electronics", "Clothing", "Home & Kitchen", "Books", "Toys", "Sports", "Beauty"
    ])
    schema.add_categorical_field("rating", categories=["1", "2", "3", "4", "5"], 
                              weights=[0.05, 0.1, 0.2, 0.3, 0.35])
    
    # Add fields for metadata
    schema.add_temporal_field("created_at", field_type="datetime")
    schema.add_temporal_field("updated_at", field_type="datetime")
    schema.add_categorical_field("status", categories=["active", "discontinued", "out_of_stock"])
    
    # Custom field for tags (represented as a JSON array in string format)
    schema.add_field("tags", {
        "type": "json_array",
        "min_items": 1,
        "max_items": 5,
        "possible_values": [
            "bestseller", "new", "sale", "eco-friendly", "limited-edition", 
            "handmade", "imported", "organic", "premium", "exclusive"
        ]
    })
    
    # Validate and save schema
    schema.validate()
    output_dir = Path(__file__).parent / "schemas"
    output_dir.mkdir(exist_ok=True)
    schema_path = output_dir / "product_schema.json"
    schema.save_schema(str(schema_path))
    logger.info(f"Saved complex schema to {schema_path}")
    
    return schema

def create_data_quality_schema():
    """Create a schema with data quality constraints."""
    logger.info("Creating schema with data quality constraints")
    
    schema = SchemaDefinition()
    
    # Add fields with data quality constraints
    schema.add_numeric_field("customer_id", field_type="integer", min_val=1, max_val=100000,
                           unique=True)  # Unique constraint
    
    schema.add_text_field("email", min_length=5, max_length=100,
                        pattern=r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}",
                        unique=True)  # Unique with pattern constraint
    
    schema.add_text_field("phone", pattern=r"\d{3}-\d{3}-\d{4}",
                        null_probability=0.1)  # Can be null with 10% probability
    
    schema.add_numeric_field("age", field_type="integer", min_val=18, max_val=100,
                           distribution="normal", mean=35, std=12)
    
    schema.add_numeric_field("income", field_type="float", min_val=0, max_val=250000,
                           null_probability=0.2)  # Can be null with 20% probability
    
    schema.add_categorical_field("marital_status", 
                              categories=["single", "married", "divorced", "widowed"],
                              null_probability=0.05)  # Can be null with 5% probability
    
    # Add field with custom validation rule
    schema.add_field("credit_score", {
        "type": "integer",
        "min": 300,
        "max": 850,
        "distribution": "normal",
        "mean": 680,
        "std": 100,
        "validation": {
            "rule": "value >= 300 and value <= 850",
            "message": "Credit score must be between 300 and 850"
        }
    })
    
    # Validate and save schema
    schema.validate()
    output_dir = Path(__file__).parent / "schemas"
    output_dir.mkdir(exist_ok=True)
    schema_path = output_dir / "customer_quality_schema.json"
    schema.save_schema(str(schema_path))
    logger.info(f"Saved data quality schema to {schema_path}")
    
    return schema

def schema_with_relationships():
    """Create schemas with relationships between them."""
    logger.info("Creating schemas with relationships")
    
    # Create customer schema
    customer_schema = SchemaDefinition()
    customer_schema.add_numeric_field("customer_id", field_type="integer", min_val=1, max_val=1000, unique=True)
    customer_schema.add_text_field("name", min_length=3, max_length=50)
    customer_schema.add_text_field("email", pattern=r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}")
    
    # Create order schema with relationship to customer
    order_schema = SchemaDefinition()
    order_schema.add_numeric_field("order_id", field_type="integer", min_val=10001, max_val=99999, unique=True)
    
    # Reference to customer schema
    order_schema.add_field("customer_id", {
        "type": "reference",
        "reference_schema": "customer",
        "reference_field": "customer_id"
    })
    
    order_schema.add_temporal_field("order_date", field_type="date")
    order_schema.add_numeric_field("total_amount", field_type="float", min_val=1.0, max_val=1000.0)
    
    # Create order items schema with relationship to order
    order_item_schema = SchemaDefinition()
    order_item_schema.add_numeric_field("item_id", field_type="integer", min_val=1, max_val=100000, unique=True)
    
    # Reference to order schema
    order_item_schema.add_field("order_id", {
        "type": "reference",
        "reference_schema": "order",
        "reference_field": "order_id"
    })
    
    order_item_schema.add_text_field("product_name", min_length=3, max_length=100)
    order_item_schema.add_numeric_field("quantity", field_type="integer", min_val=1, max_val=100)
    order_item_schema.add_numeric_field("price", field_type="float", min_val=0.1, max_val=500.0)
    
    # Save schemas
    output_dir = Path(__file__).parent / "schemas" / "relationships"
    output_dir.mkdir(exist_ok=True, parents=True)
    
    customer_schema.save_schema(str(output_dir / "customer_schema.json"))
    order_schema.save_schema(str(output_dir / "order_schema.json"))
    order_item_schema.save_schema(str(output_dir / "order_item_schema.json"))
    
    # Create relationship definition
    relationships = {
        "schemas": ["customer", "order", "order_item"],
        "relationships": [
            {
                "parent_schema": "customer",
                "parent_field": "customer_id",
                "child_schema": "order",
                "child_field": "customer_id",
                "relationship_type": "one_to_many"
            },
            {
                "parent_schema": "order",
                "parent_field": "order_id",
                "child_schema": "order_item",
                "child_field": "order_id",
                "relationship_type": "one_to_many"
            }
        ]
    }
    
    # Save relationship definition
    with open(str(output_dir / "relationships.json"), "w") as f:
        json.dump(relationships, f, indent=2)
    
    logger.info(f"Saved relationship schemas to {output_dir}")
    
    return {
        "customer": customer_schema,
        "order": order_schema,
        "order_item": order_item_schema,
        "relationships": relationships
    }

def main():
    """Run all schema examples."""
    logger.info("Starting schema examples")
    
    # Basic schema
    basic_schema = basic_schema_example()
    
    # Load and extend schema
    extended_schema = load_and_extend_schema()
    
    # Complex schema
    complex_schema = create_complex_schema()
    
    # Data quality schema
    quality_schema = create_data_quality_schema()
    
    # Schemas with relationships
    related_schemas = schema_with_relationships()
    
    logger.info("Schema examples completed")

if __name__ == "__main__":
    main()